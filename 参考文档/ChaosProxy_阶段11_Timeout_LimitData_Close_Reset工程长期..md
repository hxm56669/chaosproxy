# ChaosProxy 阶段11：Timeout / LimitData / Close / Reset 工程长期记忆

> 文档定位：本文建立在阶段10 `Bandwidth / Slicer + Chunk共享backing + Sequence Gate流式提交` 基线上，只记录阶段11新增/修改内容。继续使用阶段8后 `net / reactor / proxy / toxic` 四层目录，不回退旧扁平结构。

## 1. 当前项目位置

```text
项目：ChaosProxy
完成层级：L1 故障注入闭环（进行中）
前置工程：M3 + Stage7 TimerQueue + Stage8 ToxicPipeline + Stage9 Latency/Jitter + Stage10 Bandwidth/Slicer
当前阶段：阶段11——Timeout / LimitData / Close / Reset
本轮核心产出：控制型 DirectionContext + 方向级 graceful stop + abortive reset + 4种Toxic + 单元/集成测试
下一阶段：阶段12 配置、动态控制与可观测性
```

阶段11继续保持已有主线：

```text
ConnectionToken{id,generation} 防 stale callback
PipelineSnapshot 保证旧Chunk继续旧配置
Sequence Gate 保证跨sequence顺序
queued = pending + delayed + reorder
Timer注册到Direction，Connection关闭时主动Cancel
ConnectionManager dispatch_depth 防同步嵌套回调自析构/UAF
```

## 2. Stage11最关键设计决定

### D-S11-001：Toxic只提交控制意图，不直接接触fd

新增控制路径：

```text
Toxic
→ DirectionContext::RequestGracefulClose / RequestReset
→ ConnectionManager::DispatchTerminationRequest
→ Find(ConnectionToken{id,generation})
→ ConnectionPair::RequestTermination
→ 真正修改Direction/Endpoint/Connection生命周期
```

Toxic 不获得 `fd`，不调用 `close/shutdown/epoll_ctl`，因此 fd 所有权与 generation 保护不被破坏。

### D-S11-002：graceful stop 与 source EOF 分开建模

新增 Direction 状态：

```cpp
bool graceful_stop_requested{false};
CloseReason graceful_reason{CloseReason::kNone};
```

不把故障注入伪装成 `recv()==0`。二者都可触发方向结束，但原因不同：

```text
source_eof
= 真实对端FIN

graceful_stop_requested
= Toxic主动要求本方向停止继续接收新字节
```

两者最终都复用：

```text
停止继续读
→ 等pipeline_inflight归零
→ 等pipeline_results清空
→ 等delayed_bytes归零
→ 等delayed_timers清空
→ 等pending排空
→ shutdown(destination, SHUT_WR)
```

### D-S11-003：Reset作用于触发Direction的destination Endpoint

例如：

```text
client → upstream Direction触发Reset
→ 对upstream Endpoint设置 SO_LINGER {1,0}
→ Cleanup关闭该fd
→ upstream一侧通常观察RST / ECONNRESET
```

随后整个 `ConnectionPair` 进入异常回收，另一个代理侧Socket也被释放。Reset不是半关闭，不等待已有数据drain。

### D-S11-004：有状态Toxic的运行状态放在DirectionRuntimeState

`Toxic` 对象属于 PipelineSnapshot，可能被多个连接/Direction共享，因此以下状态不能作为 Toxic 普通成员：

```text
Timeout是否已armed
LimitData已经允许多少字节
```

Stage11延续Stage10 Bandwidth的 runtime-key 模型：

```cpp
DirectionRuntimeState
├── bandwidth_buckets[runtime_key]
├── timeout_states[runtime_key]
└── limit_data_states[runtime_key]
```

于是：

```text
同一个Toxic实例 + 不同Direction
→ 状态完全隔离
```

### D-S11-005：CloseReason 与 TerminationMode 分开

新增：

```cpp
enum class TerminationMode {
    kGraceful,
    kReset
};
```

语义：

```text
CloseReason    = 为什么结束
TerminationMode = 怎么结束
```

Stage11新增原因：

```text
kTimeoutToxic
kLimitDataToxic
kCloseToxic
kResetToxic
```

`CloseReason` 从 `connection.h` 提取到 `proxy/termination.h`，让 Toxic 可以依赖关闭语义而不反向 include 整个 ConnectionPair。

## 3. 新增/修改目录

```text
新增：
include/chaosproxy/proxy/termination.h

include/chaosproxy/toxic/timeout_toxic.h
include/chaosproxy/toxic/limit_data_toxic.h
include/chaosproxy/toxic/close_toxic.h
include/chaosproxy/toxic/reset_toxic.h

src/toxic/timeout_toxic.cpp
src/toxic/limit_data_toxic.cpp
src/toxic/close_toxic.cpp
src/toxic/reset_toxic.cpp

tests/unit/toxic/stage11_toxic_test.cpp
tests/integration/stage11_integration_test.cpp

修改：
include/chaosproxy/toxic/direction_runtime.h
include/chaosproxy/toxic/toxic.h
src/toxic/toxic.cpp
include/chaosproxy/proxy/connection.h
src/proxy/connection.cpp
include/chaosproxy/proxy/connection_manager.h
src/proxy/connection_manager.cpp
tests/helpers/test_connection_harness.h
CMakeLists.txt
```

## 4. DirectionContext控制能力

Stage11在原有：

```text
ScheduleAt / ScheduleAfter
TryReserveDelayedBytes / ReleaseDelayedBytes
RegisterTimer / UnregisterTimer
BandwidthBucket / Latency barrier
```

基础上新增：

```cpp
TimeoutRuntimeState& TimeoutState(std::uint64_t runtime_key) const;
LimitDataRuntimeState& LimitDataState(std::uint64_t runtime_key) const;

bool RequestGracefulClose(CloseReason reason) const noexcept;
bool RequestReset(CloseReason reason) const noexcept;
```

生产环境的 termination callback 必须返回 Manager；兼容/独立Toxic测试构造器没有控制回调时，请求只返回 false，不直接操作资源。

## 5. TimeoutToxic

接口：

```cpp
explicit TimeoutToxic(Duration timeout);
```

语义：

```text
第一个Chunk到达
→ 当前Chunk不Emit（黑洞）
→ per-Direction state.armed = true
→ ScheduleAfter(timeout)
→ TimerId注册到Direction

后续Chunk到达
→ 继续黑洞
→ 不重复创建timer

Timer到期
→ UnregisterTimer
→ RequestGracefulClose(kTimeoutToxic)
```

注意：Timeout 不把 Chunk 保存到 Timer 中，因此不会为黑洞数据增加 delayed_bytes；数据在 Process 返回时即被故意丢弃，Pipeline ExecutionState正常完成。

连接提前关闭时，Stage9已有 `CancelDirectionTimers()` 会取消该 Timer；TimerQueue 的 Cancel 会立即释放 callback capture。generation 仍作为漏取消/旧回调晚到时的正确性兜底。

## 6. LimitDataToxic

接口：

```cpp
explicit LimitDataToxic(std::size_t limit_bytes);
```

状态：

```cpp
std::size_t forwarded_bytes;
bool reached;
```

其中 `forwarded_bytes` 的严格定义是：

> 已经被该 LimitDataToxic 允许继续进入后续 Pipeline 的字节数。

它不是“已经真正 send 到内核”的字节数，因为 LimitData 后面仍可能有 Latency/Bandwidth。

处理：

```text
remaining = limit - forwarded

chunk < remaining
→ 整块output

chunk == remaining
→ 整块output
→ reached
→ RequestGracefulClose

chunk > remaining
→ TakePrefix(remaining)
→ 只output合法前缀
→ suffix丢弃
→ reached
→ RequestGracefulClose

limit == 0
→ 0字节通过
→ 立即请求graceful stop
```

达到 limit 后，ConnectionPair 停止继续 source recv；已经合法进入 downstream Toxic / delayed / reorder / pending 的字节继续 drain，最终才 `SHUT_WR`。

## 7. CloseToxic

第一版语义：

```text
当前Chunk已经进入该Direction
→ output当前Chunk继续原PipelineSnapshot
→ RequestGracefulClose(kCloseToxic)
→ 不再读取新的source字节
→ 等当前及更老合法工作全部drain
→ shutdown(destination, SHUT_WR)
```

因此 `CloseToxic → Latency` 时不会出现 `Emit后立刻close导致最后Chunk丢失`；`pipeline_inflight + delayed_timers + Sequence Gate` 会阻止提前FIN。

## 8. ResetToxic

语义：

```text
当前Chunk不output
→ RequestReset(kResetToxic)
→ Manager按ConnectionToken重新Find
→ ConnectionPair找到destination Endpoint
→ setsockopt(SO_LINGER, {1,0})
→ Cleanup取消Timer / Remove epoll token / 关闭fd
→ 对destination对端形成abortive close，通常观察ECONNRESET
```

RST 的外部表现依赖TCP状态与内核行为；自动化集成测试在已建立的 loopback TCP 连接上验证 `ECONNRESET`，而不是仅验证“Socket被关了”。

## 9. ConnectionPair方向级graceful stop

`HandleReadable()`新增三层停止保护：

```text
函数入口：graceful_stop_requested → 不读
while条件：graceful_stop_requested → 不继续下一次recv
本次Chunk处理完：如果Toxic同步请求stop → break
```

这解决一个重要边界：一次 EPOLLIN 回调可能循环 recv 多次。Close/LimitData 在当前 Chunk 内触发后，不能因为 read budget 还没耗尽又读取下一批字节。

`DesiredInterests()` 同样不再给该source Endpoint订阅 `EPOLLIN`。

## 10. graceful finish 条件

Stage11把原来的：

```text
source_eof && 全部队列为空
```

扩展为：

```text
(source_eof || graceful_stop_requested)
&& pipeline_inflight == 0
&& pipeline_results.empty()
&& delayed_bytes == 0
&& delayed_timers.empty()
&& pending.empty()
&& !write_shutdown
```

满足后：

```cpp
shutdown(destination_fd, SHUT_WR);
```

另一 Direction 不会因为单向 graceful close 自动停止，仍可继续读取并返回数据，保持 TCP 半关闭语义。

## 11. Reset与Manager嵌套dispatch生命周期

Reset 可能发生在：

```text
ConnectionManager::DispatchEvent
→ ConnectionPair::HandleReadable
→ ToxicPipeline
→ ResetToxic
→ DirectionContext control callback
→ ConnectionManager::DispatchTerminationRequest
→ ConnectionPair::RequestTermination(reset)
```

这是同步嵌套回 Manager。Stage9已有 `dispatch_depth`：

```text
内层Close只标pending_destroy
→ 不当场reset unique_ptr
→ 直到最外层dispatch返回
→ CollectPendingClosed
```

因此不会在 `ResetToxic::Process()` 尚未返回时析构 `ConnectionPair`。

## 12. CMake增量

`chaosproxy_core` 增加：

```cmake
src/toxic/timeout_toxic.cpp
src/toxic/limit_data_toxic.cpp
src/toxic/close_toxic.cpp
src/toxic/reset_toxic.cpp
```

`chaosproxy_tests` 增加：

```cmake
tests/unit/toxic/stage11_toxic_test.cpp
tests/integration/stage11_integration_test.cpp
```

## 13. Stage11测试目标

### 单元测试

```text
[x] Timeout：Chunk不Emit
[x] Timeout：多个Chunk只arm一个Timer
[x] Timeout：deadline前不终止，deadline到达后请求kTimeoutToxic/graceful
[x] LimitData：跨Chunk精确切边界
[x] LimitData：limit=0零字节通过
[x] Close：当前Chunk先output，再请求graceful
[x] Reset：当前Chunk不output，请求reset
```

### 集成测试

```text
[x] CloseToxic：upstream先收到当前合法Chunk，再观察EOF/FIN
[x] LimitDataToxic：upstream精确收到N字节，再观察EOF/FIN
[x] TimeoutToxic：upstream不收到被黑洞的数据，到期后观察EOF/FIN
[x] ResetToxic：已建立TCP连接上观察ECONNRESET，而不是EOF
```

正式仓库还应补充/保留已有回归：

```text
Stage9 stale timer + ConnectionToken generation
Stage9 Timer Cancel立即释放callback
Stage10 Bandwidth Sequence Gate流式提交
Stage6 half-close
Close幂等
```

## 14. 应用方式

交付包中提供：

```text
apply_chaosproxy_stage11.py
chaosproxy_stage11_overlay/
```

在 Stage10 仓库基线运行：

```bash
python3 /path/to/apply_chaosproxy_stage11.py /path/to/chaosproxy
```

然后：

```bash
cmake -S . -B build-debug \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DBUILD_TESTING=ON

cmake --build build-debug -j
ctest --test-dir build-debug --output-on-failure

./build-debug/chaosproxy_tests \
  --gtest_filter='Stage11ToxicTest.*:Stage11IntegrationTest.*'
```

Sanitizer：

```bash
cmake -S . -B build-asan \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DBUILD_TESTING=ON \
    -DCMAKE_CXX_FLAGS='-fsanitize=address,undefined -fno-omit-frame-pointer'

cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure
```

## 15. 本次验证状态

当前对话提供的是 Stage0～10 工程长期记忆与 Stage10 patch/应用器信息，不是完整 Git 仓库，因此不能诚实宣称“正式仓库 CMake/GTest/CTest 全部通过”。

本次已经实际执行：

```text
[x] apply_chaosproxy_stage11.py Python语法检查
[x] apply脚本对 documented Stage10 fragment fixture 成功应用
[x] apply脚本二次运行幂等检查
[x] Stage11新生产模块：g++ C++17 -Wall -Wextra -Wpedantic -Werror 编译通过
[x] Stage11 deterministic smoke：Timeout / LimitData / Close / Reset 语义通过
[x] Stage11新模块 ASan + UBSan smoke通过（LeakSanitizer关闭）
[ ] 正式完整仓库 CMake/Ninja/GTest/CTest：必须在真实Stage10仓库运行
[ ] 正式完整仓库 ASan/UBSan全套：必须在真实Stage10仓库运行
```

## 16. Stage11完成边界

代码层面已经完成 Stage11 主干：

```text
Timeout
LimitData
Close
Reset
DirectionContext control path
ConnectionManager generation-safe dispatch
Direction graceful drain
RST abortive close
Stage11 unit/integration tests
```

但只有真实仓库跑过 Stage0～11 全套质量门后，才能把 M4 标为正式通过。

M4通过后下一阶段进入：

```text
阶段12：配置、动态控制与可观测性
```

重点转向：配置解析/校验、策略配置、连接日志、关闭原因和关键指标，不再新增一套新的网络生命周期模型。
