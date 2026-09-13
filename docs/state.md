# ChaosProxy + PhotoTask 状态

- 项目：ChaosProxy + PhotoTask
- 蓝图版本：V2.1
- 真实仓库/分支：`Z:/project/chaosproxy` / `feature/stage8-toxic-pipeline`
- 当前 commit：见 `git rev-parse HEAD`（M3～M5 已提交并推送）
- 当前里程碑/组：M7 / G2
- 状态：M3～M6 已完成，M7/G1～G2 已完成；未发现足以支持结构性优化的性能证据

## 本次唯一行为或不变量

M0 已形成公共基础库、正式 GoogleTest 基线和 CLI 入口；A1/A2/A3 已形成单一 fd 所有权、非阻塞 socket、epoll 注册、generation token、连接表、双向推进和有界 ByteQueue。

## 本组已改文件

- `.gitignore`
- `CMakeLists.txt`
- `CMakePresets.json`
- `vcpkg.json`
- `.gitmodules` 与 `third_party/vcpkg`（gitlink 已固定）
- `docs/DEPENDENCIES.md`
- `docs/state.md`
- `include/chaosproxy/common/status.h`
- `src/common/status.cpp`
- `include/chaosproxy/common/status_or.h`
- `include/chaosproxy/common/completion.h`
- `include/chaosproxy/common/logging.h`
- `src/common/logging.cpp`
- `cmake/project_options.cmake`
- `src/CMakeLists.txt`
- `tests/CMakeLists.txt`
- `tests/unit/common/status_test.cpp`
- `apps/CMakeLists.txt`
- `apps/chaosproxy_main.cpp`
- `apps/chaosctl_main.cpp`
- `include/chaosproxy/common/unique_fd.h`
- `include/chaosproxy/proxy/types.h`
- `include/chaosproxy/proxy/socket_ops.h`
- `src/proxy/socket_ops.cpp`
- `tests/unit/proxy/socket_ops_test.cpp`
- `include/chaosproxy/proxy/event_loop.h`
- `include/chaosproxy/proxy/connection_table.h`
- `include/chaosproxy/proxy/connection_pair.h`
- `src/proxy/event_loop.cpp`
- `src/proxy/connection_table.cpp`
- `src/proxy/connection_pair.cpp`
- `tests/unit/proxy/connection_table_test.cpp`
- `tests/integration/proxy/connection_pair_test.cpp`
- `include/chaosproxy/proxy/buffer.h`
- `src/proxy/buffer.cpp`
- `tests/unit/proxy/buffer_test.cpp`
- `tests/integration/proxy/half_close_test.cpp`
- `tests/unit/proxy/event_loop_test.cpp`
- `include/chaosproxy/proxy/clock.h`
- `include/chaosproxy/proxy/timer_queue.h`
- `src/proxy/timer_queue.cpp`
- `tests/unit/proxy/timer_queue_test.cpp`
- `include/chaosproxy/proxy/toxics.h`
- `include/chaosproxy/proxy/toxic_pipeline.h`
- `src/proxy/toxics.cpp`
- `src/proxy/toxic_pipeline.cpp`
- `tests/unit/proxy/toxic_pipeline_test.cpp`
- `include/chaosproxy/proxy/config.h`
- `src/proxy/config.cpp`
- `include/chaosproxy/proxy/proxy_server.h`
- `src/proxy/proxy_server.cpp`
- `include/chaosproxy/control/control_protocol.h`
- `src/control/control_protocol.cpp`
- `include/chaosproxy/control/control_server.h`
- `src/control/control_server.cpp`
- `tests/unit/proxy/config_control_test.cpp`
- `configs/proxy.json`
- `include/chaosproxy/proxy/trace_writer.h`
- `src/proxy/trace_writer.cpp`
- `benchmarks/run_proxy.py`
- `tests/integration/proxy/trace_writer_test.cpp`

## 已通过测试与命令

- 已读取蓝图与代码约束文档，确认当前组为 A0.1。
- 已确认仓库当前存在用户已有删除改动，本次未恢复或覆盖这些文件。
- Ubuntu：`git -C third_party/vcpkg rev-parse HEAD` → `9e593bb18ea69cc5095e012465dcd675a822ed0d`。
- Ubuntu：`./third_party/vcpkg/bootstrap-vcpkg.sh -disableMetrics` 成功。
- Ubuntu：`./third_party/vcpkg/vcpkg version` → `2026-07-27-98d7cb0cf1f4686a3e43aa5672b6230c1d56bce8`。
- Ubuntu：`cmake --preset proxy-debug` 成功；vcpkg 解析并安装 cli11 2.6.2、jsoncpp 1.9.6、fmt 12.2.0#1、spdlog 1.17.0#1、gtest 1.17.0#3 及工具包。
- Ubuntu：CMake 识别 GCC/G++ 13.4.0，生成 `build/proxy-debug`。
- Ubuntu：`cmake --build --preset proxy-debug` 成功，Ninja 报告 `no work to do`（当前无 target）。
- Ubuntu 工具版本：CMake 4.2.3、Ninja 1.13.2、pkg-config 2.5.1。
- Ubuntu：`g++-13 ... -c src/common/status.cpp` 成功生成 `build/probes/status.o`。
- Ubuntu：StatusOr/Completion move-only、错误访问和回调探针运行成功，输出 `A0.3_TEST_OK`。
- Ubuntu：spdlog 异步 file sink 探针运行成功，输出 `A0.4_TEST_OK`；满队列策略为 `overrun_oldest`，不阻塞调用线程。
- Ubuntu：`cmake --preset proxy-debug && cmake --build --preset proxy-debug && ctest --preset proxy-debug --output-on-failure` 成功，5/5 测试通过。
- Ubuntu：`chaosproxy --help`、`chaosproxy --version`、`chaosctl --version` 通过；无配置启动返回退出码 2 和明确错误。
- Ubuntu：`cmake --preset proxy-debug && cmake --build --preset proxy-debug && ctest --preset proxy-debug --output-on-failure` 成功，9/9 测试通过。
- Ubuntu：同一命令成功，13/13 测试通过；覆盖 generation 失效、epoll token 分发、双向 ConnectionPair。
- Ubuntu：同一命令成功，16/16 测试通过；覆盖 allocation 预算、逻辑队列字节、短写消费和 FIN 排空。
- Ubuntu：同一命令成功，18/18 测试通过；覆盖 ET 就绪队列按 connection generation/方向去重，以及 generation 变化后的重新入队。
- Ubuntu：同一命令成功，21/21 测试通过；覆盖 TimerQueue deadline/sequence 排序、callback budget、惰性取消，以及 EventLoop timerfd 唤醒。
- Ubuntu：同一命令成功，27/27 测试通过；覆盖 token bucket 显式 refill、latency/jitter continuation、bandwidth 部分 Chunk、slicer、pause/timeout 和 limit_data 实际成功字节边界。
- Ubuntu：同一命令成功，32/32 测试通过；覆盖多 Listener JSON 校验、expected_policy_version 冲突、64 KiB 控制帧、部分读帧、ACK 编码和真实 UDS 创建/清理。
- Ubuntu：同一命令成功，35/35 测试通过；覆盖 ControlServer 真实 UDS 分片请求/ACK、TraceWriter 有界队列耗尽与 drain 落盘。
- Ubuntu：`python3 benchmarks/run_proxy.py --binary build/proxy-debug/apps/chaosproxy --runs 1` 成功，输出 `trace_incomplete=false`；`chaosproxy --check-config --config configs/proxy.json`、两个 CLI `--version` 均通过。

## 失败/未执行测试

- 首次 bootstrap 因缺少 zip/unzip/pkg-config 失败；已安装系统包后重试成功。
- `ctest --preset proxy-debug`：未执行；A0.5 之前没有测试 target，当前 test preset 的 `noTestsAction=error` 不适用。
- A0.3 首次探针因遗漏链接 `status.cpp` 失败，补齐直接依赖后通过。
- A0.4 首次探针因 spdlog 多 sink 构造参数和直接静态链接顺序问题失败，修正后通过。
- A0.5 首次链接因静态 spdlog 需要 `libsupc++` 且测试 fixture 触发 `-Wshadow` 失败，修正后 5/5 通过。
- A0.6：未发现实现失败。
- A1 首次构建发现 StatusOr 构造函数 `explicit` 阻止 API 错误/值隐式返回，移除后通过。
- A2 首次构建发现 PumpBudget 临时对象不能绑定到可变引用、测试出现 vexing parse，修正后通过。
- A3 首次构建发现 buffer.h 缺少 StatusOr 直接包含，修正后通过。
- A4 首次构建发现 ReadyKey 默认比较依赖 ConnectionToken 相等运算符，改为按 slot/generation/direction 显式比较后通过。
- A5 首次构建发现取消集合在 const 查询中需要可变访问；首次集成测试还发现 timerfd 绝对时间纪元和毫秒向下取整边界，改为相对 timerfd、超时向上取整后通过。
- A6 首次构建发现策略头文件缺少 types 直接依赖、测试写入 std::byte 的类型转换问题；随后修正 FakeClock 零 epoch 初始化和异步成功字节共享状态后通过。
- A7 首次构建发现 `StatusOr<std::optional<T>>` 不能直接从 `std::nullopt` 构造，改为显式空 optional 后通过。
- A8 首次编译发现 UDS 集成测试缺少 `sockaddr_un` 头文件，补齐后通过。

## 当前已确认 Decision

- D01：单仓库、独立 target/进程。
- D02：C++20 / Linux x86-64。
- D15：固定 vcpkg commit、baseline、triplet；不虚构本机工具版本。

## 已知 Bug/风险

- 原分支已有大量 tracked 文件删除状态，属于本次任务前的工作树状态，未触碰。
- 远程 Linux 工作树上的 build/vcpkg 下载与安装目录已生成，均被 `.gitignore` 排除，未加入版本控制。

## 下一组目标

下一步：G3 新环境 replay、迁移、运行和关停验证。

## 下次必须提供文件

按蓝图进入 B0～B6，先补 PhotoTask 领域模型和持久化边界。

## 下次可省略的已验证模块

已验证的 A0.1 构建入口、vcpkg 安装结果和 A0.2～A0.4 公共基础细节可省略。

## 已完成组记录

- A0.1：固定 vcpkg/CMake 工程入口；Ubuntu configure/build 成功。
- A0.2：`Status`、errno 分类、唯一退出码映射；G++ 13 编译成功。
- A0.3：`StatusOr<T>`、`Completion<T>`；move-only 与错误访问运行探针成功。
- A0.4：有界异步 spdlog 初始化；file sink 运行探针成功。
- A0.5：正式 `common_tests` 建立；5/5 测试通过。
- A0.6：CLI11 入口构建成功，help/version/错误退出行为通过。
- A1：非阻塞 listener/connect/read/write、EOF、WouldBlock 和 UniqueFd 测试通过，累计 9/9。
- A2：EventLoop、ConnectionTable、ConnectionPair 测试通过，累计 13/13。
- A3：ByteQueue、BufferBudget、短写和半关闭测试通过，累计 16/16。
- A4：ET ready queue 去重与 generation 安全测试通过，累计 18/18。
- A5：TimerQueue、取消、callback budget 与 timerfd 唤醒测试通过，累计 21/21。
- A6：Toxic continuation、latency/jitter、bandwidth、slicer、timeout、limit_data 测试通过，累计 27/27。
- A7：JSON 配置、ProxyServer 版本应用、控制协议/UDS 测试通过，累计 32/32。
- A8：TraceWriter 有界队列、drain、代理基准探针和 CLI 配置检查通过，累计 35/35。
- B0：Drogon 真实 MariaDB 事务提交回调、提交后可见性、Protobuf 生成/解析通过；photo-debug 37/37。
- B1：领域模型规范化、SHA-256 内容寻址导入、001_core.sql migration、MariaDB 重复导入幂等通过；累计 41/41。
- B2：任务 fingerprint/idempotency、冲突和 HTTP 202 响应通过。
- B3：outbox batch claim 将 PENDING_DISPATCH 推进 READY，本地 dispatcher/executor 路径通过。
- B4：真实 PPM 输入缩略图、元数据、attempt 文件发布和失败 retry 路径通过。
- B5：lease renew、过期 requeue、owner+epoch CAS 防旧 owner 提交通过。
- B6：commit unknown 后按原 request key 查询恢复原 task 通过。
- M3：B0～B6 完成，photo-debug 48/48 全部通过。
- C1：hiredis 真实 Redis PING/GET/SETEX/DEL 与 DB miss 回源通过。
- C2：permit、failure threshold、cooldown breaker 通过。
- C3：Redis 删除失败进入 invalidation pending 队列，恢复后 flush 通过。
- C4：strong 读绕过缓存，更新后的值赢得 stale read race 通过。
- M4：C1～C4 完成，photo-debug 52/52 全部通过。
- D1：photo_runtime 双 API Compose 入口配置和运行入口骨架完成。
- D2：Nginx upstream 动态解析与路由配置完成；宿主机实际 `nginx -t` 通过。
- D3：GET 路由最多两次上游尝试，任务写路由 `proxy_next_upstream off`。
- D4：photo_runtime 实际 HTTP `/livez`、`/readyz`、`/metrics` 响应通过；C++ 测试通过。
- E1：JSON 场景 runner 只接受 argv 数组，不执行任意 shell 字符串；parse/ready/request/finally 框架完成。
- E2：commit-unknown 场景配置与 B6 独立查询测试完成；场景标记为 dry-run/skipped，不冒充真实命中。
- E3：批量报告包含 UTC 时间、seed、git build 和逐场景结果；runner/report Python 语法检查与 dry-run 通过。
- M5：D1～D4、E1～E3 完成；photo-debug 53/53 全部通过。
- F1：Kafka broker route 校验、固定 librdkafka 2.14.2 客户端依赖和事件通道边界完成。
- F2：KafkaPublisher produce/poll 完成，测试明确区分本地入队和 broker delivery。
- F3：InboxLedger 幂等 admission 与 contiguous offset 提交完成。
- F4：assignment epoch、revoke、有限队列和坏消息 quarantine 完成。
- F5：重复 event_id 只保留一个逻辑 inbox 记录，重放不会重复接入。
- F6：消费积压队列有界，Kafka 路由/单 broker 客户端配置入口完成。
- M6：F1～F6 完成，kafka-debug 59/59 全部通过；无 Kafka broker，K01～K08 真实场景未执行。

## M6 验证证据

- Ubuntu：`cmake --preset kafka-debug` 成功，并安装固定 librdkafka 2.14.2、lz4、zstd 依赖。
- Ubuntu：`cmake --build --preset kafka-debug && ctest --preset kafka-debug --output-on-failure` 最终通过 59/59。
- Kafka 测试覆盖：route 校验、producer 入队/交付语义、inbox 去重、连续 offset、assignment epoch、revoke、quarantine 和有界 backlog。
- 未执行：Kafka broker metadata、真实 produce delivery、ACK 丢失、rebalance 与多 broker quorum；远程 `docker`/Kafka CLI 不存在。
- G1：`benchmarks/run_m7.py` 实际执行 photo runtime health 与 kafka-debug 全量 ctest，生成 `/tmp/phototask-m7-baseline.json`；记录真实 seed、git、主机和命令级耗时，不推导吞吐。
- G2：`benchmarks/profile_m7.py` 实际执行 5 次固定 `photo_runtime --health`，生成 `/tmp/phototask-m7-profile.json`；真实样本 min/mean/max 为约 5.54/6.14/6.51 ms，仅作为命令级基线，不宣称业务吞吐。

## M3～M5 验证证据

- Ubuntu：MariaDB 11.8、Redis 8.0 已启动；MariaDB `phototask_test` 与 Redis PING 可用。
- Ubuntu：`cmake --preset photo-debug && cmake --build --preset photo-debug && ctest --preset photo-debug --output-on-failure` 最终通过 53/53。
- Ubuntu：`cmake --preset proxy-debug && cmake --build --preset proxy-debug && ctest --preset proxy-debug --output-on-failure` 回归通过 35/35。
- Ubuntu：`photo_runtime --health` 输出 `ok`；真实监听后 curl `/livez`、`/readyz`、`/metrics` 均成功。
- Ubuntu：Nginx `-t -c deploy/nginx.conf` syntax ok；Docker 未安装，因此 Compose 容器联调未执行。
- Ubuntu：`python3 -m py_compile tools/scenarios/runner.py tools/scenarios/report.py`、两个场景 dry-run 和批量报告生成成功；dry-run 结果按设计为 `skipped`。

## M3～M5 已知边界

- PhotoTask 当前图像处理器为受预算约束的 PPM P6 最小实现；JPG/PNG 已作为导入格式保留，但尚未接入解码缩略图路径。
- `TaskRepository` 是确定性内存端口实现；B0/B1 已用真实 Drogon/MariaDB 验证连接、事务和照片登记，完整任务 SQL CAS 接线留给后续持久化收口。
- Compose 文件已静态检查，远程没有 Docker，未宣称双实例容器联调通过。
