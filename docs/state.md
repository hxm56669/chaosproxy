# ChaosProxy + PhotoTask 状态

- 项目：ChaosProxy + PhotoTask
- 蓝图版本：V2.1
- 真实仓库/分支：`Z:/project/chaosproxy` / `feature/stage8-toxic-pipeline`
- 当前 commit：`0b7c668`（M0～M2 已提交并推送）
- 当前里程碑/组：M2 / A8
- 状态：M0～M2 已完成，准备交付

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

M3：进入 PhotoTask 真实照片输入、MySQL 任务与本地执行闭环。

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
