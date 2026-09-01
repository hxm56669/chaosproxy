# Decisions

## D-001：核心逻辑独立成库

`chaosproxy` 与 `chaosproxy_tests` 共同链接 `chaosproxy_core`。这样生产程序和测试执行同一份实现，避免复制代码，也避免测试目标与 `main()` 冲突。

## D-002：输出流依赖注入

`chaosproxy::Run` 接收 `std::ostream&`，由进程入口传入 `std::cout` 和 `std::cerr`，测试传入 `std::ostringstream`。这使输出可精确断言，并避免全局重定向带来的共享状态。

## D-003：M0 不包含网络实现

M0 只建立可持续验证的工程基线。网络数据路径从 M1 开始，避免把代理自身的 I/O 缺陷误判为故障注入结果。

