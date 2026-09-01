# Requirements

## M0 必须完成

- 使用 C++17，并在编译器不支持时配置失败。
- 能完成 CMake 配置、编译、运行和 CTest 测试。
- `chaosproxy_core` 承载可测试的核心逻辑。
- `chaosproxy` 只保留进程入口，并链接 `chaosproxy_core`。
- `chaosproxy_tests` 链接 `chaosproxy_core` 和 GoogleTest。
- 无参数和 `--help` 返回 0；非法参数返回 2。
- 正常输出写入标准输出，参数错误写入标准错误。

## M0 明确不做

- Socket、epoll 或其他网络 I/O。
- TCP 双向转发、缓冲、背压和半关闭。
- 多上游、负载均衡、健康检查和故障转移。
- 延迟、限速、丢包、RST 等故障注入。

