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

以上是 M0 的历史边界，不代表当前工程状态。

## M2/M3 已实现

- 使用 Linux 非阻塞 Socket 与 LT epoll 驱动多个连接。
- 每个代理会话包含 Client/Upstream 两个 Endpoint 和两个独立 Direction。
- 每个 Direction 使用固定容量 Buffer，正确处理短写和 `EAGAIN`。
- pending 为空时不订阅 `EPOLLOUT`。
- 达到 high watermark 后暂停 source 读取，降低到 low watermark 后恢复。
- 每次事件使用读写字节预算，防止单连接长期占用 Reactor。
- FIN 必须等待 pending 排空后再向 destination 执行 `SHUT_WR`。
- 两个 Direction 都完成后才正常回收连接；RST 和 fatal error 整体关闭。
- `ConnectionManager` 唯一拥有连接，长期回调只保存 `ConnectionToken{id,generation}`。

## Stage7 已实现

- Timer 使用 `std::chrono::steady_clock`。
- `TimerQueue` 使用 deadline + `TimerId` 排序的最小堆。
- 同一 EventLoop 的全部逻辑 Timer 共用一个 `timerfd`。
- Timer 支持取消；已取消任务不得执行。
- 连接级 Timer 通过 `ConnectionToken` 重新解析连接，不保存长期裸指针。
- Reactor 线程禁止通过 `sleep` 实现延迟。

## 当前明确未实现

- 可运行代理的监听地址/上游地址命令行配置与入口接线。
- ToxicPipeline 和实际故障注入策略。
- 跨线程调度、多 Reactor、日志、指标和动态控制面。
