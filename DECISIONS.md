# Decisions

## D-001：核心逻辑独立成库

`chaosproxy` 与 `chaosproxy_tests` 共同链接 `chaosproxy_core`。这样生产程序和测试执行同一份实现，避免复制代码，也避免测试目标与 `main()` 冲突。

## D-002：输出流依赖注入

`chaosproxy::Run` 接收 `std::ostream&`，由进程入口传入 `std::cout` 和 `std::cerr`，测试传入 `std::ostringstream`。这使输出可精确断言，并避免全局重定向带来的共享状态。

## D-003：M0 不包含网络实现

M0 只建立可持续验证的工程基线。网络数据路径从 M1 开始，避免把代理自身的 I/O 缺陷误判为故障注入结果。

## D-004：连接由 Manager 唯一拥有

`ConnectionManager` 使用 `unique_ptr` 唯一拥有 `ConnectionPair`。EventLoop 回调不参与所有权，只携带 `ConnectionToken{id,generation}`；Manager 校验后才能在当前同步调用中借用连接。

## D-005：固定容量 Buffer 与水位背压

每个 Direction 使用固定容量环形 Buffer。达到 high watermark 时暂停 source EPOLLIN，降低到 low watermark 后恢复；pending 为空时不长期订阅 destination EPOLLOUT。

## D-006：FIN 与 fatal error 分开处理

FIN 是 Direction 级正常结束：先 drain pending，再向 destination 传播 `SHUT_WR`。RST、连接失败和 fatal I/O error 则关闭整个 ConnectionPair。`Close()` 必须幂等。

## D-007：一个 Reactor 只使用一个 timerfd

`TimerQueue` 管理全部逻辑 Timer，一个 EventLoop 只用一个 timerfd 映射最近 deadline。Timer 使用 `steady_clock`，回调运行在 Reactor 线程，不允许通过 sleep 实现延迟。

## D-008：Cancel 与 generation 职责分离

Timer 失效时应主动 Cancel；`ConnectionToken` 的 generation 负责在旧 Timer 漏取消或晚到时阻止它命中新连接。Cancel 是正常生命周期管理，generation 是身份安全兜底。
