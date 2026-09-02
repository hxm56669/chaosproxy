# Design

## 当前模块关系

```text
Listener ──accepted fd──> ConnectionManager
                              │ unique_ptr
                              ▼
                        ConnectionPair
                     ┌────────┴────────┐
                  Client            Upstream
                     │                  │
                     └─ two Directions ┘
                         fixed Buffer
                              │
                              ▼
EventLoop ── epoll + timerfd + TimerQueue
```

`EventLoop` 只拥有 epoll 注册与时间调度，不拥有业务连接。`ConnectionManager` 使用 `unique_ptr` 唯一拥有 `ConnectionPair`；EventLoop 回调保存 `ConnectionToken{id,generation}` 和 EndpointSide，回到 Manager 校验身份后才临时借用连接。

每个 Direction 独立维护 pending Buffer、EOF、写半关闭和背压状态。短写只消费实际写入字节；source FIN 后先 drain pending，再对 destination 执行 `shutdown(SHUT_WR)`。两个方向都结束后才正常回收整个连接。

## Stage7 时间路径

```text
ScheduleAt/After
→ TimerQueue::Schedule
→ 最近 deadline 映射到单个 timerfd
→ epoll_wait
→ timerfd EPOLLIN
→ RunExpired(steady_clock::now())
→ callback
→ 重新 arm 下一 deadline
```

Timer callback 与网络事件都运行在同一个 Reactor 线程。TimerQueue 负责逻辑排序和取消，timerfd 只负责在最近 deadline 唤醒 EventLoop。连接级 Timer 必须保存 `ConnectionToken`，不能保存长期裸 `ConnectionPair*`。

## 进程入口边界

`main.cpp` 仍只调用 `chaosproxy::Run`。监听地址、上游地址和 Listener/Manager 的实际装配尚未进入命令行入口；当前网络能力通过核心库测试验证。
