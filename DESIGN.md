# Design

## M0 目标关系

```text
chaosproxy (src/main.cpp) ──┐
                           ├──> chaosproxy_core (src/app.cpp)
chaosproxy_tests ──────────┘
          └──> GoogleTest
```

`main.cpp` 只把进程参数和标准流交给 `chaosproxy::Run`。`Run` 接收 `std::ostream&`，使测试能用内存流独立验证标准输出和标准错误，而不修改整个进程的流状态。

后续网络组件（如 `EventLoop`、`ConnectionPair`、`Buffer`、`TimerQueue` 和 `ToxicPipeline`）进入核心库，不堆入进程入口。

