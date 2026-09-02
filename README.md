# ChaosProxy

ChaosProxy 是一个面向故障注入实验的 Linux TCP 代理项目。当前核心库已经完成 M2/M3 非阻塞多连接数据路径，以及 Stage7 `TimerQueue + timerfd` 定时基础设施。

已实现：

- `UniqueFd` 与非阻塞 Socket 操作；
- LT 模式 epoll `EventLoop` 和 `Listener`；
- 固定容量环形 Buffer、短写与动态 `EPOLLOUT`；
- 双向转发、高低水位背压和每事件 I/O 预算；
- FIN drain、半关闭、RST/fatal error 回收；
- `ConnectionToken{id,generation}` 防止旧回调命中复用连接；
- 单调时钟 `TimerQueue`、Timer 取消和单 `timerfd` Reactor 唤醒。

当前 `chaosproxy` 命令行入口仍保持工程骨架行为，尚未接入监听地址和固定上游配置。网络核心通过单元及集成测试验证。

## 构建

环境要求：

- Linux
- 支持 C++17 的编译器
- CMake 3.16 或更高版本
- GoogleTest
- Ninja

```bash
cmake -S . -B build-stage7 \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTING=ON
cmake --build build-stage7 -j
ctest --test-dir build-stage7 --output-on-failure
```

如只需构建程序，可关闭测试：

```bash
cmake -S . -B build-release \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF
cmake --build build-release -j
```

## 运行

```bash
./build-stage7/chaosproxy
./build-stage7/chaosproxy --help
```
