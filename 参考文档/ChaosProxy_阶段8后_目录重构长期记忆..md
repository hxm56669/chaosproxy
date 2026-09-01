# ChaosProxy 阶段8后：目录职责分层重构长期记忆

> 本文记录阶段8完成后、阶段9 Latency/Jitter 开始前的一次纯物理目录重构。重构基于现有 `ConnectionToken{id,generation}`、`ConnectionManager unique_ptr`、TimerQueue、ToxicPipeline、PipelineSnapshot、Fast Path 与 pipeline_inflight 语义；不改变任何运行时行为。

## 1. 重构时机

阶段4～6曾明确决定暂时保持 `include/chaosproxy/*.h + src/*.cpp` 扁平结构，并在 Timer/Toxic/Config 模块明显增多后重新评估。阶段7已经加入 Reactor Timer 基础设施，阶段8已经加入 ToxicPipeline，且阶段9～11还会持续增加多种 Toxic，因此阶段8结束后是第一次目录分层的合适节点。

## 2. 重构原则

本轮只修改：

```text
文件位置
#include 路径
CMake 源文件/测试文件路径
unit test 物理目录
```

本轮不修改：

```text
namespace chaosproxy
公开接口
ConnectionManager unique_ptr 所有权
ConnectionToken{id,generation}
EventToken
TimerQueue/timerfd语义
Direction/Buffer/背压
FIN/RST状态机
PipelineSnapshot
continuation
pipeline_inflight
CMake target 数量
```

仍然只有：

```text
chaosproxy_core
chaosproxy
chaosproxy_tests
```

## 3. 重构后的目录

```text
chaosproxy/
├── CMakeLists.txt
├── include/
│   └── chaosproxy/
│       ├── app.h
│       ├── net/
│       │   ├── unique_fd.h
│       │   ├── socket_ops.h
│       │   └── buffer.h
│       ├── reactor/
│       │   ├── clock.h
│       │   ├── timer_queue.h
│       │   └── event_loop.h
│       ├── proxy/
│       │   ├── listener.h
│       │   ├── endpoint.h
│       │   ├── connection_token.h
│       │   ├── connection.h
│       │   └── connection_manager.h
│       └── toxic/
│           ├── chunk.h
│           ├── toxic.h
│           ├── toxic_pipeline.h
│           └── pass_through_toxic.h
├── src/
│   ├── app.cpp
│   ├── main.cpp
│   ├── net/
│   │   ├── socket_ops.cpp
│   │   └── buffer.cpp
│   ├── reactor/
│   │   ├── timer_queue.cpp
│   │   └── event_loop.cpp
│   ├── proxy/
│   │   ├── listener.cpp
│   │   ├── endpoint.cpp
│   │   ├── connection.cpp
│   │   └── connection_manager.cpp
│   └── toxic/
│       ├── chunk.cpp
│       ├── toxic.cpp
│       ├── toxic_pipeline.cpp
│       └── pass_through_toxic.cpp
└── tests/
    ├── helpers/
    ├── unit/
    │   ├── app_test.cpp
    │   ├── net/
    │   │   ├── unique_fd_test.cpp
    │   │   ├── socket_ops_test.cpp
    │   │   └── buffer_test.cpp
    │   ├── reactor/
    │   │   ├── event_loop_test.cpp
    │   │   ├── timer_queue_test.cpp
    │   │   └── event_loop_timer_test.cpp
    │   ├── proxy/
    │   │   └── connection_test.cpp
    │   └── toxic/
    │       └── toxic_pipeline_test.cpp
    ├── integration/
    │   └── 保持按跨模块场景组织
    └── manual/
        └── 保持现有smoke测试
```

## 4. 模块职责

### net

只放底层 fd、Socket 调用和通用字节 Buffer。它不应知道 Connection、Toxic 或具体业务状态机。

### reactor

只放 EventLoop、TimerQueue、monotonic clock 等事件/时间调度基础设施。

### proxy

负责 TCP 代理会话领域对象：Listener、Endpoint、ConnectionToken、ConnectionPair、ConnectionManager，以及双 Direction、背压和关闭生命周期。

### toxic

负责 Chunk、Toxic 抽象、ToxicPipeline、PipelineSnapshot 和具体故障策略。阶段9～11新增 Latency/Jitter/Bandwidth/Slicer/Timeout/Reset 等均进入此目录。

## 5. include 规则

重构前：

```cpp
#include "chaosproxy/event_loop.h"
#include "chaosproxy/connection.h"
#include "chaosproxy/toxic.h"
```

重构后：

```cpp
#include "chaosproxy/reactor/event_loop.h"
#include "chaosproxy/proxy/connection.h"
#include "chaosproxy/toxic/toxic.h"
```

namespace 仍统一使用：

```cpp
namespace chaosproxy {
}
```

本轮不拆 `chaosproxy::net/reactor/proxy/toxic` 子命名空间，避免物理目录重构与 API/符号重构混在同一次变更。

## 6. CMake决定

当前仍使用一个 `chaosproxy_core`。目录分层用于让开发者看清职责；等阶段11核心 Toxic 完成后，再根据依赖图是否稳定决定是否拆为多个 CMake target。

如果现在立即拆 `chaosproxy_net/reactor/proxy/toxic` 多个库，会过早引入 PUBLIC/PRIVATE 链接关系与潜在循环依赖，收益不足。

## 7. 测试目录规则

unit test 按所属模块分目录，因为它们天然测试单个模块。

integration test 继续按场景放在 `tests/integration/`，因为如 backpressure、half-close、pipeline-generation 本身横跨 net/reactor/proxy/toxic，硬塞入单一模块会误导。

## 8. 本轮验证

实际在 Stage8 验证工程副本上执行了目录移动与 include/CMake 更新：

```text
[x] CMake + Ninja + BUILD_TESTING=OFF 构建成功
[x] chaosproxy_core 构建成功
[x] chaosproxy 链接成功
[x] -Wall -Wextra -Wpedantic -Werror manual smoke 编译成功
[x] stage8_smoke 运行通过
[x] stage8_generation_smoke 运行通过
[x] ASan + UBSan stage8_smoke 运行通过（LeakSanitizer关闭）
[x] 归一化掉include路径后，移动前后生产/单测源码逻辑差异 = 0
```

正式仓库仍应运行完整 GTest/CTest：

```bash
cmake -S . -B build-debug \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DBUILD_TESTING=ON

cmake --build build-debug -j
ctest --test-dir build-debug --output-on-failure
```

## 9. 下一次重构触发条件

阶段9～11先继续使用当前四层目录。阶段11核心 Toxic 完成后重新评估：

1. 是否增加 `config/`、`observability/` 等新职责目录；
2. 模块依赖方向是否已经稳定；
3. 是否需要把一个 `chaosproxy_core` 拆为多个 CMake library 来强制依赖边界；
4. 是否值得进一步拆 namespace。

原则：先让目录帮助人理解模块，再让 CMake target 帮编译系统强制模块边界。

## 10. 下一阶段入口

```text
阶段9：Latency 与 Jitter
```

新增文件建议直接进入：

```text
include/chaosproxy/toxic/latency_toxic.h
include/chaosproxy/toxic/jitter_toxic.h
src/toxic/latency_toxic.cpp
src/toxic/jitter_toxic.cpp
tests/unit/toxic/latency_toxic_test.cpp
tests/unit/toxic/jitter_toxic_test.cpp
```
