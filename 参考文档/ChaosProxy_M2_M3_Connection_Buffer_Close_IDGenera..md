# ChaosProxy 阶段4～6：ConnectionPair、Buffer、背压与关闭工程长期记忆

> 文档定位：本文是在《ChaosProxy 阶段2～3：非阻塞 Socket 与 Reactor 工程长期记忆》基础上的增量工程文档。阶段2～3已有 `UniqueFd`、`socket_ops`、`EventLoop`、`Listener` 不重写；本文只记录阶段4～6新增/调整的目录、核心代码、测试、设计决定、验证结果和后续入口。
> **2026-08-25 架构修订：** 阶段4～6的连接生命周期统一改为 `ConnectionId + generation`。`ConnectionManager` 通过 `unique_ptr` 唯一拥有 `ConnectionPair`；EventLoop 长期回调只保存 `ConnectionToken{id,generation}`，回到 Manager 校验后再借用对象。旧版 `shared_ptr/weak_ptr` 方案作废，后续阶段7 Timer 也沿用同一 token 身份模型。


---

## 1. 当前项目位置

```text
项目：ChaosProxy
完成层级：L0 原理闭环版（M2→M3工程闭环）
前置基线：M0工程基线、M1最小透明代理、阶段2/3基础模块
本轮教学阶段：阶段4 + 阶段5 + 阶段6
本轮核心产出：ConnectionPair + ConnectionManager + 固定容量Buffer + 动态EPOLLOUT + 高低水位背压 + 半关闭 + RST/错误回收
当前验证：核心构建、严格编译、普通冒烟、ASan/UBSan冒烟通过；正式GTest/CTest仍需在安装GTest的仓库环境执行
下一阶段：阶段7 TimerQueue
```

### 本轮解决的问题

```text
阶段2/3：
fd已经能非阻塞读写，EventLoop已经能分发事件

但还缺：
client fd 和 upstream fd 到底属于谁？
两个方向的 pending 数据放在哪里？
短写后怎样继续？
目标端写不动时怎样反压？
FIN 到来时怎样先 drain 再 SHUT_WR？
多个错误同时到达怎样只清理一次？

阶段4～6：
把这些问题统一收进 ConnectionPair / Direction / Buffer 生命周期。
```

---

## 2. 目录规划决定

继续保持当前扁平结构，不创建 `net/`、`reactor/`、`stage46/` 平行源码树。

```text
chaosproxy/
├── CMakeLists.txt
├── include/chaosproxy/
│   ├── app.h                     # 已有
│   ├── unique_fd.h               # 已有：阶段2
│   ├── socket_ops.h              # 已有：阶段2
│   ├── event_loop.h              # 已有：阶段3
│   ├── listener.h                # 已有：阶段3
│   ├── buffer.h                  # 阶段5新增
│   ├── endpoint.h                # 阶段4新增
│   ├── connection.h              # 阶段4～6核心
│   └── connection_manager.h      # 阶段4新增
├── src/
│   ├── app.cpp
│   ├── main.cpp
│   ├── socket_ops.cpp
│   ├── event_loop.cpp
│   ├── listener.cpp
│   ├── buffer.cpp                # 新增
│   ├── endpoint.cpp              # 新增
│   ├── connection.cpp            # 新增
│   └── connection_manager.cpp    # 新增
└── tests/
    ├── helpers/
    │   ├── test_socket_utils.h
    │   └── test_connection_harness.h   # ID+generation版直接Connection测试辅助
    ├── unit/
    │   ├── ...阶段0～3已有测试
    │   ├── buffer_test.cpp
    │   └── connection_test.cpp
    ├── integration/
    │   ├── listener_event_loop_test.cpp
    │   ├── multi_connection_proxy_test.cpp
    │   ├── backpressure_test.cpp
    │   ├── half_close_test.cpp
    │   ├── connection_error_test.cpp
    │   └── connection_generation_test.cpp # id复用/generation/stale token回归
    └── manual/
        ├── m3_smoke.cpp
        └── manager_reset_smoke.cpp
```

### 为什么不继续拆目录

目前网络核心公开模块仍然只有个位数到十个左右，`include/chaosproxy/*.h` 足够清晰。现在拆成 `net/`、`reactor/`、`proxy/` 会制造 include 路径和重构噪音，却没有明显收益。等阶段8以后 Toxic/Timer/Config 模块明显增多，再评估是否按职责分层。

---

## 3. 新对象关系与所有权（ID + generation 修订版）

```text
Listener
   │ accepted_callback(move client_fd)
   ▼
ConnectionManager
   │ 唯一拥有 unique_ptr<ConnectionPair>
   │
   ├─ Slot[id=1, generation=4] → ConnectionPair
   ├─ Slot[id=2, generation=9] → ConnectionPair
   └─ free_ids：回收后允许复用ConnectionId

ConnectionPair
├── client Endpoint
│    └── UniqueFd(client_fd)
├── upstream Endpoint
│    └── UniqueFd(upstream_fd)
├── client_to_upstream Direction
│    └── Buffer pending
└── upstream_to_client Direction
     └── Buffer pending

EventLoop
└── 只借用fd
    └── callback长期保存：ConnectionToken{id,generation} + EndpointSide
```

### 为什么改成 `ConnectionId + generation`

单独的 `ConnectionId` 与 fd 一样，都可能复用。假设：

```text
Connection A = {id=7, generation=3}
A关闭，slot 7回收到free_ids
Connection B复用slot 7 = {id=7, generation=4}
```

A 留下的旧 EventLoop callback 或未来 Timer callback 即使随后执行，携带的仍是 `{7,3}`。`ConnectionManager::Find({7,3})` 会看到 slot 7 当前 generation 已经是 4，因此直接返回 `nullptr`，不会误操作 B。

因此长期异步上下文只保存：

```cpp
ConnectionToken {
    ConnectionId id;
    ConnectionGeneration generation;
};
```

而不是保存：

```cpp
ConnectionPair*          // 可能悬空
std::shared_ptr<...>     // 会改变对象生命周期
std::weak_ptr<...>       // 本项目不再采用这套身份模型
```

### Manager 为什么使用 `unique_ptr`

`ConnectionManager` 是连接对象唯一逻辑拥有者：

```text
Manager slot
→ unique_ptr<ConnectionPair>
```

EventLoop 不拥有 ConnectionPair。事件到达时：

```text
EventLoop callback
→ 带 ConnectionToken{id,generation}
→ ConnectionManager::DispatchEvent()
→ Find(token)
→ id存在？
→ generation仍一致？
→ 是：临时借用 ConnectionPair*
→ OnEvent()
```

借用指针只在当前同步调用期间使用，不能跨 EventLoop turn 保存。

### 为什么需要 `pending_destroy`

使用 `unique_ptr` 后不能在 `ConnectionPair::Close()` 的关闭回调里立刻：

```cpp
slot.connection.reset();
```

因为 `Close()` 很可能正发生在：

```text
ConnectionPair::OnEvent()
→ HandleReadable / HandleWritable
→ Close()
```

如果 closed callback 当场析构 ConnectionPair，就会形成：

```text
成员函数还没有返回
但 this 已经被释放
→ use-after-free
```

因此回收分两步：

```text
ConnectionPair::Close()
→ Cleanup token/fd
→ Manager::OnClosed(token)
→ 只设置 pending_destroy=true

OnEvent()返回到ConnectionManager::DispatchEvent()
→ CollectClosed(token)
→ 此时才 unique_ptr::reset()
```

这就是 ID+generation 版本替代旧“shared_ptr临时保活”的生命周期闭环。

## 4. 阶段4：Endpoint / ConnectionPair / ConnectionManager

### Endpoint职责

`Endpoint`只表示一个Socket端点：

```text
UniqueFd
+ EndpointSide(client/upstream)
+ EndpointState(connecting/established/closed)
+ EventToken
+ 当前epoll interests
```

它不保存双向转发数据；转发数据属于 `Direction`。

### Direction职责

每个 `ConnectionPair` 固定拥有两个Direction：

```text
client_to_upstream
upstream_to_client
```

每个Direction保存：

```text
source
 destination
 pending
 source_eof
 write_shutdown
 read_paused
```

### 上游建连期间

当前选择：允许 client 在 upstream `Connecting` 期间继续读取，但只能进入有界 `client_to_upstream.pending`。如果达到高水位就暂停 client 的 `EPOLLIN`。上游建连完成后再 flush；如果建连失败，则整个 ConnectionPair 进入错误清理。

这比“建连前完全不读client”更能吸收短暂建连延迟，同时仍由固定容量Buffer保证内存不会无限增长。

---

## 5. 阶段5：固定容量Buffer、短写与背压

### Buffer选择

第一版使用固定容量环形Buffer，而不是会动态扩容的 `std::vector` 线性队列。

理由：

1. 每个Direction的排队字节有物理上限；
2. Append/Consume不需要频繁搬移全部数据；
3. `send()`只需要读取当前头部连续区间；
4. 环绕后最多分两段发送，下一次循环自然处理第二段；
5. 第一版不引入Buffer Pool。

### 动态EPOLLOUT

```text
pending为空
→ destination不订阅EPOLLOUT

产生pending
→ 先立即TryWrite

短写/EAGAIN后仍有pending
→ destination订阅EPOLLOUT

EPOLLOUT到达
→ 继续flush

pending排空
→ 取消EPOLLOUT
```

### 高低水位

默认：

```text
capacity = 256 KiB
high     = 192 KiB
low      = 128 KiB
```

状态变化：

```text
pending >= high
→ read_paused = true
→ source取消EPOLLIN

pending <= low
→ read_paused = false
→ source恢复EPOLLIN
```

高、低两个阈值分离是为了避免队列大小在单一阈值附近来回波动时频繁 `epoll_ctl(MOD)`。

### 公平性预算

当前EventLoop仍然沿用阶段3的LT模式。每次事件增加：

```text
read_budget_per_event
write_budget_per_event
```

一个大连接达到本轮预算后返回EventLoop，让其他ready fd获得处理机会。因为当前是LT，如果Socket仍可读/可写，下一轮仍会再次通知。

**重要边界：如果未来切换ET，不能在未读写到EAGAIN时仅靠预算直接返回；届时必须重新设计requeue/oneshot等机制。**

---

## 6. 阶段6：FIN、半关闭、RST与幂等回收

### 正常EOF路径

```text
TryRead(source) == kEof
↓
source_eof = true
↓
停止source继续读取
↓
保留pending
↓
继续flush destination
↓
pending.empty()
↓
shutdown(destination, SHUT_WR)
↓
write_shutdown = true
```

核心不变量：

```text
EOF ≠ close
EOF → drain pending → SHUT_WR
```

### ConnectionPair正常完成

只有两个Direction都满足：

```text
source_eof
&& pending.empty()
&& write_shutdown
```

才执行：

```text
Close(kGracefulEof)
→ EventLoop::Remove两个token
→ UniqueFd关闭两个fd
→ Manager删除ConnectionPair
```

### 异常路径

```text
ECONNRESET / EPIPE
→ kConnectionReset

upstream非阻塞connect失败
→ kConnectFailed

其他recv错误
→ kReadError

其他send错误
→ kWriteError

EPOLLERR
→ SO_ERROR
→ kSocketError / kConnectionReset
```

异常路径默认整体结束 ConnectionPair，不再假装成正常半关闭。

### Close幂等

第一次 `Close(reason)`：

```text
记录reason
Remove token
close fd
通知Manager
```

第二次及以后：直接返回。

这同时防止重复 `epoll_ctl(DEL)`、重复fd关闭和重复Manager删除。

---

## 7. 完整CMake

### `CMakeLists.txt`

```cmake
cmake_minimum_required(VERSION 3.16)

project(
    ChaosProxy
    VERSION 0.1.0
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(chaosproxy_core
    src/app.cpp
    src/socket_ops.cpp
    src/event_loop.cpp
    src/listener.cpp
    src/buffer.cpp
    src/endpoint.cpp
    src/connection.cpp
    src/connection_manager.cpp
)

target_include_directories(chaosproxy_core
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

target_compile_definitions(chaosproxy_core
    PRIVATE
        _GNU_SOURCE
)

target_compile_options(chaosproxy_core
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

add_executable(chaosproxy
    src/main.cpp
)

target_link_libraries(chaosproxy
    PRIVATE
        chaosproxy_core
)

include(CTest)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)

    add_executable(chaosproxy_tests
        tests/unit/app_test.cpp
        tests/unit/unique_fd_test.cpp
        tests/unit/socket_ops_test.cpp
        tests/unit/event_loop_test.cpp
        tests/unit/buffer_test.cpp
        tests/unit/connection_test.cpp
        tests/integration/listener_event_loop_test.cpp
        tests/integration/multi_connection_proxy_test.cpp
        tests/integration/backpressure_test.cpp
        tests/integration/half_close_test.cpp
        tests/integration/connection_error_test.cpp
        tests/integration/connection_generation_test.cpp
    )

    target_include_directories(chaosproxy_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/tests
    )

    target_compile_definitions(chaosproxy_tests
        PRIVATE
            _GNU_SOURCE
    )

    target_compile_options(chaosproxy_tests
        PRIVATE
            -Wall
            -Wextra
            -Wpedantic
    )

    target_link_libraries(chaosproxy_tests
        PRIVATE
            chaosproxy_core
            GTest::gtest_main
    )

    include(GoogleTest)
    gtest_discover_tests(chaosproxy_tests)
endif()
```

---

## 8. 阶段4～6新增生产代码

### `include/chaosproxy/buffer.h`

```cpp
#pragma once

#include <cstddef>
#include <vector>

namespace chaosproxy {

class Buffer final {
public:
    explicit Buffer(std::size_t capacity);

    [[nodiscard]] std::size_t Size() const noexcept;
    [[nodiscard]] std::size_t Capacity() const noexcept;
    [[nodiscard]] std::size_t Available() const noexcept;
    [[nodiscard]] bool Empty() const noexcept;

    [[nodiscard]] const char* FrontData() const noexcept;
    [[nodiscard]] std::size_t FrontSize() const noexcept;

    [[nodiscard]] bool Append(
        const void* data,
        std::size_t size) noexcept;

    void Consume(std::size_t size) noexcept;
    void Clear() noexcept;

private:
    std::vector<char> storage_;
    std::size_t head_{0};
    std::size_t size_{0};
};

}  // namespace chaosproxy
```

### `src/buffer.cpp`

```cpp
#include "chaosproxy/buffer.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace chaosproxy {

Buffer::Buffer(std::size_t capacity)
    : storage_(capacity) {
    if (capacity == 0) {
        throw std::invalid_argument("buffer capacity must be positive");
    }
}

std::size_t Buffer::Size() const noexcept {
    return size_;
}

std::size_t Buffer::Capacity() const noexcept {
    return storage_.size();
}

std::size_t Buffer::Available() const noexcept {
    return Capacity() - size_;
}

bool Buffer::Empty() const noexcept {
    return size_ == 0;
}

const char* Buffer::FrontData() const noexcept {
    if (Empty()) {
        return nullptr;
    }
    return storage_.data() + head_;
}

std::size_t Buffer::FrontSize() const noexcept {
    if (Empty()) {
        return 0;
    }

    return std::min(size_, Capacity() - head_);
}

bool Buffer::Append(
    const void* data,
    std::size_t size) noexcept {
    if (size == 0) {
        return true;
    }

    if (data == nullptr || size > Available()) {
        return false;
    }

    const auto* bytes = static_cast<const char*>(data);
    const std::size_t tail = (head_ + size_) % Capacity();
    const std::size_t first = std::min(size, Capacity() - tail);

    std::memcpy(storage_.data() + tail, bytes, first);

    const std::size_t second = size - first;
    if (second > 0) {
        std::memcpy(storage_.data(), bytes + first, second);
    }

    size_ += size;
    return true;
}

void Buffer::Consume(std::size_t size) noexcept {
    const std::size_t consumed = std::min(size, size_);
    head_ = (head_ + consumed) % Capacity();
    size_ -= consumed;

    if (size_ == 0) {
        head_ = 0;
    }
}

void Buffer::Clear() noexcept {
    head_ = 0;
    size_ = 0;
}

}  // namespace chaosproxy
```

### `include/chaosproxy/endpoint.h`

```cpp
#pragma once

#include "chaosproxy/event_loop.h"
#include "chaosproxy/unique_fd.h"

#include <cstdint>

namespace chaosproxy {

enum class EndpointSide {
    kClient,
    kUpstream
};

enum class EndpointState {
    kConnecting,
    kEstablished,
    kClosed
};

class Endpoint final {
public:
    Endpoint(
        EndpointSide side,
        UniqueFd fd,
        EndpointState state) noexcept;

    [[nodiscard]] EndpointSide Side() const noexcept;
    [[nodiscard]] EndpointState State() const noexcept;
    void SetState(EndpointState state) noexcept;

    [[nodiscard]] int Fd() const noexcept;
    [[nodiscard]] bool HasFd() const noexcept;
    void CloseFd() noexcept;

    [[nodiscard]] EventToken Token() const noexcept;
    void SetToken(EventToken token) noexcept;

    [[nodiscard]] std::uint32_t Interests() const noexcept;
    void SetInterests(std::uint32_t interests) noexcept;

private:
    EndpointSide side_;
    UniqueFd fd_;
    EndpointState state_;
    EventToken token_{0};
    std::uint32_t interests_{0};
};

}  // namespace chaosproxy
```

### `src/endpoint.cpp`

```cpp
#include "chaosproxy/endpoint.h"

#include <utility>

namespace chaosproxy {

Endpoint::Endpoint(
    EndpointSide side,
    UniqueFd fd,
    EndpointState state) noexcept
    : side_(side),
      fd_(std::move(fd)),
      state_(state) {}

EndpointSide Endpoint::Side() const noexcept {
    return side_;
}

EndpointState Endpoint::State() const noexcept {
    return state_;
}

void Endpoint::SetState(EndpointState state) noexcept {
    state_ = state;
}

int Endpoint::Fd() const noexcept {
    return fd_.Get();
}

bool Endpoint::HasFd() const noexcept {
    return fd_.IsValid();
}

void Endpoint::CloseFd() noexcept {
    fd_.Reset();
    state_ = EndpointState::kClosed;
}

EventToken Endpoint::Token() const noexcept {
    return token_;
}

void Endpoint::SetToken(EventToken token) noexcept {
    token_ = token;
}

std::uint32_t Endpoint::Interests() const noexcept {
    return interests_;
}

void Endpoint::SetInterests(std::uint32_t interests) noexcept {
    interests_ = interests;
}

}  // namespace chaosproxy
```

### `include/chaosproxy/connection.h`

```cpp
#pragma once

#include "chaosproxy/buffer.h"
#include "chaosproxy/endpoint.h"
#include "chaosproxy/event_loop.h"
#include "chaosproxy/unique_fd.h"

#include <cstddef>
#include <cstdint>
#include <functional>

namespace chaosproxy {

using ConnectionId = std::uint64_t;
using ConnectionGeneration = std::uint64_t;

struct ConnectionToken {
    ConnectionId id{0};
    ConnectionGeneration generation{0};

    [[nodiscard]] bool IsValid() const noexcept {
        return id != 0 && generation != 0;
    }
};

inline bool operator==(
    const ConnectionToken& lhs,
    const ConnectionToken& rhs) noexcept {
    return lhs.id == rhs.id && lhs.generation == rhs.generation;
}

inline bool operator!=(
    const ConnectionToken& lhs,
    const ConnectionToken& rhs) noexcept {
    return !(lhs == rhs);
}

enum class CloseReason {
    kNone,
    kGracefulEof,
    kConnectFailed,
    kConnectionReset,
    kReadError,
    kWriteError,
    kSocketError,
    kShutdownError,
    kLocalStop,
    kInternalError
};

struct ConnectionLimits {
    std::size_t buffer_capacity{256 * 1024};
    std::size_t high_watermark{192 * 1024};
    std::size_t low_watermark{128 * 1024};
    std::size_t read_budget_per_event{64 * 1024};
    std::size_t write_budget_per_event{64 * 1024};
};

class ConnectionManager;

class ConnectionPair final {
public:
    using ClosedCallback =
        std::function<void(ConnectionToken, CloseReason)>;

    using EventDispatchCallback = std::function<void(
        ConnectionToken,
        EndpointSide,
        EventToken,
        std::uint32_t)>;

    ConnectionPair(
        EventLoop& loop,
        ConnectionToken token,
        UniqueFd client_fd,
        UniqueFd upstream_fd,
        EndpointState upstream_state,
        ConnectionLimits limits,
        EventDispatchCallback event_dispatch_callback,
        ClosedCallback closed_callback);

    ~ConnectionPair() noexcept;

    ConnectionPair(const ConnectionPair&) = delete;
    ConnectionPair& operator=(const ConnectionPair&) = delete;

    void Start();
    void Close(CloseReason reason) noexcept;

    [[nodiscard]] ConnectionToken Token() const noexcept;
    [[nodiscard]] ConnectionId Id() const noexcept;
    [[nodiscard]] ConnectionGeneration Generation() const noexcept;
    [[nodiscard]] bool IsStarted() const noexcept;
    [[nodiscard]] bool IsClosed() const noexcept;
    [[nodiscard]] CloseReason Reason() const noexcept;

    [[nodiscard]] std::size_t PendingBytes(
        EndpointSide source) const noexcept;

    [[nodiscard]] bool ReadPaused(
        EndpointSide source) const noexcept;

    [[nodiscard]] bool SourceEof(
        EndpointSide source) const noexcept;

    [[nodiscard]] bool WriteShutdownFor(
        EndpointSide source) const noexcept;

    // EventLoop-facing endpoint handler. Production code must call this only
    // through ConnectionManager after validating ConnectionToken.
    void OnEvent(
        EndpointSide side,
        EventToken token,
        std::uint32_t events) noexcept;

private:
    struct Direction {
        Direction(
            EndpointSide source_side,
            EndpointSide destination_side,
            std::size_t capacity)
            : source(source_side),
              destination(destination_side),
              pending(capacity) {}

        EndpointSide source;
        EndpointSide destination;
        Buffer pending;
        bool source_eof{false};
        bool write_shutdown{false};
        bool read_paused{false};
    };

    void HandleEventImpl(
        EndpointSide side,
        EventToken token,
        std::uint32_t events);

    bool FinishUpstreamConnect();
    void HandleReadable(EndpointSide source_side);
    void HandleWritable(EndpointSide destination_side);

    void FlushDirection(
        Direction& direction,
        std::size_t& write_budget);

    void UpdateBackpressure(Direction& direction) noexcept;
    void TryFinishDirection(Direction& direction);
    void MaybeFinishConnection();

    [[nodiscard]] Endpoint& GetEndpoint(EndpointSide side) noexcept;
    [[nodiscard]] const Endpoint& GetEndpoint(EndpointSide side) const noexcept;

    [[nodiscard]] Direction& SourceDirection(
        EndpointSide source) noexcept;

    [[nodiscard]] const Direction& SourceDirection(
        EndpointSide source) const noexcept;

    [[nodiscard]] Direction& DestinationDirection(
        EndpointSide destination) noexcept;

    [[nodiscard]] const Direction& DestinationDirection(
        EndpointSide destination) const noexcept;

    [[nodiscard]] std::uint32_t DesiredInterests(
        EndpointSide side) const noexcept;

    void RefreshInterests();
    void RefreshEndpointInterests(Endpoint& endpoint);

    [[nodiscard]] bool ShutdownWrite(Direction& direction) noexcept;
    void Cleanup() noexcept;

    EventLoop& loop_;
    ConnectionToken token_;
    ConnectionLimits limits_;
    EventDispatchCallback event_dispatch_callback_;
    ClosedCallback closed_callback_;

    Endpoint client_;
    Endpoint upstream_;

    Direction client_to_upstream_;
    Direction upstream_to_client_;

    bool started_{false};
    bool closed_{false};
    CloseReason close_reason_{CloseReason::kNone};
};

}  // namespace chaosproxy
```

### `src/connection.cpp`

```cpp
#include "chaosproxy/connection.h"

#include "chaosproxy/socket_ops.h"

#include <sys/epoll.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <stdexcept>
#include <utility>

namespace chaosproxy {
namespace {

constexpr std::size_t kReadChunkSize = 16 * 1024;

void ValidateLimits(const ConnectionLimits& limits) {
    if (limits.buffer_capacity == 0 ||
        limits.low_watermark >= limits.high_watermark ||
        limits.high_watermark > limits.buffer_capacity ||
        limits.read_budget_per_event == 0 ||
        limits.write_budget_per_event == 0) {
        throw std::invalid_argument("invalid connection limits");
    }
}

CloseReason ReadErrorReason(int error_number) noexcept {
    return error_number == ECONNRESET
        ? CloseReason::kConnectionReset
        : CloseReason::kReadError;
}

CloseReason WriteErrorReason(int error_number) noexcept {
    return error_number == ECONNRESET ||
           error_number == EPIPE
        ? CloseReason::kConnectionReset
        : CloseReason::kWriteError;
}

}  // namespace

ConnectionPair::ConnectionPair(
    EventLoop& loop,
    ConnectionToken token,
    UniqueFd client_fd,
    UniqueFd upstream_fd,
    EndpointState upstream_state,
    ConnectionLimits limits,
    EventDispatchCallback event_dispatch_callback,
    ClosedCallback closed_callback)
    : loop_(loop),
      token_(token),
      limits_(limits),
      event_dispatch_callback_(std::move(event_dispatch_callback)),
      closed_callback_(std::move(closed_callback)),
      client_(
          EndpointSide::kClient,
          std::move(client_fd),
          EndpointState::kEstablished),
      upstream_(
          EndpointSide::kUpstream,
          std::move(upstream_fd),
          upstream_state),
      client_to_upstream_(
          EndpointSide::kClient,
          EndpointSide::kUpstream,
          limits.buffer_capacity),
      upstream_to_client_(
          EndpointSide::kUpstream,
          EndpointSide::kClient,
          limits.buffer_capacity) {
    ValidateLimits(limits_);

    if (!token_.IsValid() ||
        !client_.HasFd() ||
        !upstream_.HasFd() ||
        !event_dispatch_callback_ ||
        !closed_callback_) {
        throw std::invalid_argument("invalid connection arguments");
    }

    if (upstream_state != EndpointState::kConnecting &&
        upstream_state != EndpointState::kEstablished) {
        throw std::invalid_argument("invalid upstream state");
    }
}

ConnectionPair::~ConnectionPair() noexcept {
    Cleanup();
}

void ConnectionPair::Start() {
    if (started_ || closed_) {
        return;
    }

    const ConnectionToken connection_token = token_;
    const EventDispatchCallback dispatch = event_dispatch_callback_;

    const auto add_endpoint =
        [&](Endpoint& endpoint) {
            const EndpointSide side = endpoint.Side();
            const std::uint32_t interests = DesiredInterests(side);

            const EventToken event_token = loop_.Add(
                endpoint.Fd(),
                interests,
                [connection_token, side, dispatch](
                    EventToken observed_token,
                    std::uint32_t events) {
                    dispatch(
                        connection_token,
                        side,
                        observed_token,
                        events);
                });

            endpoint.SetToken(event_token);
            endpoint.SetInterests(interests);
        };

    try {
        add_endpoint(client_);
        add_endpoint(upstream_);
        started_ = true;
    } catch (...) {
        Cleanup();
        throw;
    }
}

void ConnectionPair::Close(CloseReason reason) noexcept {
    if (closed_) {
        return;
    }

    closed_ = true;
    close_reason_ = reason;
    Cleanup();

    try {
        closed_callback_(token_, reason);
    } catch (...) {
        // Closing a connection must not throw back into EventLoop.
    }
}

ConnectionToken ConnectionPair::Token() const noexcept {
    return token_;
}

ConnectionId ConnectionPair::Id() const noexcept {
    return token_.id;
}

ConnectionGeneration ConnectionPair::Generation() const noexcept {
    return token_.generation;
}

bool ConnectionPair::IsStarted() const noexcept {
    return started_;
}

bool ConnectionPair::IsClosed() const noexcept {
    return closed_;
}

CloseReason ConnectionPair::Reason() const noexcept {
    return close_reason_;
}

std::size_t ConnectionPair::PendingBytes(
    EndpointSide source) const noexcept {
    return SourceDirection(source).pending.Size();
}

bool ConnectionPair::ReadPaused(
    EndpointSide source) const noexcept {
    return SourceDirection(source).read_paused;
}

bool ConnectionPair::SourceEof(
    EndpointSide source) const noexcept {
    return SourceDirection(source).source_eof;
}

bool ConnectionPair::WriteShutdownFor(
    EndpointSide source) const noexcept {
    return SourceDirection(source).write_shutdown;
}

void ConnectionPair::OnEvent(
    EndpointSide side,
    EventToken token,
    std::uint32_t events) noexcept {
    if (closed_) {
        return;
    }

    try {
        HandleEventImpl(side, token, events);
    } catch (...) {
        Close(CloseReason::kInternalError);
    }
}

void ConnectionPair::HandleEventImpl(
    EndpointSide side,
    EventToken token,
    std::uint32_t events) {
    Endpoint& endpoint = GetEndpoint(side);

    if (token == 0 || token != endpoint.Token()) {
        return;
    }

    if (endpoint.State() == EndpointState::kConnecting) {
        if ((events & (EPOLLOUT | EPOLLERR | EPOLLHUP)) != 0U) {
            if (!FinishUpstreamConnect()) {
                return;
            }
        } else {
            return;
        }
    }

    if (closed_) {
        return;
    }

    if ((events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) != 0U) {
        HandleReadable(side);
    }

    if (closed_) {
        return;
    }

    if ((events & EPOLLERR) != 0U) {
        const int socket_error = GetPendingSocketError(endpoint.Fd());
        Close(
            socket_error == ECONNRESET
                ? CloseReason::kConnectionReset
                : CloseReason::kSocketError);
        return;
    }

    if ((events & EPOLLOUT) != 0U) {
        HandleWritable(side);
    }

    if (closed_) {
        return;
    }

    RefreshInterests();
}

bool ConnectionPair::FinishUpstreamConnect() {
    if (upstream_.State() != EndpointState::kConnecting) {
        return true;
    }

    const ConnectResult result = CompleteConnect(upstream_.Fd());

    if (result.status == ConnectStatus::kInProgress) {
        RefreshEndpointInterests(upstream_);
        return false;
    }

    if (result.status == ConnectStatus::kError) {
        Close(
            result.error_number == ECONNRESET
                ? CloseReason::kConnectionReset
                : CloseReason::kConnectFailed);
        return false;
    }

    upstream_.SetState(EndpointState::kEstablished);

    std::size_t write_budget = limits_.write_budget_per_event;
    FlushDirection(client_to_upstream_, write_budget);

    if (closed_) {
        return false;
    }

    TryFinishDirection(client_to_upstream_);
    if (closed_) {
        return false;
    }

    RefreshInterests();
    return true;
}

void ConnectionPair::HandleReadable(EndpointSide source_side) {
    Direction& direction = SourceDirection(source_side);

    if (direction.source_eof || direction.read_paused) {
        return;
    }

    Endpoint& source = GetEndpoint(source_side);
    if (source.State() != EndpointState::kEstablished) {
        return;
    }

    std::array<char, kReadChunkSize> buffer{};
    std::size_t read_budget = limits_.read_budget_per_event;
    std::size_t write_budget = limits_.write_budget_per_event;

    while (read_budget > 0 &&
           !direction.source_eof &&
           !direction.read_paused) {
        const std::size_t available = direction.pending.Available();
        if (available == 0) {
            direction.read_paused = true;
            break;
        }

        const std::size_t capacity = std::min(
            {buffer.size(), available, read_budget});

        const ReadResult result = TryRead(
            source.Fd(),
            buffer.data(),
            capacity);

        if (result.status == ReadStatus::kData) {
            if (!direction.pending.Append(
                    buffer.data(),
                    result.bytes_transferred)) {
                Close(CloseReason::kInternalError);
                return;
            }

            read_budget -= result.bytes_transferred;
            UpdateBackpressure(direction);
            FlushDirection(direction, write_budget);

            if (closed_) {
                return;
            }

            UpdateBackpressure(direction);
            continue;
        }

        if (result.status == ReadStatus::kEof) {
            direction.source_eof = true;
            direction.read_paused = true;
            TryFinishDirection(direction);
            break;
        }

        if (result.status == ReadStatus::kWouldBlock) {
            break;
        }

        Close(ReadErrorReason(result.error_number));
        return;
    }

    UpdateBackpressure(direction);
    TryFinishDirection(direction);
    MaybeFinishConnection();
}

void ConnectionPair::HandleWritable(
    EndpointSide destination_side) {
    Direction& direction = DestinationDirection(destination_side);

    std::size_t write_budget = limits_.write_budget_per_event;
    FlushDirection(direction, write_budget);

    if (closed_) {
        return;
    }

    UpdateBackpressure(direction);
    TryFinishDirection(direction);
    MaybeFinishConnection();
}

void ConnectionPair::FlushDirection(
    Direction& direction,
    std::size_t& write_budget) {
    Endpoint& destination = GetEndpoint(direction.destination);

    if (destination.State() != EndpointState::kEstablished ||
        direction.write_shutdown) {
        return;
    }

    while (!direction.pending.Empty() && write_budget > 0) {
        const std::size_t to_write = std::min(
            direction.pending.FrontSize(),
            write_budget);

        const WriteResult result = TryWrite(
            destination.Fd(),
            direction.pending.FrontData(),
            to_write);

        if (result.status == WriteStatus::kWritten) {
            direction.pending.Consume(result.bytes_transferred);
            write_budget -= result.bytes_transferred;
            continue;
        }

        if (result.status == WriteStatus::kWouldBlock) {
            break;
        }

        Close(WriteErrorReason(result.error_number));
        return;
    }
}

void ConnectionPair::UpdateBackpressure(
    Direction& direction) noexcept {
    if (direction.source_eof) {
        direction.read_paused = true;
        return;
    }

    if (!direction.read_paused &&
        direction.pending.Size() >= limits_.high_watermark) {
        direction.read_paused = true;
        return;
    }

    if (direction.read_paused &&
        direction.pending.Size() <= limits_.low_watermark) {
        direction.read_paused = false;
    }
}

void ConnectionPair::TryFinishDirection(Direction& direction) {
    if (!direction.source_eof ||
        !direction.pending.Empty() ||
        direction.write_shutdown) {
        return;
    }

    Endpoint& destination = GetEndpoint(direction.destination);

    if (destination.State() == EndpointState::kConnecting) {
        return;
    }

    if (destination.State() != EndpointState::kEstablished) {
        return;
    }

    if (!ShutdownWrite(direction)) {
        Close(CloseReason::kShutdownError);
        return;
    }

    direction.write_shutdown = true;
}

void ConnectionPair::MaybeFinishConnection() {
    const auto finished = [](const Direction& direction) {
        return direction.source_eof &&
               direction.pending.Empty() &&
               direction.write_shutdown;
    };

    if (finished(client_to_upstream_) &&
        finished(upstream_to_client_)) {
        Close(CloseReason::kGracefulEof);
    }
}

Endpoint& ConnectionPair::GetEndpoint(
    EndpointSide side) noexcept {
    return side == EndpointSide::kClient
        ? client_
        : upstream_;
}

const Endpoint& ConnectionPair::GetEndpoint(
    EndpointSide side) const noexcept {
    return side == EndpointSide::kClient
        ? client_
        : upstream_;
}

ConnectionPair::Direction& ConnectionPair::SourceDirection(
    EndpointSide source) noexcept {
    return source == EndpointSide::kClient
        ? client_to_upstream_
        : upstream_to_client_;
}

const ConnectionPair::Direction& ConnectionPair::SourceDirection(
    EndpointSide source) const noexcept {
    return source == EndpointSide::kClient
        ? client_to_upstream_
        : upstream_to_client_;
}

ConnectionPair::Direction& ConnectionPair::DestinationDirection(
    EndpointSide destination) noexcept {
    return destination == EndpointSide::kUpstream
        ? client_to_upstream_
        : upstream_to_client_;
}

const ConnectionPair::Direction& ConnectionPair::DestinationDirection(
    EndpointSide destination) const noexcept {
    return destination == EndpointSide::kUpstream
        ? client_to_upstream_
        : upstream_to_client_;
}

std::uint32_t ConnectionPair::DesiredInterests(
    EndpointSide side) const noexcept {
    const Endpoint& endpoint = GetEndpoint(side);

    if (endpoint.State() == EndpointState::kClosed) {
        return 0;
    }

    if (endpoint.State() == EndpointState::kConnecting) {
        return EPOLLOUT;
    }

    std::uint32_t interests = 0;

    const Direction& read_direction = SourceDirection(side);
    if (!read_direction.source_eof &&
        !read_direction.read_paused) {
        interests |= EPOLLIN | EPOLLRDHUP;
    }

    const Direction& write_direction = DestinationDirection(side);
    if (!write_direction.write_shutdown &&
        !write_direction.pending.Empty()) {
        interests |= EPOLLOUT;
    }

    return interests;
}

void ConnectionPair::RefreshInterests() {
    RefreshEndpointInterests(client_);
    if (!closed_) {
        RefreshEndpointInterests(upstream_);
    }
}

void ConnectionPair::RefreshEndpointInterests(Endpoint& endpoint) {
    if (closed_ || endpoint.Token() == 0) {
        return;
    }

    const std::uint32_t desired = DesiredInterests(endpoint.Side());
    if (desired == endpoint.Interests()) {
        return;
    }

    loop_.Modify(endpoint.Token(), desired);
    endpoint.SetInterests(desired);
}

bool ConnectionPair::ShutdownWrite(Direction& direction) noexcept {
    Endpoint& destination = GetEndpoint(direction.destination);

    for (;;) {
        if (::shutdown(destination.Fd(), SHUT_WR) == 0) {
            return true;
        }

        const int error_number = errno;
        if (error_number == EINTR) {
            continue;
        }

        if (error_number == ENOTCONN ||
            error_number == EPIPE) {
            return false;
        }

        return false;
    }
}

void ConnectionPair::Cleanup() noexcept {
    if (client_.Token() != 0) {
        (void)loop_.Remove(client_.Token());
        client_.SetToken(0);
    }

    if (upstream_.Token() != 0) {
        (void)loop_.Remove(upstream_.Token());
        upstream_.SetToken(0);
    }

    client_.SetInterests(0);
    upstream_.SetInterests(0);

    client_.CloseFd();
    upstream_.CloseFd();
    started_ = false;
}

}  // namespace chaosproxy
```

### `include/chaosproxy/connection_manager.h`

```cpp
#pragma once

#include "chaosproxy/connection.h"
#include "chaosproxy/event_loop.h"
#include "chaosproxy/unique_fd.h"

#include <sys/socket.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chaosproxy {

struct CreateConnectionResult {
    ConnectionToken token{};
    int error_number{0};

    [[nodiscard]] bool Ok() const noexcept {
        return token.IsValid() && error_number == 0;
    }
};

class ConnectionManager final {
public:
    ConnectionManager(
        EventLoop& loop,
        const sockaddr* upstream_address,
        socklen_t upstream_address_length,
        ConnectionLimits limits = {});

    ~ConnectionManager() noexcept;

    ConnectionManager(const ConnectionManager&) = delete;
    ConnectionManager& operator=(const ConnectionManager&) = delete;

    [[nodiscard]] CreateConnectionResult Create(
        UniqueFd client_fd);

    [[nodiscard]] bool Close(
        ConnectionToken token,
        CloseReason reason = CloseReason::kLocalStop) noexcept;

    void CloseAll(
        CloseReason reason = CloseReason::kLocalStop) noexcept;

    [[nodiscard]] std::size_t Size() const noexcept;

    // Returned pointer is borrowed. It is valid only until the manager next
    // closes/reclaims this token. Callers must not retain it across EventLoop
    // turns or timers; retain ConnectionToken instead.
    [[nodiscard]] ConnectionPair* Find(
        ConnectionToken token) noexcept;

    [[nodiscard]] const ConnectionPair* Find(
        ConnectionToken token) const noexcept;

private:
    struct Slot {
        ConnectionGeneration generation{0};
        std::unique_ptr<ConnectionPair> connection;
        bool pending_destroy{false};
    };

    [[nodiscard]] ConnectionToken AllocateToken();
    void ReleaseSlot(ConnectionToken token) noexcept;
    void CollectClosed(ConnectionToken token) noexcept;

    [[nodiscard]] Slot* FindSlot(ConnectionToken token) noexcept;
    [[nodiscard]] const Slot* FindSlot(ConnectionToken token) const noexcept;

    void DispatchEvent(
        ConnectionToken connection_token,
        EndpointSide side,
        EventToken event_token,
        std::uint32_t events) noexcept;

    void OnClosed(
        ConnectionToken token,
        CloseReason reason) noexcept;

    EventLoop& loop_;
    sockaddr_storage upstream_address_{};
    socklen_t upstream_address_length_{0};
    ConnectionLimits limits_;

    // Slot index + 1 is ConnectionId. Closed slots are reused; generation is
    // incremented on every reuse so stale EventLoop/timer callbacks cannot
    // resolve a new connection that happens to reuse the same id.
    std::vector<Slot> slots_;
    std::vector<ConnectionId> free_ids_;
    std::size_t active_count_{0};
};

}  // namespace chaosproxy
```

### `src/connection_manager.cpp`

```cpp
#include "chaosproxy/connection_manager.h"

#include "chaosproxy/socket_ops.h"

#include <sys/socket.h>

#include <cerrno>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>

namespace chaosproxy {
namespace {

ConnectionGeneration NextGeneration(
    ConnectionGeneration current) noexcept {
    ++current;
    if (current == 0) {
        ++current;
    }
    return current;
}

}  // namespace

ConnectionManager::ConnectionManager(
    EventLoop& loop,
    const sockaddr* upstream_address,
    socklen_t upstream_address_length,
    ConnectionLimits limits)
    : loop_(loop),
      upstream_address_length_(upstream_address_length),
      limits_(limits) {
    if (upstream_address == nullptr ||
        upstream_address_length == 0 ||
        static_cast<std::size_t>(upstream_address_length) >
            sizeof(upstream_address_)) {
        throw std::invalid_argument("invalid upstream address");
    }

    std::memcpy(
        &upstream_address_,
        upstream_address,
        upstream_address_length);
}

ConnectionManager::~ConnectionManager() noexcept {
    CloseAll(CloseReason::kLocalStop);
}

CreateConnectionResult ConnectionManager::Create(
    UniqueFd client_fd) {
    if (!client_fd.IsValid()) {
        return {{}, EBADF};
    }

    const int family = upstream_address_.ss_family;
    UniqueFd upstream_fd(::socket(
        family,
        SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
        0));

    if (!upstream_fd.IsValid()) {
        return {{}, errno};
    }

    const ConnectResult connect_result = StartConnect(
        upstream_fd.Get(),
        reinterpret_cast<const sockaddr*>(&upstream_address_),
        upstream_address_length_);

    if (connect_result.status == ConnectStatus::kError) {
        return {{}, connect_result.error_number};
    }

    const EndpointState upstream_state =
        connect_result.status == ConnectStatus::kConnected
            ? EndpointState::kEstablished
            : EndpointState::kConnecting;

    const ConnectionToken token = AllocateToken();
    Slot& slot = slots_[static_cast<std::size_t>(token.id - 1)];

    try {
        slot.connection = std::make_unique<ConnectionPair>(
            loop_,
            token,
            std::move(client_fd),
            std::move(upstream_fd),
            upstream_state,
            limits_,
            [this](
                ConnectionToken connection_token,
                EndpointSide side,
                EventToken event_token,
                std::uint32_t events) {
                DispatchEvent(
                    connection_token,
                    side,
                    event_token,
                    events);
            },
            [this](ConnectionToken closed_token, CloseReason reason) {
                OnClosed(closed_token, reason);
            });

        ++active_count_;
        slot.connection->Start();
    } catch (...) {
        if (slot.connection) {
            slot.connection.reset();
            --active_count_;
        }
        slot.pending_destroy = false;
        free_ids_.push_back(token.id);
        throw;
    }

    return {token, 0};
}

bool ConnectionManager::Close(
    ConnectionToken token,
    CloseReason reason) noexcept {
    ConnectionPair* connection = Find(token);
    if (connection == nullptr) {
        return false;
    }

    connection->Close(reason);
    CollectClosed(token);
    return true;
}

void ConnectionManager::CloseAll(CloseReason reason) noexcept {
    for (Slot& slot : slots_) {
        if (slot.connection) {
            slot.connection->Close(reason);
        }
    }

    for (std::size_t index = 0; index < slots_.size(); ++index) {
        Slot& slot = slots_[index];
        if (slot.connection && slot.pending_destroy) {
            const ConnectionToken token{
                static_cast<ConnectionId>(index + 1),
                slot.generation
            };
            ReleaseSlot(token);
        }
    }
}

std::size_t ConnectionManager::Size() const noexcept {
    return active_count_;
}

ConnectionPair* ConnectionManager::Find(
    ConnectionToken token) noexcept {
    Slot* slot = FindSlot(token);
    return slot == nullptr ? nullptr : slot->connection.get();
}

const ConnectionPair* ConnectionManager::Find(
    ConnectionToken token) const noexcept {
    const Slot* slot = FindSlot(token);
    return slot == nullptr ? nullptr : slot->connection.get();
}

ConnectionToken ConnectionManager::AllocateToken() {
    if (!free_ids_.empty()) {
        const ConnectionId id = free_ids_.back();
        free_ids_.pop_back();

        Slot& slot = slots_[static_cast<std::size_t>(id - 1)];
        slot.generation = NextGeneration(slot.generation);
        slot.pending_destroy = false;
        return {id, slot.generation};
    }

    if (slots_.size() >=
        static_cast<std::size_t>(
            std::numeric_limits<ConnectionId>::max())) {
        throw std::overflow_error("connection id space exhausted");
    }

    Slot slot;
    slot.generation = 1;
    slots_.push_back(std::move(slot));

    return {
        static_cast<ConnectionId>(slots_.size()),
        1
    };
}

void ConnectionManager::ReleaseSlot(
    ConnectionToken token) noexcept {
    Slot* slot = FindSlot(token);
    if (slot == nullptr || !slot->connection) {
        return;
    }

    slot->connection.reset();
    slot->pending_destroy = false;
    free_ids_.push_back(token.id);

    if (active_count_ > 0) {
        --active_count_;
    }
}

void ConnectionManager::CollectClosed(
    ConnectionToken token) noexcept {
    Slot* slot = FindSlot(token);
    if (slot == nullptr || !slot->connection) {
        return;
    }

    if (!slot->pending_destroy ||
        !slot->connection->IsClosed()) {
        return;
    }

    ReleaseSlot(token);
}

ConnectionManager::Slot* ConnectionManager::FindSlot(
    ConnectionToken token) noexcept {
    if (!token.IsValid() || token.id > slots_.size()) {
        return nullptr;
    }

    Slot& slot = slots_[static_cast<std::size_t>(token.id - 1)];
    if (!slot.connection || slot.generation != token.generation) {
        return nullptr;
    }

    return &slot;
}

const ConnectionManager::Slot* ConnectionManager::FindSlot(
    ConnectionToken token) const noexcept {
    if (!token.IsValid() || token.id > slots_.size()) {
        return nullptr;
    }

    const Slot& slot = slots_[static_cast<std::size_t>(token.id - 1)];
    if (!slot.connection || slot.generation != token.generation) {
        return nullptr;
    }

    return &slot;
}

void ConnectionManager::DispatchEvent(
    ConnectionToken connection_token,
    EndpointSide side,
    EventToken event_token,
    std::uint32_t events) noexcept {
    ConnectionPair* connection = Find(connection_token);
    if (connection == nullptr) {
        return;
    }

    // Important: OnClosed() only marks this slot pending. The object is not
    // destroyed until OnEvent() returns, avoiding self-destruction/UAF.
    connection->OnEvent(side, event_token, events);
    CollectClosed(connection_token);
}

void ConnectionManager::OnClosed(
    ConnectionToken token,
    CloseReason /*reason*/) noexcept {
    Slot* slot = FindSlot(token);
    if (slot == nullptr) {
        return;
    }

    // Do not erase/reset here: Close() may have been called from inside the
    // ConnectionPair member function currently executing on the EventLoop.
    // DispatchEvent()/Close()/CloseAll() performs reclamation after the call
    // stack has returned to the manager.
    slot->pending_destroy = true;
}

}  // namespace chaosproxy
```

---

## 9. 正式测试代码

测试原则：阶段4～6不重复验证阶段2～3已经覆盖的 `UniqueFd/EAGAIN/SO_ERROR/EventLoop Add/Modify/Remove`，只验证新组合语义。

| 测试 | 目标 |
|---|---|
| `BufferTest` | 固定容量、Consume、环绕、不扩容 |
| `ConnectionTest` | 双向透明转发、Close幂等 |
| `MultiConnectionProxyTest` | 一个EventLoop管理多个ConnectionPair |
| `BackpressureTest` | high暂停source，low恢复source，pending不超过capacity |
| `HalfCloseTest` | client FIN后先完整转发，再向upstream传播EOF；反向响应仍能返回 |
| `ConnectionErrorTest` | TCP RST触发整体异常关闭且只回调一次 |

### `tests/helpers/test_socket_utils.h`

```cpp
#pragma once

#include "chaosproxy/event_loop.h"
#include "chaosproxy/socket_ops.h"
#include "chaosproxy/unique_fd.h"

#include <sys/socket.h>

#include <array>
#include <cstddef>
#include <string>
#include <utility>

namespace chaosproxy::test {

inline std::pair<UniqueFd, UniqueFd> MakeSocketPair() {
    int raw_fds[2]{};
    if (::socketpair(
            AF_UNIX,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            raw_fds) < 0) {
        return {};
    }

    return {
        UniqueFd(raw_fds[0]),
        UniqueFd(raw_fds[1])
    };
}

inline bool PumpUntil(
    EventLoop& loop,
    const std::function<bool()>& condition,
    int max_rounds = 200,
    int timeout_ms = 10) {
    for (int round = 0; round < max_rounds; ++round) {
        if (condition()) {
            return true;
        }
        (void)loop.RunOnce(timeout_ms);
    }
    return condition();
}

inline std::string DrainAvailable(int fd) {
    std::string output;
    std::array<char, 4096> buffer{};

    for (;;) {
        const ReadResult result = TryRead(
            fd,
            buffer.data(),
            buffer.size());

        if (result.status == ReadStatus::kData) {
            output.append(buffer.data(), result.bytes_transferred);
            continue;
        }

        break;
    }

    return output;
}

inline bool WriteAllWithLoop(
    EventLoop& loop,
    int fd,
    const std::string& data,
    int max_rounds = 500) {
    std::size_t offset = 0;

    for (int round = 0;
         round < max_rounds && offset < data.size();
         ++round) {
        const WriteResult result = TryWrite(
            fd,
            data.data() + offset,
            data.size() - offset);

        if (result.status == WriteStatus::kWritten) {
            offset += result.bytes_transferred;
        } else if (result.status != WriteStatus::kWouldBlock) {
            return false;
        }

        (void)loop.RunOnce(1);
    }

    return offset == data.size();
}

}  // namespace chaosproxy::test
```

### `tests/unit/buffer_test.cpp`

```cpp
#include "chaosproxy/buffer.h"

#include <gtest/gtest.h>

#include <string>

namespace {

using chaosproxy::Buffer;

TEST(BufferTest, AppendsConsumesAndWrapsWithoutGrowing) {
    Buffer buffer(8);

    ASSERT_TRUE(buffer.Append("abcdef", 6));
    EXPECT_EQ(buffer.Size(), 6U);
    EXPECT_EQ(buffer.FrontSize(), 6U);

    buffer.Consume(5);
    EXPECT_EQ(buffer.Size(), 1U);

    ASSERT_TRUE(buffer.Append("12345", 5));
    EXPECT_EQ(buffer.Size(), 6U);
    EXPECT_EQ(buffer.Capacity(), 8U);

    std::string observed;
    while (!buffer.Empty()) {
        observed.append(buffer.FrontData(), buffer.FrontSize());
        buffer.Consume(buffer.FrontSize());
    }

    EXPECT_EQ(observed, "f12345");
}

TEST(BufferTest, RejectsAppendBeyondFixedCapacity) {
    Buffer buffer(4);

    EXPECT_TRUE(buffer.Append("abcd", 4));
    EXPECT_FALSE(buffer.Append("e", 1));
    EXPECT_EQ(buffer.Size(), 4U);
    EXPECT_EQ(buffer.Available(), 0U);
}

}  // namespace
```

### `tests/helpers/test_connection_harness.h`

```cpp
#pragma once

#include "chaosproxy/connection.h"

#include <memory>
#include <utility>

namespace chaosproxy::test {

class DirectConnectionHarness final {
public:
    DirectConnectionHarness(
        EventLoop& loop,
        ConnectionToken token,
        UniqueFd client_fd,
        UniqueFd upstream_fd,
        EndpointState upstream_state,
        ConnectionLimits limits,
        ConnectionPair::ClosedCallback closed_callback) {
        connection_ = std::make_unique<ConnectionPair>(
            loop,
            token,
            std::move(client_fd),
            std::move(upstream_fd),
            upstream_state,
            limits,
            [this](
                ConnectionToken observed_connection,
                EndpointSide side,
                EventToken event_token,
                std::uint32_t events) {
                if (connection_ == nullptr ||
                    observed_connection != connection_->Token()) {
                    return;
                }
                connection_->OnEvent(side, event_token, events);
            },
            std::move(closed_callback));
    }

    DirectConnectionHarness(const DirectConnectionHarness&) = delete;
    DirectConnectionHarness& operator=(const DirectConnectionHarness&) = delete;

    void Start() {
        connection_->Start();
    }

    [[nodiscard]] ConnectionPair& Connection() noexcept {
        return *connection_;
    }

    [[nodiscard]] const ConnectionPair& Connection() const noexcept {
        return *connection_;
    }

private:
    std::unique_ptr<ConnectionPair> connection_;
};

}  // namespace chaosproxy::test
```

### `tests/unit/connection_test.cpp`

```cpp
#include "chaosproxy/connection.h"
#include "chaosproxy/socket_ops.h"
#include "helpers/test_connection_harness.h"
#include "helpers/test_socket_utils.h"

#include <gtest/gtest.h>

#include <string>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;

TEST(ConnectionTest, ForwardsBytesInBothDirections) {
    auto client_pair = MakeSocketPair();
    auto upstream_pair = MakeSocketPair();
    ASSERT_TRUE(client_pair.first.IsValid());
    ASSERT_TRUE(upstream_pair.first.IsValid());

    EventLoop loop;
    CloseReason close_reason = CloseReason::kNone;

    DirectConnectionHarness harness(
        loop,
        ConnectionToken{1, 1},
        std::move(client_pair.second),
        std::move(upstream_pair.first),
        EndpointState::kEstablished,
        ConnectionLimits{},
        [&](ConnectionToken, CloseReason reason) {
            close_reason = reason;
        });

    harness.Start();

    const std::string request = "client-to-upstream";
    ASSERT_EQ(
        TryWrite(client_pair.first.Get(), request.data(), request.size()).status,
        WriteStatus::kWritten);

    std::string upstream_received;
    ASSERT_TRUE(PumpUntil(loop, [&] {
        upstream_received += DrainAvailable(upstream_pair.second.Get());
        return upstream_received.size() == request.size();
    }));
    EXPECT_EQ(upstream_received, request);

    const std::string response = "upstream-to-client";
    ASSERT_EQ(
        TryWrite(upstream_pair.second.Get(), response.data(), response.size()).status,
        WriteStatus::kWritten);

    std::string client_received;
    ASSERT_TRUE(PumpUntil(loop, [&] {
        client_received += DrainAvailable(client_pair.first.Get());
        return client_received.size() == response.size();
    }));
    EXPECT_EQ(client_received, response);
    EXPECT_EQ(close_reason, CloseReason::kNone);
}

TEST(ConnectionTest, CloseIsIdempotent) {
    auto client_pair = MakeSocketPair();
    auto upstream_pair = MakeSocketPair();
    EventLoop loop;
    int close_callback_count = 0;

    DirectConnectionHarness harness(
        loop,
        ConnectionToken{7, 3},
        std::move(client_pair.second),
        std::move(upstream_pair.first),
        EndpointState::kEstablished,
        ConnectionLimits{},
        [&](ConnectionToken token, CloseReason) {
            EXPECT_EQ(token, (ConnectionToken{7, 3}));
            ++close_callback_count;
        });

    harness.Start();
    harness.Connection().Close(CloseReason::kLocalStop);
    harness.Connection().Close(CloseReason::kSocketError);

    EXPECT_TRUE(harness.Connection().IsClosed());
    EXPECT_EQ(harness.Connection().Reason(), CloseReason::kLocalStop);
    EXPECT_EQ(close_callback_count, 1);
}

}  // namespace
```

### `tests/integration/multi_connection_proxy_test.cpp`

```cpp
#include "chaosproxy/connection_manager.h"
#include "chaosproxy/socket_ops.h"
#include "helpers/test_socket_utils.h"

#include <arpa/inet.h>
#include <sys/socket.h>

#include <array>
#include <cerrno>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;

UniqueFd MakeLoopbackListener(sockaddr_in& address) {
    UniqueFd listen_fd(::socket(
        AF_INET,
        SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
        0));

    if (!listen_fd.IsValid()) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "socket");
    }

    address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;

    if (::bind(
            listen_fd.Get(),
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)) < 0) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "bind");
    }

    if (::listen(listen_fd.Get(), SOMAXCONN) < 0) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "listen");
    }

    socklen_t length = sizeof(address);
    if (::getsockname(
            listen_fd.Get(),
            reinterpret_cast<sockaddr*>(&address),
            &length) < 0) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "getsockname");
    }

    return listen_fd;
}

TEST(MultiConnectionProxyTest, ManagerRunsSeveralConnectionsOnOneEventLoop) {
    sockaddr_in upstream_address{};
    UniqueFd upstream_listener = MakeLoopbackListener(upstream_address);

    EventLoop loop;
    ConnectionManager manager(
        loop,
        reinterpret_cast<const sockaddr*>(&upstream_address),
        sizeof(upstream_address));

    std::vector<UniqueFd> client_apps;
    client_apps.reserve(3);

    for (int index = 0; index < 3; ++index) {
        auto pair = MakeSocketPair();
        ASSERT_TRUE(pair.first.IsValid());
        ASSERT_TRUE(pair.second.IsValid());

        const CreateConnectionResult result =
            manager.Create(std::move(pair.second));
        ASSERT_TRUE(result.Ok());
        client_apps.push_back(std::move(pair.first));
    }

    EXPECT_EQ(manager.Size(), 3U);

    std::vector<UniqueFd> upstream_apps;
    upstream_apps.reserve(3);

    ASSERT_TRUE(PumpUntil(loop, [&] {
        for (;;) {
            AcceptResult result = TryAccept(upstream_listener.Get());
            if (result.status == AcceptStatus::kAccepted) {
                upstream_apps.push_back(std::move(result.client_fd));
                continue;
            }
            EXPECT_EQ(result.status, AcceptStatus::kWouldBlock);
            break;
        }
        return upstream_apps.size() == 3U;
    }));

    for (int index = 0; index < 3; ++index) {
        const char byte = static_cast<char>('a' + index);
        ASSERT_EQ(
            TryWrite(client_apps[index].Get(), &byte, 1).status,
            WriteStatus::kWritten);
    }

    std::string observed;
    ASSERT_TRUE(PumpUntil(loop, [&] {
        for (auto& upstream : upstream_apps) {
            observed += DrainAvailable(upstream.Get());
        }
        return observed.size() == 3U;
    }));

    EXPECT_NE(observed.find('a'), std::string::npos);
    EXPECT_NE(observed.find('b'), std::string::npos);
    EXPECT_NE(observed.find('c'), std::string::npos);
}

}  // namespace
```

### `tests/integration/backpressure_test.cpp`

```cpp
#include "chaosproxy/connection.h"
#include "chaosproxy/socket_ops.h"
#include "helpers/test_connection_harness.h"
#include "helpers/test_socket_utils.h"

#include <sys/socket.h>

#include <array>
#include <gtest/gtest.h>
#include <string>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;

TEST(BackpressureTest, PausesSourceAtHighWaterAndResumesAtLowWater) {
    auto client_pair = MakeSocketPair();
    auto upstream_pair = MakeSocketPair();
    ASSERT_TRUE(client_pair.first.IsValid());
    ASSERT_TRUE(upstream_pair.first.IsValid());

    int send_buffer = 1024;
    ASSERT_EQ(
        ::setsockopt(
            upstream_pair.first.Get(),
            SOL_SOCKET,
            SO_SNDBUF,
            &send_buffer,
            sizeof(send_buffer)),
        0);

    std::array<char, 4096> filler{};
    bool upstream_blocked = false;
    for (int i = 0; i < 1024; ++i) {
        const WriteResult result = TryWrite(
            upstream_pair.first.Get(),
            filler.data(),
            filler.size());

        if (result.status == WriteStatus::kWouldBlock) {
            upstream_blocked = true;
            break;
        }
        ASSERT_EQ(result.status, WriteStatus::kWritten);
    }
    ASSERT_TRUE(upstream_blocked);

    ConnectionLimits limits;
    limits.buffer_capacity = 8192;
    limits.high_watermark = 4096;
    limits.low_watermark = 2048;
    limits.read_budget_per_event = 8192;
    limits.write_budget_per_event = 8192;

    EventLoop loop;
    DirectConnectionHarness harness(
        loop,
        ConnectionToken{1, 1},
        std::move(client_pair.second),
        std::move(upstream_pair.first),
        EndpointState::kEstablished,
        limits,
        [](ConnectionToken, CloseReason) {});
    harness.Start();
    ConnectionPair& connection = harness.Connection();

    const std::string data(8192, 'x');
    ASSERT_TRUE(WriteAllWithLoop(loop, client_pair.first.Get(), data));

    ASSERT_TRUE(PumpUntil(loop, [&] {
        return connection.ReadPaused(EndpointSide::kClient);
    }));

    EXPECT_GE(
        connection.PendingBytes(EndpointSide::kClient),
        limits.high_watermark);
    EXPECT_LE(
        connection.PendingBytes(EndpointSide::kClient),
        limits.buffer_capacity);

    for (int round = 0; round < 200; ++round) {
        (void)DrainAvailable(upstream_pair.second.Get());
        (void)loop.RunOnce(1);
        if (!connection.ReadPaused(EndpointSide::kClient) &&
            connection.PendingBytes(EndpointSide::kClient) == 0) {
            break;
        }
    }

    EXPECT_FALSE(connection.ReadPaused(EndpointSide::kClient));
    EXPECT_EQ(connection.PendingBytes(EndpointSide::kClient), 0U);
}

}  // namespace
```

### `tests/integration/half_close_test.cpp`

```cpp
#include "chaosproxy/connection.h"
#include "chaosproxy/socket_ops.h"
#include "helpers/test_connection_harness.h"
#include "helpers/test_socket_utils.h"

#include <sys/socket.h>

#include <array>
#include <gtest/gtest.h>
#include <string>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;

TEST(HalfCloseTest, ClientFinIsPropagatedOnlyAfterRequestIsForwarded) {
    auto client_pair = MakeSocketPair();
    auto upstream_pair = MakeSocketPair();
    ASSERT_TRUE(client_pair.first.IsValid());
    ASSERT_TRUE(upstream_pair.first.IsValid());

    ConnectionLimits limits;
    limits.buffer_capacity = 256 * 1024;
    limits.high_watermark = 192 * 1024;
    limits.low_watermark = 128 * 1024;
    limits.read_budget_per_event = 256 * 1024;
    limits.write_budget_per_event = 64 * 1024;

    EventLoop loop;
    CloseReason close_reason = CloseReason::kNone;

    DirectConnectionHarness harness(
        loop,
        ConnectionToken{1, 1},
        std::move(client_pair.second),
        std::move(upstream_pair.first),
        EndpointState::kEstablished,
        limits,
        [&](ConnectionToken, CloseReason reason) {
            close_reason = reason;
        });
    harness.Start();
    ConnectionPair& connection = harness.Connection();

    const std::string request(64 * 1024, 'r');
    ASSERT_TRUE(WriteAllWithLoop(loop, client_pair.first.Get(), request));
    ASSERT_EQ(::shutdown(client_pair.first.Get(), SHUT_WR), 0);

    std::string upstream_received;
    bool upstream_eof = false;

    ASSERT_TRUE(PumpUntil(loop, [&] {
        std::array<char, 8192> buffer{};
        for (;;) {
            const ReadResult result = TryRead(
                upstream_pair.second.Get(),
                buffer.data(),
                buffer.size());

            if (result.status == ReadStatus::kData) {
                upstream_received.append(
                    buffer.data(),
                    result.bytes_transferred);
                continue;
            }

            if (result.status == ReadStatus::kEof) {
                upstream_eof = true;
            }
            break;
        }

        return upstream_eof;
    }, 500, 2));

    EXPECT_EQ(upstream_received, request);
    EXPECT_TRUE(connection.SourceEof(EndpointSide::kClient));
    EXPECT_TRUE(connection.WriteShutdownFor(EndpointSide::kClient));

    const std::string response = "response-after-half-close";
    ASSERT_EQ(
        TryWrite(
            upstream_pair.second.Get(),
            response.data(),
            response.size()).status,
        WriteStatus::kWritten);
    ASSERT_EQ(::shutdown(upstream_pair.second.Get(), SHUT_WR), 0);

    std::string client_received;
    bool client_eof = false;

    ASSERT_TRUE(PumpUntil(loop, [&] {
        std::array<char, 128> buffer{};
        for (;;) {
            const ReadResult result = TryRead(
                client_pair.first.Get(),
                buffer.data(),
                buffer.size());

            if (result.status == ReadStatus::kData) {
                client_received.append(
                    buffer.data(),
                    result.bytes_transferred);
                continue;
            }

            if (result.status == ReadStatus::kEof) {
                client_eof = true;
            }
            break;
        }

        return client_eof && connection.IsClosed();
    }, 500, 2));

    EXPECT_EQ(client_received, response);
    EXPECT_EQ(close_reason, CloseReason::kGracefulEof);
}

}  // namespace
```

### `tests/integration/connection_error_test.cpp`

```cpp
#include "chaosproxy/connection.h"
#include "helpers/test_connection_harness.h"
#include "helpers/test_socket_utils.h"

#include <arpa/inet.h>
#include <sys/socket.h>

#include <cerrno>
#include <gtest/gtest.h>
#include <system_error>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;

struct TcpPair {
    UniqueFd client;
    UniqueFd server;
};

TcpPair MakeTcpPair() {
    UniqueFd listen_fd(::socket(
        AF_INET,
        SOCK_STREAM | SOCK_CLOEXEC,
        0));
    if (!listen_fd.IsValid()) {
        throw std::system_error(errno, std::generic_category(), "socket");
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;

    if (::bind(
            listen_fd.Get(),
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)) < 0 ||
        ::listen(listen_fd.Get(), 8) < 0) {
        throw std::system_error(errno, std::generic_category(), "listen setup");
    }

    socklen_t length = sizeof(address);
    if (::getsockname(
            listen_fd.Get(),
            reinterpret_cast<sockaddr*>(&address),
            &length) < 0) {
        throw std::system_error(errno, std::generic_category(), "getsockname");
    }

    UniqueFd client(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
    if (!client.IsValid()) {
        throw std::system_error(errno, std::generic_category(), "socket client");
    }

    if (::connect(
            client.Get(),
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)) < 0) {
        throw std::system_error(errno, std::generic_category(), "connect");
    }

    UniqueFd server(::accept4(
        listen_fd.Get(),
        nullptr,
        nullptr,
        SOCK_NONBLOCK | SOCK_CLOEXEC));
    if (!server.IsValid()) {
        throw std::system_error(errno, std::generic_category(), "accept4");
    }

    return {std::move(client), std::move(server)};
}

TEST(ConnectionErrorTest, TcpResetClosesWholeConnectionExactlyOnce) {
    TcpPair client_side = MakeTcpPair();
    auto upstream_pair = MakeSocketPair();
    EventLoop loop;

    int close_count = 0;
    CloseReason reason = CloseReason::kNone;

    DirectConnectionHarness harness(
        loop,
        ConnectionToken{1, 1},
        std::move(client_side.server),
        std::move(upstream_pair.first),
        EndpointState::kEstablished,
        ConnectionLimits{},
        [&](ConnectionToken, CloseReason observed) {
            ++close_count;
            reason = observed;
        });
    harness.Start();
    ConnectionPair& connection = harness.Connection();

    linger reset_linger{};
    reset_linger.l_onoff = 1;
    reset_linger.l_linger = 0;
    ASSERT_EQ(
        ::setsockopt(
            client_side.client.Get(),
            SOL_SOCKET,
            SO_LINGER,
            &reset_linger,
            sizeof(reset_linger)),
        0);

    client_side.client.Reset();

    ASSERT_TRUE(PumpUntil(loop, [&] {
        return connection.IsClosed();
    }));

    EXPECT_EQ(close_count, 1);
    EXPECT_EQ(reason, CloseReason::kConnectionReset);
}

}  // namespace
```

---

### `tests/integration/connection_generation_test.cpp`

该测试专门验证阶段4～7的新身份不变量：ConnectionId复用后generation必须变化，旧token/旧timer不能命中新连接。

```cpp
#include "chaosproxy/connection_manager.h"
#include "chaosproxy/event_loop.h"
#include "chaosproxy/socket_ops.h"
#include "helpers/test_socket_utils.h"

#include <arpa/inet.h>
#include <sys/socket.h>

#include <cerrno>
#include <chrono>
#include <gtest/gtest.h>
#include <system_error>
#include <utility>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;
using namespace std::chrono_literals;

UniqueFd MakeGenerationTestListener(sockaddr_in& address) {
    UniqueFd listen_fd(::socket(
        AF_INET,
        SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
        0));

    if (!listen_fd.IsValid()) {
        throw std::system_error(errno, std::generic_category(), "socket");
    }

    address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;

    if (::bind(
            listen_fd.Get(),
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)) < 0 ||
        ::listen(listen_fd.Get(), SOMAXCONN) < 0) {
        throw std::system_error(errno, std::generic_category(), "listen setup");
    }

    socklen_t length = sizeof(address);
    if (::getsockname(
            listen_fd.Get(),
            reinterpret_cast<sockaddr*>(&address),
            &length) < 0) {
        throw std::system_error(errno, std::generic_category(), "getsockname");
    }

    return listen_fd;
}

UniqueFd AcceptGenerationPeer(EventLoop& loop, int listen_fd) {
    UniqueFd accepted;

    EXPECT_TRUE(PumpUntil(loop, [&] {
        AcceptResult result = TryAccept(listen_fd);
        if (result.status == AcceptStatus::kAccepted) {
            accepted = std::move(result.client_fd);
            return true;
        }

        EXPECT_EQ(result.status, AcceptStatus::kWouldBlock);
        return false;
    }));

    return accepted;
}

TEST(ConnectionGenerationTest, ReusedIdGetsNewGeneration) {
    sockaddr_in upstream_address{};
    UniqueFd upstream_listener = MakeGenerationTestListener(upstream_address);

    EventLoop loop;
    ConnectionManager manager(
        loop,
        reinterpret_cast<const sockaddr*>(&upstream_address),
        sizeof(upstream_address));

    auto first_pair = MakeSocketPair();
    const CreateConnectionResult first =
        manager.Create(std::move(first_pair.second));
    ASSERT_TRUE(first.Ok());
    UniqueFd first_upstream =
        AcceptGenerationPeer(loop, upstream_listener.Get());
    ASSERT_TRUE(first_upstream.IsValid());

    ASSERT_TRUE(manager.Close(first.token));
    EXPECT_EQ(manager.Size(), 0U);
    EXPECT_EQ(manager.Find(first.token), nullptr);

    auto second_pair = MakeSocketPair();
    const CreateConnectionResult second =
        manager.Create(std::move(second_pair.second));
    ASSERT_TRUE(second.Ok());
    UniqueFd second_upstream =
        AcceptGenerationPeer(loop, upstream_listener.Get());
    ASSERT_TRUE(second_upstream.IsValid());

    EXPECT_EQ(second.token.id, first.token.id);
    EXPECT_NE(second.token.generation, first.token.generation);
    EXPECT_EQ(manager.Find(first.token), nullptr);
    EXPECT_NE(manager.Find(second.token), nullptr);
}

TEST(ConnectionGenerationTest, StaleTimerCannotCloseReusedConnection) {
    sockaddr_in upstream_address{};
    UniqueFd upstream_listener = MakeGenerationTestListener(upstream_address);

    EventLoop loop;
    ConnectionManager manager(
        loop,
        reinterpret_cast<const sockaddr*>(&upstream_address),
        sizeof(upstream_address));

    auto first_pair = MakeSocketPair();
    const CreateConnectionResult first =
        manager.Create(std::move(first_pair.second));
    ASSERT_TRUE(first.Ok());
    UniqueFd first_upstream =
        AcceptGenerationPeer(loop, upstream_listener.Get());
    ASSERT_TRUE(first_upstream.IsValid());

    ASSERT_TRUE(manager.Close(first.token));

    auto second_pair = MakeSocketPair();
    const CreateConnectionResult second =
        manager.Create(std::move(second_pair.second));
    ASSERT_TRUE(second.Ok());
    UniqueFd second_upstream =
        AcceptGenerationPeer(loop, upstream_listener.Get());
    ASSERT_TRUE(second_upstream.IsValid());

    ASSERT_EQ(second.token.id, first.token.id);
    ASSERT_NE(second.token.generation, first.token.generation);

    bool stale_timer_ran = false;
    (void)loop.ScheduleAfter(
        5ms,
        [&manager, stale = first.token, &stale_timer_ran] {
            stale_timer_ran = true;
            EXPECT_FALSE(manager.Close(stale));
        });

    EXPECT_TRUE(PumpUntil(loop, [&] {
        return stale_timer_ran;
    }, 100, 10));

    EXPECT_EQ(manager.Size(), 1U);
    EXPECT_NE(manager.Find(second.token), nullptr);
}

}  // namespace
```

## 10. Listener接入ConnectionManager的方式

阶段3的 `Listener` 不改职责，只把accepted fd移动给Manager：

```cpp
Listener listener(
    loop,
    std::move(listen_fd),
    [&](UniqueFd client_fd) {
        const CreateConnectionResult result =
            manager.Create(std::move(client_fd));

        if (!result.Ok()) {
            // 后续Logger阶段记录result.error_number
        }
    },
    [&](int error_number) {
        // 后续Logger阶段记录listener错误
    });
```

数据路径因此成为：

```text
Listener accept
→ ConnectionManager::Create
→ 创建nonblocking upstream socket
→ StartConnect
→ ConnectionPair::Start
→ client/upstream两个Endpoint注册进同一个EventLoop
→ 两个Direction独立转发
```

---

## 11. 关键状态推演

### 正常双向转发

```text
client EPOLLIN
→ TryRead
→ c2u.pending.Append
→ 尝试TryWrite(upstream)
→ 写空则不订阅EPOLLOUT
→ 短写/EAGAIN则保留pending并订阅upstream EPOLLOUT
```

反方向完全对称。

### 慢Upstream背压

```text
upstream send EAGAIN
→ c2u.pending增长
→ >= high
→ client read_paused
→ client取消EPOLLIN

upstream EPOLLOUT
→ flush pending
→ <= low
→ client read_paused=false
→ 恢复client EPOLLIN
```

### Client先FIN

```text
client recv==0
→ c2u.source_eof=true
→ client不再EPOLLIN
→ c2u.pending继续flush
→ pending==0
→ shutdown(upstream, SHUT_WR)
→ c2u.write_shutdown=true

与此同时：
upstream→client Direction仍可继续返回响应
```

### RST

```text
recv返回ECONNRESET
或EPOLLERR + SO_ERROR=ECONNRESET
→ Close(kConnectionReset)
→ 两个token注销
→ 两个fd关闭
→ Manager删除ConnectionPair
```

---

## 12. 本阶段工程不变量（ID + generation 修订版）

1. `ConnectionManager` 是 `ConnectionPair` 的唯一逻辑拥有者，使用 `unique_ptr`；EventLoop 不拥有业务对象；
2. 长期 EventLoop 回调只保存 `ConnectionToken{id,generation}` 和方向，不保存裸 `ConnectionPair*`；
3. `ConnectionId` 可以复用，因此只有 `id + generation` 一起匹配才代表同一个连接实例；
4. `ConnectionManager::Find(token)` 返回的裸指针只是当前同步调用期间的借用，不得保存到下一轮 EventLoop 或 Timer；
5. `Close()` 回调只能标记 `pending_destroy`，不能在 `ConnectionPair` 当前成员函数调用栈内立即析构对象；
6. `DispatchEvent()` 必须在 `OnEvent()` 返回后再执行 `CollectClosed()`；
7. 每个 Endpoint 只拥有一个 `UniqueFd`；
8. 每个 Direction 的 `pending` 固定容量，不允许无限增长；
9. `TryWrite` 只推进实际写入的字节数；
10. pending 为空时不得长期订阅 `EPOLLOUT`；
11. pending 达到 high 后暂停 source 读取，降到 low 后才恢复；
12. 当前 LT 模式下每次事件有读写字节预算，防止单连接长期占用 EventLoop；
13. `recv==0` 只设置当前 Direction 的 EOF，不立即关闭整个 ConnectionPair；
14. source EOF 后必须 `drain pending → shutdown(destination, SHUT_WR)`；
15. 一个 Direction 完成不代表另一个 Direction 完成；
16. 两个 Direction 都 finished 后才走正常 ConnectionPair 回收；
17. RST、connect失败和 fatal I/O 错误走异常整体清理；
18. `Close()` 必须幂等；
19. Remove EventToken 发生在业务 fd 关闭之前；
20. EventLoop 自身的 `EventToken` 防 fd/注册复用；ConnectionToken 防 Connection slot 复用，两者职责不同；
21. 阶段7及以后所有连接级 Timer 保存 ConnectionToken，不保存裸 ConnectionPair 指针；
22. Timer 正常失效时仍应 Cancel；generation 是 stale callback 的身份安全兜底，不能替代 Cancel。

## 13. 设计决定

### D-S46-001：目录继续扁平

```text
选择：继续include/chaosproxy/*.h + src/*.cpp。
原因：模块数量仍可控，避免无收益的目录层级。
重新评估：Timer/Toxic/Config明显增多后。
```

### D-S46-002：固定容量环形Buffer

```text
选择：每个Direction一个固定容量Buffer。
原因：真正限制排队内存，避免vector动态增长；能够支持短写偏移和环绕。
代价：send一次只处理当前头部连续片段，环绕部分下一轮继续。
```

### D-S46-003：ConnectionManager唯一拥有 + ConnectionToken身份校验

```text
选择：Manager以unique_ptr唯一拥有ConnectionPair；EventLoop callback保存ConnectionToken{id,generation}。
原因：不让异步回调参与对象所有权，同时显式防止ConnectionId复用后旧回调命中新连接。
回收：Close只标记pending_destroy，当前OnEvent返回后由Manager真正reset unique_ptr。
边界：Manager必须比它注册到EventLoop中的连接回调活得久；析构时CloseAll先注销所有EventToken。
```

### D-S46-004：上游Connecting期间允许有限读取Client

```text
选择：client数据可以先进入c2u.pending。
保护：固定capacity + high/low water背压。
失败：upstream connect失败时整体关闭并丢弃尚未转发的数据，因为透明会话已无法成立。
```

### D-S46-005：保留LT，增加每事件字节预算

```text
选择：不在阶段4～6切ET；继续阶段3 LT。
原因：预算用完后LT仍会重新报告就绪，公平性实现简单。
边界：未来ET不能直接沿用“预算用完就返回”。
```

### D-S46-006：FIN正常收尾，RST/fatal error整体失败

```text
FIN：Direction级渐进完成。
RST/fatal error：ConnectionPair整体异常关闭。
原因：两种TCP语义不能混成统一closed状态。
```

---

## 14. 已执行验证

### 严格源码编译

已执行：

```bash
g++ -std=c++17 \
    -D_GNU_SOURCE \
    -Wall -Wextra -Wpedantic -Werror \
    -Iinclude \
    -c src/socket_ops.cpp \
       src/event_loop.cpp \
       src/listener.cpp \
       src/buffer.cpp \
       src/endpoint.cpp \
       src/connection.cpp \
       src/connection_manager.cpp
```

结果：通过。

### CMake + Ninja核心构建

已执行：

```bash
cmake -S . -B build-core \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DBUILD_TESTING=OFF

cmake --build build-core -j
```

结果：`chaosproxy_core` 与 `chaosproxy` 构建成功。

### 手工冒烟测试

当前执行环境没有安装GoogleTest，因此不能把下面结果描述成正式CTest通过。为了先验证网络语义，额外编译了两个不依赖GTest的manual smoke：

```text
m3_smoke：
✓ client→upstream转发
✓ upstream→client转发
✓ client SHUT_WR后upstream收到EOF
✓ upstream仍可回响应
✓ 双Direction完成后GracefulEof
✓ 写阻塞后达到high暂停client读取
✓ drain到low后恢复client读取

manager_reset_smoke：
✓ ConnectionManager同时创建多个ConnectionPair
✓ 非阻塞upstream connect进入同一个EventLoop
✓ 多连接数据均可转发
✓ CloseAll清理
✓ SO_LINGER{1,0}制造TCP RST
✓ RST被识别为ConnectionReset
✓ Close回调只执行一次
```

普通构建结果：通过。

ASan + UBSan：通过（当前执行时关闭LeakSanitizer检测）。

### GTest状态

正式GTest源码已经写入并做过C++语法检查，但当前容器没有GoogleTest头文件/库，因此：

```text
[ ] find_package(GTest REQUIRED)
[ ] chaosproxy_tests正式链接
[ ] gtest_discover_tests
[ ] CTest全部通过
```

必须在用户正式仓库环境继续执行，不能把manual smoke冒充成GTest/CTest验收。

---

## 15. 正式仓库验收命令

```bash
cmake \
    -S . \
    -B build-debug \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DBUILD_TESTING=ON

cmake --build build-debug -j

ctest \
    --test-dir build-debug \
    --output-on-failure
```

只跑阶段4～6：

```bash
./build-debug/chaosproxy_tests \
  --gtest_filter='BufferTest.*:ConnectionTest.*:MultiConnectionProxyTest.*:BackpressureTest.*:HalfCloseTest.*:ConnectionErrorTest.*'
```

Sanitizer建议：

```bash
cmake \
    -S . \
    -B build-asan \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DBUILD_TESTING=ON \
    -DCMAKE_CXX_FLAGS='-fsanitize=address,undefined -fno-omit-frame-pointer'

cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure
```

---

## 16. M2 / M3状态判定

### M2 非阻塞多连接代理

代码能力已经具备：

```text
[x] nonblocking socket
[x] epoll EventLoop
[x] 多ConnectionPair
[x] nonblocking upstream connect + SO_ERROR
[x] token防fd复用
[x] ConnectionManager生命周期
[x] 动态EPOLLOUT
```

但正式仓库的GTest/CTest仍未在当前环境运行，所以长期记忆状态应写：

```text
M2：工程实现完成，正式仓库质量门待用户环境验证
```

### M3 正确双向流与关闭

代码能力已经具备：

```text
[x] 固定上限pending
[x] 短写保留
[x] 动态EPOLLOUT
[x] high/low water背压
[x] 每事件字节预算
[x] FIN半关闭
[x] EOF后drain再SHUT_WR
[x] 双Direction独立完成
[x] RST异常回收
[x] Close幂等
[x] ASan/UBSan manual smoke通过
```

正式状态：

```text
M3：核心工程实现与独立运行验证通过，正式GTest/CTest质量门待用户环境验证
```

---

## 17. 当前明确未做

- `main/app` 仍保持此前基线入口，没有在本文加入完整命令行监听地址/上游地址配置；
- 没有全局所有连接总内存预算，目前是每Direction固定上限；
- 没有TimerQueue；
- 没有Latency/Jitter；
- 没有ToxicPipeline；
- 没有日志与指标；
- 没有多Reactor；
- 没有Buffer Pool；
- 没有修改阶段3的LT模型；
- 没有把manual smoke当正式自动化质量门。

这些边界是有意保留，避免在M3尚未正式仓库验收前叠加阶段7+功能。

---

## 18. 下一阶段入口

下一阶段：

```text
阶段7：TimerQueue
```

下一唯一工程问题：

```text
如何让EventLoop在“未来某个单调时钟deadline”被唤醒并执行任务，
同时保证连接已经销毁时旧timer不会访问悬空ConnectionPair？
```

阶段7必须复用本阶段生命周期结论：

```text
EventToken不能只靠fd
Timer也不能只靠裸Connection*
```

建议下一批新增：

```text
include/chaosproxy/clock.h
include/chaosproxy/timer_queue.h
src/timer_queue.cpp
tests/unit/timer_queue_test.cpp
```

是否使用 `timerfd` 接入EventLoop，在阶段7工程设计时再确定。

---

## 19. 长期记忆摘要

```text
阶段4～6继续建立在阶段2～3的UniqueFd/socket_ops/EventLoop/Listener上。

【2026-08-25修订】
不再使用 ConnectionManager shared_ptr + EventLoop weak_ptr 作为连接身份模型。

现在统一为：
ConnectionManager唯一拥有unique_ptr<ConnectionPair>；
ConnectionToken = {ConnectionId, generation}；
ConnectionId关闭后允许复用slot；每次复用generation递增；
EventLoop长期callback只保存ConnectionToken + EndpointSide；
Manager先校验id和generation，再临时借用ConnectionPair*执行OnEvent。

Close不能在当前ConnectionPair调用栈内直接reset unique_ptr：
Close → OnClosed只标pending_destroy → OnEvent返回 → CollectClosed再析构。

每个Direction继续拥有固定容量环形Buffer：
短写保留pending；
写阻塞动态EPOLLOUT；
high暂停source EPOLLIN；
low恢复source EPOLLIN；
LT模式用每事件字节预算保证公平性。

FIN：EOF → drain pending → shutdown(destination, SHUT_WR)；
两个Direction都finished才正常回收；
RST/fatal error整体异常关闭；
Close保持幂等。

EventLoop自己的EventToken负责防fd/epoll注册复用；
ConnectionToken负责防ConnectionId slot复用。
阶段7 Timer同样保存ConnectionToken，旧Timer即使晚到也不能命中新generation。

核心ID+generation修订源码已通过严格编译、CMake/Ninja核心构建、
ID复用/旧Timer manual smoke与ASan/UBSan；正式GTest/CTest仍需在用户环境运行。
下一阶段：TimerQueue / ToxicPipeline继续复用该身份模型。
```

