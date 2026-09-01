# ChaosProxy 阶段8：ToxicPipeline、Chunk、Snapshot 与安全 Continuation 工程长期记忆

> 文档定位：本文建立在阶段4～7的 `ConnectionToken{ConnectionId,generation}`、`ConnectionManager unique_ptr`、固定容量 Buffer、半关闭以及 TimerQueue/timerfd 工程基线上，只记录阶段8新增/修改内容。阶段8不实现真实 Latency/Jitter/Bandwidth，仅完成可插拔故障策略流水线和 PassThrough 闭环。

## 1. 当前项目位置

```text
项目：ChaosProxy
完成层级：L1 故障注入基础设施（进行中）
前置工程：M3 + 阶段7 TimerQueue
当前阶段：阶段8——故障策略抽象与数据流水线
本轮核心产出：Chunk + Toxic + DirectionContext + ToxicPipeline + PipelineSnapshot + PassThroughToxic + Fast Path + continuation/generation安全恢复
下一阶段：阶段9 Latency 与 Jitter
```

本阶段工程严格复用前置身份模型：

```text
ConnectionManager
→ unique_ptr<ConnectionPair> 唯一拥有

异步/长期身份
→ ConnectionToken{id,generation}

Pipeline旧版本生命周期
→ shared_ptr<const PipelineState> snapshot
```

二者职责不同：ConnectionToken负责判断“连接实例还活不活”；PipelineSnapshot负责保证“这次Chunk继续执行原来那一版策略”。

## 2. 本轮新增工程决定

1. `ConnectionToken` 从 `connection.h` 拆到独立 `connection_token.h`，供 Toxic/Context 使用，避免循环依赖。
2. `Chunk` 是移动语义拥有型字节块；只有启用 Toxic 时才构造，零故障路径仍直接进入原 `Buffer pending`。
3. `DirectionContext` 是可按值复制的轻量安全能力对象，只保存 `EventLoop* + ConnectionToken + source side`，不保存 `ConnectionPair*` 或 `Direction*`。
4. `ToxicOutput` 表示 continuation。`ToxicPipeline::ProcessAt()` 是静态递归执行，不捕获 Pipeline `this`。
5. Pipeline 每次 `Replace()` 创建新的 `PipelineState`；旧 Chunk 的 Output 捕获旧 snapshot，因此配置更新不会产生 pipeline tearing。
6. `PassThroughToxic` 只用于验证 Pipeline 正确性；空 Pipeline 才是零故障 Fast Path。
7. Fast Path 只绕过 ToxicPipeline，不能绕过 `pending / short write / EPOLLOUT / backpressure / FIN drain`。
8. Toxic 最终 Output 不能直接保存或调用裸 `ConnectionPair*`；它携带 `ConnectionToken + source side` 回到 `ConnectionManager` 重新 `Find()` 后再进入 `ConnectionPair::OnPipelineOutput()`。
9. Pipeline 增加 `ExecutionState` 完成通知。只要异步 Output 仍被 Timer 等对象持有，ExecutionState 就继续存活；全部 continuation 释放后才通知 Direction 该输入 Chunk 的 Pipeline 执行完成。
10. `Direction::pipeline_inflight` 参与 FIN 判定：`source_eof && pipeline_inflight==0 && pending.empty()` 后才允许 `shutdown(destination, SHUT_WR)`。这是阶段9异步 Latency 不截断数据的前置保证。
11. 阶段8仍只按 `pending` 做字节级背压；未来 Latency 延迟队列的额外字节预算在阶段9补齐，不能把当前 PassThrough 验证夸大为异步延迟内存控制已完成。

## 3. 新增/修改目录

```text
include/chaosproxy/
├── connection_token.h          # 新增：ConnectionId + generation独立身份头
├── chunk.h                     # 新增：拥有型字节块
├── toxic.h                     # 新增：Toxic接口 + DirectionContext + ToxicOutput
├── pass_through_toxic.h        # 新增
├── toxic_pipeline.h            # 新增：PipelineState/Snapshot/ProcessAt
├── connection.h                # 修改：每Direction一个Pipeline + inflight + Output/Complete入口
└── connection_manager.h        # 修改：Pipeline Output/Complete重新按token分发

src/
├── chunk.cpp                   # 新增
├── toxic.cpp                   # 新增
├── pass_through_toxic.cpp      # 新增
├── toxic_pipeline.cpp          # 新增
├── connection.cpp              # 修改：Fast Path / Pipeline Path汇合
└── connection_manager.cpp      # 修改：stale continuation generation校验

tests/
├── unit/toxic_pipeline_test.cpp
├── integration/connection_pipeline_test.cpp
├── integration/pipeline_generation_test.cpp
└── manual/
    ├── stage8_smoke.cpp
    └── stage8_generation_smoke.cpp
```

## 4. 数据路径

```text
source Endpoint EPOLLIN
        ↓
TryRead
        ↓
Direction
        ↓
Pipeline empty ?
   ┌────┴─────┐
  yes         no
   │           ↓
   │         Chunk
   │           ↓
   │    ToxicPipeline(snapshot)
   │           ↓
   │      Toxic0 → Toxic1 → ...
   │           ↓ Output
   │      ConnectionManager
   │      Find(token+generation)
   │           ↓
   └────→ ConnectionPair::OnPipelineOutput
                    ↓
                 pending
                    ↓
                 TryWrite
                    ↓
        short write / EAGAIN / EPOLLOUT
```

零故障 Fast Path 是：

```text
recv → Direction → pending → send
```

而不是：

```text
recv → send
```

因此阶段5/6已有的短写、背压和半关闭语义不会被绕过。

## 5. 异步 continuation 与 FIN

新的执行生命周期：

```text
ProcessIncoming
→ pipeline_inflight++
→ ToxicPipeline::Process
→ ExecutionState

同步PassThrough：
Output立刻完成
→ ExecutionState析构
→ OnPipelineComplete
→ pipeline_inflight--

未来Latency：
Output被Timer callback保存
→ ExecutionState仍存活
→ source即使先EOF，也不能SHUT_WR
→ Timer到期，Output恢复旧snapshot
→ 全部continuation释放
→ OnPipelineComplete
→ pipeline_inflight--
→ pending排空后才SHUT_WR
```

`ExecutionState` 的完成通知也必须经 `ConnectionManager` 按旧 `ConnectionToken` 重新解析。连接已关闭或 id 已被新 generation 复用时，旧 Output/Complete 都直接丢弃。

## 6. CMake 完整阶段8形状


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
    src/timer_queue.cpp
    src/event_loop.cpp
    src/listener.cpp
    src/buffer.cpp
    src/endpoint.cpp
    src/chunk.cpp
    src/toxic.cpp
    src/pass_through_toxic.cpp
    src/toxic_pipeline.cpp
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
        tests/unit/timer_queue_test.cpp
        tests/unit/event_loop_timer_test.cpp
        tests/unit/buffer_test.cpp
        tests/unit/toxic_pipeline_test.cpp
        tests/unit/connection_test.cpp
        tests/integration/listener_event_loop_test.cpp
        tests/integration/multi_connection_proxy_test.cpp
        tests/integration/backpressure_test.cpp
        tests/integration/half_close_test.cpp
        tests/integration/connection_error_test.cpp
        tests/integration/connection_generation_test.cpp
        tests/integration/connection_pipeline_test.cpp
        tests/integration/pipeline_generation_test.cpp
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


## 7. 阶段8生产代码

以下代码是本轮验证版本。`connection.cpp/connection_manager.cpp` 为阶段4～7实现上的阶段8合并版；实际仓库可直接使用同目录补丁应用。


### `include/chaosproxy/connection_token.h`

```cpp
#pragma once

#include <cstdint>

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

}  // namespace chaosproxy
```


### `include/chaosproxy/chunk.h`

```cpp
#pragma once

#include <cstddef>
#include <vector>

namespace chaosproxy {

class Chunk final {
public:
    Chunk() = default;

    Chunk(const void* data, std::size_t size);
    explicit Chunk(std::vector<char> data) noexcept;

    Chunk(const Chunk&) = delete;
    Chunk& operator=(const Chunk&) = delete;
    Chunk(Chunk&&) noexcept = default;
    Chunk& operator=(Chunk&&) noexcept = default;

    [[nodiscard]] const char* Data() const noexcept;
    [[nodiscard]] char* Data() noexcept;
    [[nodiscard]] std::size_t Size() const noexcept;
    [[nodiscard]] bool Empty() const noexcept;

    void Append(const void* data, std::size_t size);

private:
    std::vector<char> data_;
};

}  // namespace chaosproxy
```


### `src/chunk.cpp`

```cpp
#include "chaosproxy/chunk.h"

#include <cstring>
#include <stdexcept>
#include <utility>

namespace chaosproxy {

Chunk::Chunk(const void* data, std::size_t size)
    : data_(size) {
    if (size == 0) {
        return;
    }

    if (data == nullptr) {
        throw std::invalid_argument("chunk data must not be null");
    }

    std::memcpy(data_.data(), data, size);
}

Chunk::Chunk(std::vector<char> data) noexcept
    : data_(std::move(data)) {}

const char* Chunk::Data() const noexcept {
    return data_.empty() ? nullptr : data_.data();
}

char* Chunk::Data() noexcept {
    return data_.empty() ? nullptr : data_.data();
}

std::size_t Chunk::Size() const noexcept {
    return data_.size();
}

bool Chunk::Empty() const noexcept {
    return data_.empty();
}

void Chunk::Append(const void* data, std::size_t size) {
    if (size == 0) {
        return;
    }

    if (data == nullptr) {
        throw std::invalid_argument("chunk append data must not be null");
    }

    const auto* bytes = static_cast<const char*>(data);
    data_.insert(data_.end(), bytes, bytes + size);
}

}  // namespace chaosproxy
```


### `include/chaosproxy/toxic.h`

```cpp
#pragma once

#include "chaosproxy/chunk.h"
#include "chaosproxy/connection_token.h"
#include "chaosproxy/endpoint.h"
#include "chaosproxy/event_loop.h"

#include <functional>

namespace chaosproxy {

class DirectionContext final {
public:
    DirectionContext(
        EventLoop& loop,
        ConnectionToken connection_token,
        EndpointSide source_side) noexcept;

    [[nodiscard]] ConnectionToken Connection() const noexcept;
    [[nodiscard]] EndpointSide SourceSide() const noexcept;

    [[nodiscard]] TimerId ScheduleAt(
        TimePoint deadline,
        TimerCallback callback) const;

    [[nodiscard]] TimerId ScheduleAfter(
        Duration delay,
        TimerCallback callback) const;

    [[nodiscard]] bool CancelTimer(TimerId id) const;

private:
    EventLoop* loop_;
    ConnectionToken connection_token_;
    EndpointSide source_side_;
};

using ToxicOutput = std::function<void(Chunk)>;

class Toxic {
public:
    virtual ~Toxic() = default;

    virtual void Process(
        Chunk chunk,
        DirectionContext context,
        ToxicOutput output) = 0;
};

}  // namespace chaosproxy
```


### `src/toxic.cpp`

```cpp
#include "chaosproxy/toxic.h"

#include <utility>

namespace chaosproxy {

DirectionContext::DirectionContext(
    EventLoop& loop,
    ConnectionToken connection_token,
    EndpointSide source_side) noexcept
    : loop_(&loop),
      connection_token_(connection_token),
      source_side_(source_side) {}

ConnectionToken DirectionContext::Connection() const noexcept {
    return connection_token_;
}

EndpointSide DirectionContext::SourceSide() const noexcept {
    return source_side_;
}

TimerId DirectionContext::ScheduleAt(
    TimePoint deadline,
    TimerCallback callback) const {
    return loop_->ScheduleAt(deadline, std::move(callback));
}

TimerId DirectionContext::ScheduleAfter(
    Duration delay,
    TimerCallback callback) const {
    return loop_->ScheduleAfter(delay, std::move(callback));
}

bool DirectionContext::CancelTimer(TimerId id) const {
    return loop_->CancelTimer(id);
}

}  // namespace chaosproxy
```


### `include/chaosproxy/pass_through_toxic.h`

```cpp
#pragma once

#include "chaosproxy/toxic.h"

namespace chaosproxy {

class PassThroughToxic final : public Toxic {
public:
    void Process(
        Chunk chunk,
        DirectionContext context,
        ToxicOutput output) override;
};

}  // namespace chaosproxy
```


### `src/pass_through_toxic.cpp`

```cpp
#include "chaosproxy/pass_through_toxic.h"

#include <utility>

namespace chaosproxy {

void PassThroughToxic::Process(
    Chunk chunk,
    DirectionContext /*context*/,
    ToxicOutput output) {
    output(std::move(chunk));
}

}  // namespace chaosproxy
```


### `include/chaosproxy/toxic_pipeline.h`

```cpp
#pragma once

#include "chaosproxy/toxic.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chaosproxy {

using PipelineVersion = std::uint64_t;

struct PipelineState final {
    PipelineVersion version{0};
    std::vector<std::shared_ptr<Toxic>> toxics;
};

using PipelineSnapshot = std::shared_ptr<const PipelineState>;

class ToxicPipeline final {
public:
    ToxicPipeline();

    [[nodiscard]] PipelineSnapshot SnapshotCurrent() const noexcept;
    [[nodiscard]] PipelineVersion Version() const noexcept;
    [[nodiscard]] bool Empty() const noexcept;

    [[nodiscard]] PipelineVersion Replace(
        std::vector<std::shared_ptr<Toxic>> toxics);

    using CompletionCallback = std::function<void()>;

    void Process(
        Chunk chunk,
        DirectionContext context,
        ToxicOutput final_output,
        CompletionCallback completion = {}) const;

private:
    struct ExecutionState;

    static void ProcessAt(
        PipelineSnapshot snapshot,
        std::size_t index,
        Chunk chunk,
        DirectionContext context,
        ToxicOutput final_output,
        std::shared_ptr<ExecutionState> execution);

    [[nodiscard]] PipelineVersion NextVersion() noexcept;

    PipelineSnapshot current_;
    PipelineVersion next_version_{2};
};

}  // namespace chaosproxy
```


### `src/toxic_pipeline.cpp`

```cpp
#include "chaosproxy/toxic_pipeline.h"

#include <stdexcept>
#include <utility>

namespace chaosproxy {

struct ToxicPipeline::ExecutionState final {
    explicit ExecutionState(CompletionCallback callback)
        : completion(std::move(callback)) {}

    ~ExecutionState() noexcept {
        if (!completion) {
            return;
        }

        try {
            completion();
        } catch (...) {
            // Completion is a lifecycle notification and must never escape
            // from shared-state destruction.
        }
    }

    CompletionCallback completion;
};

ToxicPipeline::ToxicPipeline()
    : current_(std::make_shared<PipelineState>(
          PipelineState{1, {}})) {}

PipelineSnapshot ToxicPipeline::SnapshotCurrent() const noexcept {
    return current_;
}

PipelineVersion ToxicPipeline::Version() const noexcept {
    return current_->version;
}

bool ToxicPipeline::Empty() const noexcept {
    return current_->toxics.empty();
}

PipelineVersion ToxicPipeline::Replace(
    std::vector<std::shared_ptr<Toxic>> toxics) {
    for (const auto& toxic : toxics) {
        if (!toxic) {
            throw std::invalid_argument("pipeline toxic must not be null");
        }
    }

    const PipelineVersion version = NextVersion();
    current_ = std::make_shared<PipelineState>(
        PipelineState{version, std::move(toxics)});
    return version;
}

void ToxicPipeline::Process(
    Chunk chunk,
    DirectionContext context,
    ToxicOutput final_output,
    CompletionCallback completion) const {
    if (!final_output) {
        throw std::invalid_argument("pipeline final output must not be empty");
    }

    auto execution =
        std::make_shared<ExecutionState>(std::move(completion));

    ProcessAt(
        current_,
        0,
        std::move(chunk),
        context,
        std::move(final_output),
        std::move(execution));
}

void ToxicPipeline::ProcessAt(
    PipelineSnapshot snapshot,
    std::size_t index,
    Chunk chunk,
    DirectionContext context,
    ToxicOutput final_output,
    std::shared_ptr<ExecutionState> execution) {
    if (index >= snapshot->toxics.size()) {
        final_output(std::move(chunk));
        return;
    }

    std::shared_ptr<Toxic> toxic = snapshot->toxics[index];

    ToxicOutput output =
        [snapshot,
         index,
         context,
         final_output,
         execution](Chunk next_chunk) mutable {
            ProcessAt(
                snapshot,
                index + 1,
                std::move(next_chunk),
                context,
                final_output,
                execution);
        };

    toxic->Process(
        std::move(chunk),
        context,
        std::move(output));
}

PipelineVersion ToxicPipeline::NextVersion() noexcept {
    for (;;) {
        const PipelineVersion candidate = next_version_++;
        if (candidate != 0) {
            return candidate;
        }
    }
}

}  // namespace chaosproxy
```


### `include/chaosproxy/connection.h`

```cpp
#pragma once

#include "chaosproxy/buffer.h"
#include "chaosproxy/chunk.h"
#include "chaosproxy/connection_token.h"
#include "chaosproxy/endpoint.h"
#include "chaosproxy/event_loop.h"
#include "chaosproxy/toxic_pipeline.h"
#include "chaosproxy/unique_fd.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace chaosproxy {

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

    using PipelineOutputDispatchCallback = std::function<void(
        ConnectionToken,
        EndpointSide,
        Chunk)>;

    using PipelineCompletionDispatchCallback = std::function<void(
        ConnectionToken,
        EndpointSide)>;

    ConnectionPair(
        EventLoop& loop,
        ConnectionToken token,
        UniqueFd client_fd,
        UniqueFd upstream_fd,
        EndpointState upstream_state,
        ConnectionLimits limits,
        EventDispatchCallback event_dispatch_callback,
        PipelineOutputDispatchCallback pipeline_output_dispatch_callback,
        PipelineCompletionDispatchCallback pipeline_completion_dispatch_callback,
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

    [[nodiscard]] PipelineVersion ReplacePipeline(
        EndpointSide source,
        std::vector<std::shared_ptr<Toxic>> toxics);

    [[nodiscard]] PipelineVersion PipelineVersionFor(
        EndpointSide source) const noexcept;

    [[nodiscard]] bool PipelineEmpty(
        EndpointSide source) const noexcept;

    [[nodiscard]] std::size_t PipelineInflight(
        EndpointSide source) const noexcept;

    // EventLoop-facing endpoint handler. Production code must call this only
    // through ConnectionManager after validating ConnectionToken.
    void OnEvent(
        EndpointSide side,
        EventToken token,
        std::uint32_t events) noexcept;

    // Toxic continuation final-output entry. Production code must call this
    // through ConnectionManager so ConnectionToken/generation is revalidated.
    void OnPipelineOutput(
        EndpointSide source_side,
        Chunk chunk) noexcept;

    void OnPipelineComplete(
        EndpointSide source_side) noexcept;

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
        ToxicPipeline pipeline;
        std::size_t pipeline_inflight{0};
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

    bool ProcessIncoming(
        Direction& direction,
        const void* data,
        std::size_t size);

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
    PipelineOutputDispatchCallback pipeline_output_dispatch_callback_;
    PipelineCompletionDispatchCallback pipeline_completion_dispatch_callback_;
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
    PipelineOutputDispatchCallback pipeline_output_dispatch_callback,
    PipelineCompletionDispatchCallback pipeline_completion_dispatch_callback,
    ClosedCallback closed_callback)
    : loop_(loop),
      token_(token),
      limits_(limits),
      event_dispatch_callback_(std::move(event_dispatch_callback)),
      pipeline_output_dispatch_callback_(
          std::move(pipeline_output_dispatch_callback)),
      pipeline_completion_dispatch_callback_(
          std::move(pipeline_completion_dispatch_callback)),
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
        !pipeline_output_dispatch_callback_ ||
        !pipeline_completion_dispatch_callback_ ||
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

PipelineVersion ConnectionPair::ReplacePipeline(
    EndpointSide source,
    std::vector<std::shared_ptr<Toxic>> toxics) {
    return SourceDirection(source).pipeline.Replace(std::move(toxics));
}

PipelineVersion ConnectionPair::PipelineVersionFor(
    EndpointSide source) const noexcept {
    return SourceDirection(source).pipeline.Version();
}

bool ConnectionPair::PipelineEmpty(
    EndpointSide source) const noexcept {
    return SourceDirection(source).pipeline.Empty();
}

std::size_t ConnectionPair::PipelineInflight(
    EndpointSide source) const noexcept {
    return SourceDirection(source).pipeline_inflight;
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

void ConnectionPair::OnPipelineOutput(
    EndpointSide source_side,
    Chunk chunk) noexcept {
    if (closed_ || chunk.Empty()) {
        return;
    }

    try {
        Direction& direction = SourceDirection(source_side);

        if (!direction.pending.Append(chunk.Data(), chunk.Size())) {
            Close(CloseReason::kInternalError);
            return;
        }

        UpdateBackpressure(direction);

        if (!closed_) {
            RefreshInterests();
        }
    } catch (...) {
        Close(CloseReason::kInternalError);
    }
}

void ConnectionPair::OnPipelineComplete(
    EndpointSide source_side) noexcept {
    if (closed_) {
        return;
    }

    try {
        Direction& direction = SourceDirection(source_side);
        if (direction.pipeline_inflight == 0) {
            Close(CloseReason::kInternalError);
            return;
        }

        --direction.pipeline_inflight;
        UpdateBackpressure(direction);
        TryFinishDirection(direction);
        MaybeFinishConnection();

        if (!closed_) {
            RefreshInterests();
        }
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
            if (!ProcessIncoming(
                    direction,
                    buffer.data(),
                    result.bytes_transferred)) {
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

bool ConnectionPair::ProcessIncoming(
    Direction& direction,
    const void* data,
    std::size_t size) {
    if (direction.pipeline.Empty()) {
        if (!direction.pending.Append(data, size)) {
            Close(CloseReason::kInternalError);
            return false;
        }
        return true;
    }

    const ConnectionToken connection_token = token_;
    const EndpointSide source_side = direction.source;
    const PipelineOutputDispatchCallback output_dispatch =
        pipeline_output_dispatch_callback_;
    const PipelineCompletionDispatchCallback completion_dispatch =
        pipeline_completion_dispatch_callback_;

    DirectionContext context(loop_, connection_token, source_side);
    Chunk chunk(data, size);

    ++direction.pipeline_inflight;

    direction.pipeline.Process(
        std::move(chunk),
        context,
        [connection_token, source_side, output_dispatch](Chunk output) mutable {
            output_dispatch(
                connection_token,
                source_side,
                std::move(output));
        },
        [connection_token, source_side, completion_dispatch] {
            completion_dispatch(connection_token, source_side);
        });

    return !closed_;
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
        direction.pipeline_inflight != 0 ||
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
               direction.pipeline_inflight == 0 &&
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

    [[nodiscard]] bool ReplacePipeline(
        ConnectionToken token,
        EndpointSide source,
        std::vector<std::shared_ptr<Toxic>> toxics);

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

    void DispatchPipelineOutput(
        ConnectionToken connection_token,
        EndpointSide source_side,
        Chunk chunk) noexcept;

    void DispatchPipelineComplete(
        ConnectionToken connection_token,
        EndpointSide source_side) noexcept;

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
            [this](
                ConnectionToken connection_token,
                EndpointSide source_side,
                Chunk chunk) {
                DispatchPipelineOutput(
                    connection_token,
                    source_side,
                    std::move(chunk));
            },
            [this](
                ConnectionToken connection_token,
                EndpointSide source_side) {
                DispatchPipelineComplete(
                    connection_token,
                    source_side);
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

bool ConnectionManager::ReplacePipeline(
    ConnectionToken token,
    EndpointSide source,
    std::vector<std::shared_ptr<Toxic>> toxics) {
    ConnectionPair* connection = Find(token);
    if (connection == nullptr) {
        return false;
    }

    (void)connection->ReplacePipeline(source, std::move(toxics));
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

void ConnectionManager::DispatchPipelineOutput(
    ConnectionToken connection_token,
    EndpointSide source_side,
    Chunk chunk) noexcept {
    ConnectionPair* connection = Find(connection_token);
    if (connection == nullptr) {
        return;
    }

    connection->OnPipelineOutput(source_side, std::move(chunk));
    CollectClosed(connection_token);
}

void ConnectionManager::DispatchPipelineComplete(
    ConnectionToken connection_token,
    EndpointSide source_side) noexcept {
    ConnectionPair* connection = Find(connection_token);
    if (connection == nullptr) {
        return;
    }

    connection->OnPipelineComplete(source_side);
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


## 8. 新增正式测试

阶段8核心自动化测试目标：

```text
[ ] PassThrough逐字节不变
[ ] Toxic按配置顺序执行
[ ] 1→0可以不Emit
[ ] 1→N可以Emit多次
[ ] 配置Replace后旧continuation继续旧snapshot
[ ] 新Chunk使用新snapshot
[ ] 空Pipeline透明转发保持原Fast Path
[ ] 有PassThrough时仍通过原pending/write路径
[ ] 异步continuation存在时FIN不能提前传播
[ ] 旧ConnectionToken的continuation不能命中新generation连接
```


### `tests/unit/toxic_pipeline_test.cpp`

```cpp
#include "chaosproxy/pass_through_toxic.h"
#include "chaosproxy/toxic_pipeline.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace chaosproxy;

std::string ToString(const Chunk& chunk) {
    if (chunk.Empty()) {
        return {};
    }
    return std::string(chunk.Data(), chunk.Size());
}

class AppendMarkerToxic final : public Toxic {
public:
    explicit AppendMarkerToxic(char marker)
        : marker_(marker) {}

    void Process(
        Chunk chunk,
        DirectionContext /*context*/,
        ToxicOutput output) override {
        chunk.Append(&marker_, 1);
        output(std::move(chunk));
    }

private:
    char marker_;
};

class DropToxic final : public Toxic {
public:
    void Process(
        Chunk /*chunk*/,
        DirectionContext /*context*/,
        ToxicOutput /*output*/) override {}
};

class DuplicateToxic final : public Toxic {
public:
    void Process(
        Chunk chunk,
        DirectionContext /*context*/,
        ToxicOutput output) override {
        Chunk first(chunk.Data(), chunk.Size());
        Chunk second(chunk.Data(), chunk.Size());
        output(std::move(first));
        output(std::move(second));
    }
};

class HoldToxic final : public Toxic {
public:
    void Process(
        Chunk chunk,
        DirectionContext /*context*/,
        ToxicOutput output) override {
        chunk_ = std::make_unique<Chunk>(std::move(chunk));
        output_ = std::move(output);
    }

    void Release() {
        ASSERT_NE(chunk_, nullptr);
        ASSERT_TRUE(static_cast<bool>(output_));

        auto chunk = std::move(chunk_);
        ToxicOutput output = std::move(output_);
        output(std::move(*chunk));
    }

private:
    std::unique_ptr<Chunk> chunk_;
    ToxicOutput output_;
};

TEST(ToxicPipelineTest, PassThroughPreservesBytes) {
    EventLoop loop;
    ToxicPipeline pipeline;
    (void)pipeline.Replace({std::make_shared<PassThroughToxic>()});

    std::vector<std::string> outputs;
    DirectionContext context(
        loop,
        ConnectionToken{1, 1},
        EndpointSide::kClient);

    pipeline.Process(
        Chunk("hello", 5),
        context,
        [&](Chunk chunk) {
            outputs.push_back(ToString(chunk));
        });

    ASSERT_EQ(outputs.size(), 1U);
    EXPECT_EQ(outputs[0], "hello");
}

TEST(ToxicPipelineTest, AppliesToxicsInConfiguredOrder) {
    EventLoop loop;
    ToxicPipeline pipeline;
    (void)pipeline.Replace({
        std::make_shared<AppendMarkerToxic>('A'),
        std::make_shared<AppendMarkerToxic>('B'),
        std::make_shared<AppendMarkerToxic>('C')
    });

    std::string output;
    DirectionContext context(
        loop,
        ConnectionToken{2, 1},
        EndpointSide::kClient);

    pipeline.Process(
        Chunk("x", 1),
        context,
        [&](Chunk chunk) {
            output = ToString(chunk);
        });

    EXPECT_EQ(output, "xABC");
}

TEST(ToxicPipelineTest, SupportsOneToZero) {
    EventLoop loop;
    ToxicPipeline pipeline;
    (void)pipeline.Replace({std::make_shared<DropToxic>()});

    int output_count = 0;
    DirectionContext context(
        loop,
        ConnectionToken{3, 1},
        EndpointSide::kClient);

    pipeline.Process(
        Chunk("drop", 4),
        context,
        [&](Chunk) {
            ++output_count;
        });

    EXPECT_EQ(output_count, 0);
}

TEST(ToxicPipelineTest, SupportsOneToMany) {
    EventLoop loop;
    ToxicPipeline pipeline;
    (void)pipeline.Replace({std::make_shared<DuplicateToxic>()});

    std::vector<std::string> outputs;
    DirectionContext context(
        loop,
        ConnectionToken{4, 1},
        EndpointSide::kClient);

    pipeline.Process(
        Chunk("xy", 2),
        context,
        [&](Chunk chunk) {
            outputs.push_back(ToString(chunk));
        });

    EXPECT_EQ(outputs, (std::vector<std::string>{"xy", "xy"}));
}

TEST(ToxicPipelineTest, InFlightContinuationKeepsOriginalSnapshot) {
    EventLoop loop;
    ToxicPipeline pipeline;
    auto hold = std::make_shared<HoldToxic>();

    const PipelineVersion old_version = pipeline.Replace({
        hold,
        std::make_shared<AppendMarkerToxic>('A')
    });

    std::vector<std::string> outputs;
    DirectionContext context(
        loop,
        ConnectionToken{5, 1},
        EndpointSide::kClient);

    pipeline.Process(
        Chunk("old", 3),
        context,
        [&](Chunk chunk) {
            outputs.push_back(ToString(chunk));
        });

    const PipelineVersion new_version = pipeline.Replace({
        std::make_shared<AppendMarkerToxic>('B')
    });

    EXPECT_NE(old_version, new_version);
    EXPECT_TRUE(outputs.empty());

    hold->Release();

    ASSERT_EQ(outputs.size(), 1U);
    EXPECT_EQ(outputs[0], "oldA");

    pipeline.Process(
        Chunk("new", 3),
        context,
        [&](Chunk chunk) {
            outputs.push_back(ToString(chunk));
        });

    ASSERT_EQ(outputs.size(), 2U);
    EXPECT_EQ(outputs[1], "newB");
}

TEST(ToxicPipelineTest, DirectionContextCarriesStableIdentity) {
    EventLoop loop;
    const ConnectionToken token{9, 7};
    DirectionContext context(loop, token, EndpointSide::kUpstream);

    EXPECT_EQ(context.Connection(), token);
    EXPECT_EQ(context.SourceSide(), EndpointSide::kUpstream);
}

}  // namespace
```


### `tests/integration/connection_pipeline_test.cpp`

```cpp
#include "chaosproxy/connection.h"
#include "chaosproxy/pass_through_toxic.h"
#include "chaosproxy/socket_ops.h"
#include "helpers/test_connection_harness.h"
#include "helpers/test_socket_utils.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;

TEST(ConnectionPipelineTest, PassThroughUsesExistingPendingAndWritePath) {
    auto client_pair = MakeSocketPair();
    auto upstream_pair = MakeSocketPair();
    ASSERT_TRUE(client_pair.first.IsValid());
    ASSERT_TRUE(upstream_pair.first.IsValid());

    EventLoop loop;
    DirectConnectionHarness harness(
        loop,
        ConnectionToken{1, 1},
        std::move(client_pair.second),
        std::move(upstream_pair.first),
        EndpointState::kEstablished,
        ConnectionLimits{},
        [](ConnectionToken, CloseReason) {});

    (void)harness.Connection().ReplacePipeline(
        EndpointSide::kClient,
        {std::make_shared<PassThroughToxic>()});

    EXPECT_FALSE(harness.Connection().PipelineEmpty(EndpointSide::kClient));
    harness.Start();

    const std::string data = "through-pipeline";
    ASSERT_EQ(
        TryWrite(client_pair.first.Get(), data.data(), data.size()).status,
        WriteStatus::kWritten);

    std::string received;
    ASSERT_TRUE(PumpUntil(loop, [&] {
        received += DrainAvailable(upstream_pair.second.Get());
        return received.size() == data.size();
    }));

    EXPECT_EQ(received, data);
    EXPECT_EQ(harness.Connection().PendingBytes(EndpointSide::kClient), 0U);
}

TEST(ConnectionPipelineTest, EmptyPipelineRetainsTransparentFastPath) {
    auto client_pair = MakeSocketPair();
    auto upstream_pair = MakeSocketPair();
    EventLoop loop;

    DirectConnectionHarness harness(
        loop,
        ConnectionToken{2, 1},
        std::move(client_pair.second),
        std::move(upstream_pair.first),
        EndpointState::kEstablished,
        ConnectionLimits{},
        [](ConnectionToken, CloseReason) {});

    EXPECT_TRUE(harness.Connection().PipelineEmpty(EndpointSide::kClient));
    harness.Start();

    const std::string data = "fast-path";
    ASSERT_EQ(
        TryWrite(client_pair.first.Get(), data.data(), data.size()).status,
        WriteStatus::kWritten);

    std::string received;
    ASSERT_TRUE(PumpUntil(loop, [&] {
        received += DrainAvailable(upstream_pair.second.Get());
        return received.size() == data.size();
    }));

    EXPECT_EQ(received, data);
}

}  // namespace
```


### `tests/integration/pipeline_generation_test.cpp`

```cpp
#include "chaosproxy/connection_manager.h"
#include "chaosproxy/socket_ops.h"
#include "chaosproxy/toxic.h"
#include "helpers/test_socket_utils.h"

#include <arpa/inet.h>
#include <sys/socket.h>

#include <cerrno>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <system_error>
#include <utility>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;

class HoldToxic final : public Toxic {
public:
    void Process(
        Chunk chunk,
        DirectionContext /*context*/,
        ToxicOutput output) override {
        chunk_ = std::make_unique<Chunk>(std::move(chunk));
        output_ = std::move(output);
    }

    [[nodiscard]] bool HasPending() const noexcept {
        return chunk_ != nullptr && static_cast<bool>(output_);
    }

    void Release() {
        ASSERT_TRUE(HasPending());
        auto chunk = std::move(chunk_);
        ToxicOutput output = std::move(output_);
        output(std::move(*chunk));
    }

private:
    std::unique_ptr<Chunk> chunk_;
    ToxicOutput output_;
};

UniqueFd MakeListener(sockaddr_in& address) {
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

UniqueFd AcceptPeer(EventLoop& loop, int listen_fd) {
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

TEST(PipelineGenerationTest, StaleContinuationCannotEmitIntoReusedConnection) {
    sockaddr_in upstream_address{};
    UniqueFd upstream_listener = MakeListener(upstream_address);

    EventLoop loop;
    ConnectionManager manager(
        loop,
        reinterpret_cast<const sockaddr*>(&upstream_address),
        sizeof(upstream_address));

    auto first_client = MakeSocketPair();
    const CreateConnectionResult first =
        manager.Create(std::move(first_client.second));
    ASSERT_TRUE(first.Ok());
    UniqueFd first_upstream = AcceptPeer(loop, upstream_listener.Get());
    ASSERT_TRUE(first_upstream.IsValid());

    auto hold = std::make_shared<HoldToxic>();
    ASSERT_TRUE(manager.ReplacePipeline(
        first.token,
        EndpointSide::kClient,
        {hold}));

    ASSERT_EQ(
        TryWrite(first_client.first.Get(), "old", 3).status,
        WriteStatus::kWritten);

    ASSERT_TRUE(PumpUntil(loop, [&] {
        return hold->HasPending();
    }));

    ASSERT_TRUE(manager.Close(first.token));
    EXPECT_EQ(manager.Find(first.token), nullptr);

    auto second_client = MakeSocketPair();
    const CreateConnectionResult second =
        manager.Create(std::move(second_client.second));
    ASSERT_TRUE(second.Ok());
    UniqueFd second_upstream = AcceptPeer(loop, upstream_listener.Get());
    ASSERT_TRUE(second_upstream.IsValid());

    ASSERT_EQ(second.token.id, first.token.id);
    ASSERT_NE(second.token.generation, first.token.generation);

    hold->Release();
    (void)loop.RunOnce(10);

    EXPECT_TRUE(DrainAvailable(second_upstream.Get()).empty());
    EXPECT_NE(manager.Find(second.token), nullptr);
}

}  // namespace
```


## 9. 已执行验证

本轮基于用户提供的阶段0、阶段2～6、阶段7长期记忆代码重建工程后执行：

```text
1. 阶段2～8生产源码
   g++ -std=c++17 -D_GNU_SOURCE
       -Wall -Wextra -Wpedantic -Werror
   → 通过

2. CMake + Ninja + BUILD_TESTING=OFF
   → chaosproxy_core 构建成功
   → chaosproxy 链接成功

3. stage8_smoke
   → Pipeline snapshot旧版本恢复正确
   → Pipeline completion在异步Output释放前不会提前触发
   → source EOF + pipeline_inflight时不会提前SHUT_WR
   → Output释放后完整转发，再传播EOF
   → Empty Pipeline Fast Path透明转发通过
   → PassThrough Pipeline透明转发通过

4. stage8_generation_smoke
   → 连接A关闭
   → ConnectionId被B复用且generation变化
   → A遗留continuation随后Emit
   → Manager按旧token校验失败
   → 不会把A旧数据写入B

5. ASan + UBSan
   → stage8_smoke通过（本环境关闭LeakSanitizer）

6. GTest源码
   → 当前环境没有正式GoogleTest库
   → 使用最小GTest stub做全部测试源码C++语法检查，通过
```

因此当前不能写成“正式 CTest 已通过”。用户仓库仍需运行：

```bash
cmake -S . -B build-debug \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DBUILD_TESTING=ON

cmake --build build-debug -j
ctest --test-dir build-debug --output-on-failure
```

阶段8筛选：

```bash
./build-debug/chaosproxy_tests \
  --gtest_filter='ToxicPipelineTest.*:ConnectionPipelineTest.*:PipelineGenerationTest.*'
```

## 10. 阶段8新增不变量

1. `ToxicPipeline::ProcessAt()` 的 continuation 不捕获 Pipeline `this`。
2. Toxic 的长期异步状态不得保存裸 `ConnectionPair* / Direction*`。
3. `DirectionContext` 只提供稳定身份和 EventLoop Timer 能力；当前不直接拥有 Direction。
4. Pipeline Output/Complete 都通过 `ConnectionToken{id,generation}` 回到 Manager 验证。
5. 旧 ConnectionId 被复用后，generation 不匹配的 Output/Complete 必须无条件丢弃。
6. 每个输入 Chunk 在进入非空 Pipeline 时增加一次 `pipeline_inflight`；整个执行树/异步 continuation 生命周期结束后只减少一次。
7. source EOF 后，`pipeline_inflight != 0` 时禁止传播 `SHUT_WR`。
8. Pipeline snapshot 创建后结构不可原地修改；配置更新创建新 snapshot。
9. 已经进入 Pipeline 的 Chunk 始终继续原 snapshot；新 Chunk 获取 current snapshot。
10. Fast Path 只跳过 ToxicPipeline，不跳过 pending/short-write/backpressure/half-close。
11. PassThroughToxic 是 Pipeline 正确性基准策略，不是零故障 Fast Path。
12. 当前阶段没有异步延迟字节预算；真实 Latency 上线前阶段9必须增加 delayed/inflight byte 上限和 TimerId取消管理。

## 11. 设计决定

### D-S8-001：ConnectionToken拆分为独立头

```text
选择：新增connection_token.h。
原因：Toxic/DirectionContext需要稳定连接身份，但connection.h本身又要包含ToxicPipeline，拆分后避免循环依赖。
```

### D-S8-002：Pipeline continuation采用snapshot + token dispatch

```text
选择：Output捕获PipelineSnapshot；最终输出回Manager并用ConnectionToken重新解析。
拒绝：Output捕获ToxicPipeline* / ConnectionPair*。
原因：同时解决Pipeline配置版本生命周期和Connection实例生命周期，两套身份职责不混用。
```

### D-S8-003：每个Direction独立ToxicPipeline

```text
选择：client→upstream与upstream→client各自拥有Pipeline。
原因：故障策略是Direction级；未来双向Latency/Bandwidth可独立配置和维护运行态。
```

### D-S8-004：Fast Path保留原Buffer路径

```text
选择：空Pipeline直接Append到原Direction.pending，不构造Chunk。
原因：零故障时没有必要付出Chunk分配/virtual Process/continuation成本，同时不破坏既有I/O状态机。
```

### D-S8-005：ExecutionState追踪异步Pipeline完成

```text
选择：每个输入Chunk创建共享ExecutionState，所有Output continuation共享；最后一个continuation释放时通知完成。
原因：仅靠pending.empty无法判断异步Toxic是否仍持有尚未Emit的数据，FIN会提前传播。
边界：Toxic不应把Output长期存回自身形成自环；阶段9应把Output交给TimerQueue callback并保存TimerId用于Cancel。
```

## 12. 当前阶段状态

```text
阶段8理论主线：完成
Chunk：完成
Toxic接口：完成
DirectionContext：完成
ProcessAt/index：完成
1→0 / 1→1 / 1→N：完成
Pipeline顺序：完成
Pipeline snapshot：完成
安全continuation：完成
ConnectionToken generation恢复：完成
Pipeline inflight + FIN安全：完成
零故障Fast Path：完成
PassThroughToxic：完成
严格生产源码编译：通过
CMake/Ninja核心构建：通过
manual smoke：通过
ASan/UBSan smoke：通过
正式GTest/CTest：待用户仓库运行
```

## 13. 下一阶段入口

下一阶段：

```text
阶段9：Latency 与 Jitter
```

下一唯一工程问题：

```text
如何让一个Chunk在Pipeline中异步等待deadline，
不sleep阻塞EventLoop，
同时为延迟中的Chunk建立真实字节预算、TimerId取消、固定seed抖动，
并在Timer触发后继续原PipelineSnapshot。
```

阶段9必须直接复用本阶段：

```text
DirectionContext::ScheduleAfter
+ ToxicOutput continuation
+ PipelineSnapshot
+ ConnectionToken{id,generation}
+ pipeline_inflight
```

不能重新引入裸ConnectionPair指针，也不能用sleep实现Latency。
