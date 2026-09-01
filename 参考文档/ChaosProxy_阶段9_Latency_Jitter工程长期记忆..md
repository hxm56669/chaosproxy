# ChaosProxy 阶段9：Latency / Jitter 工程长期记忆

> 文档定位：本文建立在阶段8 `ToxicPipeline + PipelineSnapshot + 安全 continuation` 以及阶段8后 `net / reactor / proxy / toxic` 四层目录重构之上，记录阶段9 Latency / Jitter 的完整增量工程实现、修改文件、测试、设计决定、验证结果和后续入口。本文可以作为阶段9恢复工程时的主要长期记忆，不再依赖单独 patch 才能理解核心实现。
>
> 本阶段不实现阶段10 Bandwidth / Slicer；只完成非阻塞延迟、抖动、跨 PipelineSnapshot 保序、延迟字节预算、Timer 取消内存安全与 Reactor Timer 公平性。

---

## 1. 当前项目位置

```text
项目：ChaosProxy
完成层级：L1 故障注入基础设施（进行中）
前置工程：M3 + Stage7 TimerQueue + Stage8 ToxicPipeline
当前阶段：阶段9——Latency 与 Jitter
本轮核心产出：Scheduler + RandomSource + LatencyToxic + Direction Runtime + Sequence Gate + delayed/reorder统一预算 + Timer批处理公平性
当前工程目录：阶段8后 net / reactor / proxy / toxic 四层目录
下一阶段：阶段10——Bandwidth 与 Slicer
```

阶段9解决的核心问题：

```text
如何让一个 Chunk 在 ToxicPipeline 中等待未来 deadline，
但不 sleep 阻塞单线程 Reactor；
同时在 Jitter、动态 Pipeline Replace、异步 continuation、连接关闭、
内存背压和大量 Timer 同时到期时仍保持正确性。
```

### 本阶段完成边界

```text
[x] fixed latency 不使用 sleep
[x] jitter 使用固定 seed 可重复
[x] 每个 Direction 独立 runtime / random
[x] Latency Timer 持有 Chunk + continuation
[x] PipelineSnapshot 继续保证旧 Chunk 使用旧策略版本
[x] Direction 级 PipelineSequence / Sequence Gate 保证跨版本顺序
[x] 空 Pipeline Fast Path 在无旧异步序列时保留
[x] queued = pending + delayed + reorder
[x] high/low 基于总 queued bytes
[x] 新增 hard_limit / BufferLimitExceeded
[x] Connection 关闭主动 Cancel 延迟 Timer
[x] Cancel 立即释放 callback capture
[x] TimerQueue RunExpired 支持 budget
[x] EventLoop 每次 Timer event 限制 callback 数量
[x] generation 继续防 stale continuation / stale timer
[ ] 正式 GTest / CTest：仍需在用户仓库安装 GTest 后执行
```

---

## 2. 前置基线与本阶段不改变的东西

阶段9严格复用阶段8后的工程主线：

```text
ConnectionManager
→ unique_ptr<ConnectionPair> 唯一拥有

长期连接身份
→ ConnectionToken{id,generation}

旧 Pipeline 生命周期
→ shared_ptr<const PipelineState> PipelineSnapshot

零故障 Fast Path
→ recv → pending → TryWrite

正常关闭
→ source EOF
→ 等 pipeline / delayed / reorder 完成
→ drain pending
→ shutdown(destination, SHUT_WR)
```

本阶段不重新设计：

- `UniqueFd` 与非阻塞 Socket 结果类型；
- EventToken 防 fd / epoll 注册复用；
- `ConnectionToken{id,generation}` 防 ConnectionId 复用；
- `ConnectionManager -> unique_ptr<ConnectionPair>` 所有权；
- 两个 Direction 独立转发；
- 固定容量 `Buffer pending`、短写和动态 `EPOLLOUT`；
- FIN / 半关闭 / RST / Close 幂等；
- PipelineSnapshot 与 `ExecutionState`；
- 空 Pipeline Fast Path。

阶段9是在这些语义之上增加“时间门控”能力，而不是另起一套数据路径。

---

## 3. 阶段9核心设计决定

### D-S9-001：Latency 是延迟 continuation，不是阻塞线程

```text
选择：LatencyToxic 把 Chunk + ToxicOutput 移入 Timer callback，Process() 立即返回。
拒绝：std::this_thread::sleep_for。
原因：sleep 会阻塞单线程 EventLoop，使所有连接、Timer、accept、EPOLLOUT、FIN/RST 同时停住。
```

### D-S9-002：Scheduler 使用窄接口

```text
选择：新增 Scheduler 抽象，只暴露 Now / ScheduleAt / CancelTimer。
生产：EventLoop 实现 Scheduler。
测试：FakeScheduler 实现 Scheduler。
原因：Latency 不应依赖 epoll/timerfd 细节；虚拟时间测试不依赖真实 sleep。
```

### D-S9-003：Jitter 使用有界均匀分布

```text
选择：jitter_delta ∈ Uniform[-jitter, +jitter]。
effective_delay = max(0, latency + jitter_delta)。
随机源：SeededRandomSource。
原因：第一版边界明确、可重复、容易测试。
```

### D-S9-004：DirectionRuntimeState 属于 Direction，而不是 Toxic / PipelineSnapshot

```text
保存：
- random
- latency_release_barrier

原因：Pipeline Replace 后旧 Snapshot 和新 Snapshot 仍属于同一个字节流方向；
顺序约束必须跨 Snapshot 存活，不能随着某个 LatencyToxic 被删除而消失。
```

### D-S9-005：release barrier 只做调度层保序优化，Sequence Gate 做最终正确性

```text
Latency deadline:
final_deadline = max(raw_deadline, direction.latency_release_barrier)

但仅靠它无法解决：
V1: A → Latency(5s)
Replace
V2: Empty
B → 立即完成

所以所有输入 Chunk 都分配 PipelineSequence；
最终提交给 pending 必须按 sequence 顺序。
```

### D-S9-006：Sequence Gate 以一个 input execution 为提交单位

```text
阶段8支持 1→0 / 1→1 / 1→N。
因此一个 PipelineSequence 保存：
- outputs[]
- complete

只有该 sequence 的 ExecutionState 完成后，才允许按顺序提交该 sequence 的全部 outputs，
然后推进 next_commit_sequence。
```

> 说明：这是阶段9版本。阶段10 Bandwidth 为了真正流式限速，会继续升级成“当前最老 sequence 可流式提交 output，跨 sequence 仍保序”。阶段9本文保持当时实现语义。

### D-S9-007：Direction 统一排队预算

```text
QueuedBytes = pending.Size()
            + delayed_bytes
            + reorder_bytes
```

```text
queued >= high → 暂停 source EPOLLIN
queued <= low  → 恢复 source EPOLLIN
超过 hard_limit → Close(kBufferLimitExceeded)
```

`delayed → reorder → pending` 只是所有权状态迁移，总 queued 不因此减少；只有实际 `send()` 消费或关闭释放数据才真正解除压力。

### D-S9-008：Timer Cancel 必须立即释放 callback

阶段7的惰性 Cancel 只标记 id，callback 仍保存在 heap 节点。阶段9 callback 会捕获 `Chunk + continuation`，如果 Latency 很长，取消后继续保存 callback 会造成大量内存长时间滞留。

因此阶段9改为：

```text
heap：只保存(deadline, id)
callbacks：unordered_map<TimerId, TimerCallback>
Cancel(id)：立即 erase callback
heap tombstone：以后到堆顶再清理
known_ids：防止 tombstone 未清理前复用 TimerId
```

### D-S9-009：Timer callback 有批处理预算

```text
TimerQueue::RunExpired(now, budget)
EventLoop 每次 Timer event 最多执行 256 个 callback
```

预算耗尽但仍有过期 Timer 时，重新 arm 到极短时间（1ns），把调度权归还 EventLoop，而不是一个 Timer storm 连续占满 Reactor。

### D-S9-010：ConnectionManager 用 dispatch_depth 延迟析构

阶段8中 Pipeline Output / Complete 可能同步嵌套回到 Manager。如果最内层 callback 关闭 Connection 并立即 `reset(unique_ptr)`，外层 `ConnectionPair` 成员函数仍在执行，会产生 self-destruction / UAF。

阶段9增加 `dispatch_depth_`：

```text
进入任何 Manager dispatch → depth++
Close → 只 pending_destroy
最外层 dispatch 返回 → depth==0
→ CollectPendingClosed()
→ 才真正 reset ConnectionPair
```

---

## 4. 阶段9目录增量

阶段8后目录保持不变：

```text
chaosproxy/
├── include/chaosproxy/
│   ├── net/
│   ├── reactor/
│   ├── proxy/
│   └── toxic/
├── src/
│   ├── net/
│   ├── reactor/
│   ├── proxy/
│   └── toxic/
└── tests/
```

### 阶段9新增文件

```text
include/chaosproxy/reactor/scheduler.h
include/chaosproxy/toxic/random_source.h
include/chaosproxy/toxic/direction_runtime.h
include/chaosproxy/toxic/latency_toxic.h

src/toxic/random_source.cpp
src/toxic/latency_toxic.cpp

tests/unit/reactor/timer_queue_stage9_test.cpp
tests/unit/toxic/latency_toxic_test.cpp
tests/integration/latency_integration_test.cpp
```

### 阶段9修改文件

```text
CMakeLists.txt

include/chaosproxy/reactor/timer_queue.h
include/chaosproxy/reactor/event_loop.h
src/reactor/timer_queue.cpp
src/reactor/event_loop.cpp

include/chaosproxy/toxic/toxic.h
src/toxic/toxic.cpp

include/chaosproxy/proxy/connection.h
include/chaosproxy/proxy/connection_manager.h
src/proxy/connection.cpp
src/proxy/connection_manager.cpp

tests/helpers/test_connection_harness.h
```

---

## 5. 阶段9完整数据路径

```text
source Endpoint EPOLLIN
        ↓
TryRead
        ↓
Direction 接纳输入
        ↓
分配 PipelineSequence
        ↓
Pipeline Empty ?
   ┌────┴─────────┐
  yes             no
   │               ↓
   │          PipelineSnapshot
   │               ↓
   │          LatencyToxic
   │               ↓
   │      base latency + jitter
   │               ↓
   │         raw_deadline
   │               ↓
   │   DirectionRuntime release_barrier
   │               ↓
   │        reserve delayed_bytes
   │               ↓
   │          ScheduleAt
   │               ↓
   │   Timer持有Chunk + continuation
   │               ↓
   │        Process立即返回
   │
   │       ---- deadline到期 ----
   │               ↓
   │         unregister TimerId
   │               ↓
   │       release delayed_bytes
   │               ↓
   │       output(std::move(chunk))
   │               ↓
   │      继续原 PipelineSnapshot
   │               ↓
   │      OnPipelineOutput(sequence)
   │               ↓
   │          reorder result
   │               ↓
   │     ExecutionState Complete
   │               ↓
   └──────→ Sequence Gate
                    ↓
                  pending
                    ↓
                TryWrite
                    ↓
        short write / EAGAIN / EPOLLOUT
                    ↓
          send成功N字节才真正释放queued
```

---

## 6. 动态 Pipeline Replace 的顺序保证

场景：

```text
V1:
A(seq=0) → Latency(5s)

随后 Replace：
V2 = Empty Pipeline

B(seq=1) → Pipeline立即完成
```

不能让 B 直接进入 pending。阶段9处理为：

```text
B(seq=1) complete
→ pipeline_results[1] ready
→ next_commit_sequence仍然是0
→ B等待

5s后A(seq=0)完成
→ 提交A
→ next_commit_sequence=1
→ 再提交B
```

最终仍为：

```text
A → B
```

PipelineSnapshot 和 Sequence Gate 的职责必须分开：

```text
PipelineSnapshot：一个Chunk继续哪一版策略
Sequence Gate：不同Chunk最终以什么输入顺序进入pending
```

---

## 7. 内存预算与背压

### 7.1 分类

```text
delayed_bytes：仍由 Latency/Timer 持有
reorder_bytes：Pipeline已输出但被 Sequence Gate 挡住
pending_bytes：已经可以写 Socket，但尚未全部发送
```

总量：

```text
queued = pending + delayed + reorder
```

### 7.2 高低水位

```text
queued >= high_watermark
→ read_paused = true
→ source 取消 EPOLLIN

queued <= low_watermark
→ read_paused = false
→ source 恢复 EPOLLIN
```

### 7.3 Hard Limit

阶段9默认：

```cpp
std::size_t hard_limit{512 * 1024};
```

如果新的异步/输出数据会突破 hard limit：

```text
CloseReason::kBufferLimitExceeded
```

第一版选择关闭异常连接，而不是允许内存无限增长。

### 7.4 什么时候 queued 真正减少

```text
Latency → reorder → pending
```

都是状态迁移，queued 不减少。

只有：

```text
TryWrite成功 N 字节
→ pending Consume(N)
→ queued相应减少
```

或者连接关闭后所有剩余数据被统一释放。

---

## 8. Latency / Jitter 语义

### 固定延迟

```text
raw_deadline = now + latency
```

### Jitter

```text
jitter_delta ∈ [-jitter, +jitter]

effective_delay = max(0, latency + jitter_delta)
```

### Direction release barrier

```text
final_deadline = max(raw_deadline, previous_release_barrier)
release_barrier = final_deadline
```

它使经过 Latency 的相邻 Chunk 尽量按同方向输入顺序到期，减少无意义的提前 callback 和 reorder 压力。

但最终顺序仍由 Sequence Gate 兜底。

### Latency 的严格含义

```text
配置 latency = 200ms
```

表示：

> Chunk 在该 LatencyToxic 中不会早于其 deadline 继续。

不保证：

- 200ms 时 `send()` 一定成功；
- 200ms 时对端已经收到；
- 客户端观测的端到端延迟恰好 200ms。

后续还可能被 Sequence Gate、Socket EAGAIN、其他 Toxic、EventLoop 调度等继续延迟。

---

## 9. TimerQueue 阶段9修订

阶段7：

```text
Cancel → cancelled set
heap中 callback 仍保留
```

阶段9：

```text
heap node = deadline + TimerId
callback_map[id] = callback
known_ids 保存所有仍存在heap/tombstone的id

Cancel(id)
→ callback_map.erase(id)
→ callback立即析构
→ Chunk/continuation立即释放
→ heap节点以后惰性清理
```

`RunExpired(now, budget)`：

```text
只执行 deadline <= now 的有效任务
最多执行 budget 个callback
预算耗尽后返回EventLoop
```

相同 deadline 仍然使用递增 TimerId 作为第二排序键，保证注册顺序确定。

---

## 10. Connection 生命周期与延迟 Timer

Latency Timer 不能保存：

```cpp
ConnectionPair*
Direction*
```

它保存的是：

```text
DirectionContext
→ ConnectionToken{id,generation}
→ source side
→ Scheduler
→ DirectionRuntimeState shared handle
→ Manager dispatch callbacks
```

Timer 到期以后，最终 Output / Complete 仍要回到 `ConnectionManager`：

```text
old token
→ Find(id,generation)
→ generation匹配才进入当前ConnectionPair
```

连接关闭时：

```text
Direction.delayed_timers
→ CancelTimer
→ callback capture立即释放
```

即使漏取消或 stale callback 已经排队，旧 `ConnectionToken` 仍然是身份安全兜底，不能命中新 generation。

---

## 11. CMake 与完整阶段9修改代码

以下代码来自阶段9实际构建/验证版本。未列出的 Stage8 文件保持不变。

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
    src/net/socket_ops.cpp
    src/net/buffer.cpp
    src/reactor/timer_queue.cpp
    src/reactor/event_loop.cpp
    src/proxy/listener.cpp
    src/proxy/endpoint.cpp
    src/toxic/chunk.cpp
    src/toxic/random_source.cpp
    src/toxic/toxic.cpp
    src/toxic/pass_through_toxic.cpp
    src/toxic/toxic_pipeline.cpp
    src/toxic/latency_toxic.cpp
    src/proxy/connection.cpp
    src/proxy/connection_manager.cpp
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
        tests/unit/net/unique_fd_test.cpp
        tests/unit/net/socket_ops_test.cpp
        tests/unit/net/buffer_test.cpp
        tests/unit/reactor/event_loop_test.cpp
        tests/unit/reactor/timer_queue_test.cpp
        tests/unit/reactor/timer_queue_stage9_test.cpp
        tests/unit/reactor/event_loop_timer_test.cpp
        tests/unit/proxy/connection_test.cpp
        tests/unit/toxic/toxic_pipeline_test.cpp
        tests/unit/toxic/latency_toxic_test.cpp
        tests/integration/listener_event_loop_test.cpp
        tests/integration/multi_connection_proxy_test.cpp
        tests/integration/backpressure_test.cpp
        tests/integration/half_close_test.cpp
        tests/integration/connection_error_test.cpp
        tests/integration/connection_generation_test.cpp
        tests/integration/connection_pipeline_test.cpp
        tests/integration/pipeline_generation_test.cpp
        tests/integration/latency_integration_test.cpp
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

### `include/chaosproxy/reactor/scheduler.h`

```cpp
#pragma once

#include "chaosproxy/reactor/clock.h"
#include "chaosproxy/reactor/timer_queue.h"

#include <utility>

namespace chaosproxy {

class Scheduler {
public:
    virtual ~Scheduler() = default;

    [[nodiscard]] virtual TimePoint Now() const noexcept = 0;

    [[nodiscard]] virtual TimerId ScheduleAt(
        TimePoint deadline,
        TimerCallback callback) = 0;

    [[nodiscard]] virtual bool CancelTimer(TimerId id) = 0;

    [[nodiscard]] TimerId ScheduleAfter(
        Duration delay,
        TimerCallback callback) {
        if (delay < Duration::zero()) {
            delay = Duration::zero();
        }
        return ScheduleAt(Now() + delay, std::move(callback));
    }
};

}  // namespace chaosproxy
```

### `include/chaosproxy/reactor/timer_queue.h`

```cpp
#pragma once

#include "chaosproxy/reactor/clock.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace chaosproxy {

using TimerId = std::uint64_t;
using TimerCallback = std::function<void()>;

class TimerQueue final {
public:
    [[nodiscard]] TimerId Schedule(
        TimePoint deadline,
        TimerCallback callback);

    // Cancel releases the callback immediately. The small heap tombstone is
    // removed lazily when it reaches the top.
    [[nodiscard]] bool Cancel(TimerId id) noexcept;

    [[nodiscard]] std::optional<TimePoint> NextDeadline();

    std::size_t RunExpired(
        TimePoint now,
        std::size_t max_callbacks =
            std::numeric_limits<std::size_t>::max());

    [[nodiscard]] std::size_t Size() const noexcept;
    [[nodiscard]] bool Empty() const noexcept;

private:
    struct TimerEntry {
        TimePoint deadline;
        TimerId id;
    };

    struct TimerCompare {
        bool operator()(
            const TimerEntry& lhs,
            const TimerEntry& rhs) const noexcept;
    };

    [[nodiscard]] TimerId NextId();
    void PurgeInactiveTop() noexcept;

    std::priority_queue<
        TimerEntry,
        std::vector<TimerEntry>,
        TimerCompare> timers_;

    // known_ids_ includes cancelled heap tombstones so TimerId is not reused
    // while an old heap entry with that id still exists.
    std::unordered_set<TimerId> known_ids_;
    std::unordered_map<TimerId, TimerCallback> callbacks_;
    TimerId next_id_{1};
};

}  // namespace chaosproxy
```

### `src/reactor/timer_queue.cpp`

```cpp
#include "chaosproxy/reactor/timer_queue.h"

#include <stdexcept>
#include <utility>

namespace chaosproxy {

bool TimerQueue::TimerCompare::operator()(
    const TimerEntry& lhs,
    const TimerEntry& rhs) const noexcept {
    if (lhs.deadline != rhs.deadline) {
        return lhs.deadline > rhs.deadline;
    }
    return lhs.id > rhs.id;
}

TimerId TimerQueue::Schedule(
    TimePoint deadline,
    TimerCallback callback) {
    if (!callback) {
        throw std::invalid_argument("timer callback must not be empty");
    }

    const TimerId id = NextId();
    known_ids_.insert(id);
    callbacks_.emplace(id, std::move(callback));
    timers_.push(TimerEntry{deadline, id});
    return id;
}

bool TimerQueue::Cancel(TimerId id) noexcept {
    if (id == 0) {
        return false;
    }

    // Erasing the callback eagerly is important for LatencyToxic: a cancelled
    // long-delay timer must release its retained Chunk/continuation now, not
    // when the heap tombstone eventually reaches the top.
    return callbacks_.erase(id) != 0;
}

std::optional<TimePoint> TimerQueue::NextDeadline() {
    PurgeInactiveTop();

    if (timers_.empty()) {
        return std::nullopt;
    }

    return timers_.top().deadline;
}

std::size_t TimerQueue::RunExpired(
    TimePoint now,
    std::size_t max_callbacks) {
    std::size_t executed = 0;

    while (executed < max_callbacks) {
        PurgeInactiveTop();

        if (timers_.empty() || timers_.top().deadline > now) {
            break;
        }

        const TimerEntry entry = timers_.top();
        timers_.pop();
        known_ids_.erase(entry.id);

        auto callback_it = callbacks_.find(entry.id);
        if (callback_it == callbacks_.end()) {
            continue;
        }

        TimerCallback callback = std::move(callback_it->second);
        callbacks_.erase(callback_it);

        callback();
        ++executed;
    }

    return executed;
}

std::size_t TimerQueue::Size() const noexcept {
    return callbacks_.size();
}

bool TimerQueue::Empty() const noexcept {
    return callbacks_.empty();
}

TimerId TimerQueue::NextId() {
    for (;;) {
        const TimerId candidate = next_id_++;
        if (candidate != 0 && known_ids_.find(candidate) == known_ids_.end()) {
            return candidate;
        }
    }
}

void TimerQueue::PurgeInactiveTop() noexcept {
    while (!timers_.empty()) {
        const TimerId id = timers_.top().id;
        if (callbacks_.find(id) != callbacks_.end()) {
            break;
        }

        timers_.pop();
        known_ids_.erase(id);
    }
}

}  // namespace chaosproxy
```

### `include/chaosproxy/reactor/event_loop.h`

```cpp
#pragma once

#include "chaosproxy/net/unique_fd.h"
#include "chaosproxy/reactor/scheduler.h"
#include "chaosproxy/reactor/timer_queue.h"

#include <sys/epoll.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <vector>

namespace chaosproxy {

using EventToken = std::uint64_t;
using EventCallback = std::function<void(EventToken, std::uint32_t)>;

class EventLoop final : public Scheduler {
public:
    explicit EventLoop(std::size_t max_events = 128);

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    [[nodiscard]] EventToken Add(
        int fd,
        std::uint32_t interests,
        EventCallback callback);

    void Modify(EventToken token, std::uint32_t interests);
    [[nodiscard]] int Remove(EventToken token) noexcept;
    [[nodiscard]] bool Contains(EventToken token) const noexcept;

    [[nodiscard]] TimePoint Now() const noexcept override;

    [[nodiscard]] TimerId ScheduleAt(
        TimePoint deadline,
        TimerCallback callback) override;

    [[nodiscard]] TimerId ScheduleAfter(
        Duration delay,
        TimerCallback callback);

    [[nodiscard]] bool CancelTimer(TimerId id) override;

    int RunOnce(int timeout_ms);
    void Run();
    void Stop() noexcept;

private:
    struct Registration {
        int fd;
        std::uint32_t interests;
        EventCallback callback;
    };

    [[nodiscard]] EventToken NextToken();

    void HandleTimerEvent(
        EventToken token,
        std::uint32_t events);

    void DrainTimerFd();
    void RearmTimerFd();

    UniqueFd epoll_fd_;
    std::vector<epoll_event> ready_events_;
    std::unordered_map<EventToken, Registration> registrations_;
    EventToken next_token_{1};
    bool stop_requested_{false};

    TimerQueue timer_queue_;
    UniqueFd timer_fd_;
    EventToken timer_token_{0};
};

}  // namespace chaosproxy
```

### `src/reactor/event_loop.cpp`

```cpp
#include "chaosproxy/reactor/event_loop.h"

#include <sys/timerfd.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace chaosproxy {
namespace {

constexpr std::int64_t kNanosecondsPerSecond = 1000000000LL;

}  // namespace

EventLoop::EventLoop(std::size_t max_events) {
    if (max_events == 0 ||
        max_events > static_cast<std::size_t>(
            std::numeric_limits<int>::max())) {
        throw std::invalid_argument("invalid epoll event capacity");
    }

    const int raw_epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
    if (raw_epoll_fd < 0) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "epoll_create1");
    }
    epoll_fd_.Reset(raw_epoll_fd);
    ready_events_.resize(max_events);

    const int raw_timer_fd = ::timerfd_create(
        CLOCK_MONOTONIC,
        TFD_NONBLOCK | TFD_CLOEXEC);
    if (raw_timer_fd < 0) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "timerfd_create");
    }
    timer_fd_.Reset(raw_timer_fd);

    timer_token_ = Add(
        timer_fd_.Get(),
        EPOLLIN,
        [this](EventToken token, std::uint32_t events) {
            HandleTimerEvent(token, events);
        });
}

EventToken EventLoop::NextToken() {
    for (;;) {
        const EventToken candidate = next_token_++;
        if (candidate != 0 && !Contains(candidate)) {
            return candidate;
        }
    }
}

EventToken EventLoop::Add(
    int fd,
    std::uint32_t interests,
    EventCallback callback) {
    if (fd < 0 || !callback) {
        throw std::invalid_argument("invalid epoll registration");
    }

    const EventToken token = NextToken();
    registrations_.emplace(
        token,
        Registration{fd, interests, std::move(callback)});

    epoll_event event{};
    event.events = interests;
    event.data.u64 = token;

    if (::epoll_ctl(
            epoll_fd_.Get(),
            EPOLL_CTL_ADD,
            fd,
            &event) < 0) {
        const int error_number = errno;
        registrations_.erase(token);
        throw std::system_error(
            error_number,
            std::generic_category(),
            "epoll_ctl ADD");
    }

    return token;
}

void EventLoop::Modify(
    EventToken token,
    std::uint32_t interests) {
    auto iterator = registrations_.find(token);
    if (iterator == registrations_.end()) {
        throw std::invalid_argument("unknown epoll token");
    }

    epoll_event event{};
    event.events = interests;
    event.data.u64 = token;

    if (::epoll_ctl(
            epoll_fd_.Get(),
            EPOLL_CTL_MOD,
            iterator->second.fd,
            &event) < 0) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "epoll_ctl MOD");
    }

    iterator->second.interests = interests;
}

int EventLoop::Remove(EventToken token) noexcept {
    auto iterator = registrations_.find(token);
    if (iterator == registrations_.end()) {
        return 0;
    }

    int result = 0;
    do {
        result = ::epoll_ctl(
            epoll_fd_.Get(),
            EPOLL_CTL_DEL,
            iterator->second.fd,
            nullptr);
    } while (result < 0 && errno == EINTR);

    int error_number = 0;
    if (result < 0 && errno != ENOENT && errno != EBADF) {
        error_number = errno;
    }

    registrations_.erase(iterator);
    return error_number;
}

bool EventLoop::Contains(EventToken token) const noexcept {
    return registrations_.find(token) != registrations_.end();
}

TimePoint EventLoop::Now() const noexcept {
    return MonotonicClock::now();
}

TimerId EventLoop::ScheduleAt(
    TimePoint deadline,
    TimerCallback callback) {
    const TimerId id = timer_queue_.Schedule(
        deadline,
        std::move(callback));
    RearmTimerFd();
    return id;
}

TimerId EventLoop::ScheduleAfter(
    Duration delay,
    TimerCallback callback) {
    if (delay < Duration::zero()) {
        delay = Duration::zero();
    }

    return ScheduleAt(
        Now() + delay,
        std::move(callback));
}

bool EventLoop::CancelTimer(TimerId id) {
    const bool cancelled = timer_queue_.Cancel(id);
    if (!cancelled) {
        return false;
    }

    RearmTimerFd();
    return true;
}

int EventLoop::RunOnce(int timeout_ms) {
    int ready_count = 0;

    do {
        ready_count = ::epoll_wait(
            epoll_fd_.Get(),
            ready_events_.data(),
            static_cast<int>(ready_events_.size()),
            timeout_ms);
    } while (ready_count < 0 && errno == EINTR);

    if (ready_count < 0) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "epoll_wait");
    }

    for (int index = 0; index < ready_count; ++index) {
        const epoll_event& event = ready_events_[index];
        const EventToken token = event.data.u64;

        const auto iterator = registrations_.find(token);
        if (iterator == registrations_.end()) {
            continue;
        }

        EventCallback callback = iterator->second.callback;
        callback(token, event.events);
    }

    return ready_count;
}

void EventLoop::Run() {
    stop_requested_ = false;
    while (!stop_requested_) {
        (void)RunOnce(-1);
    }
}

void EventLoop::Stop() noexcept {
    stop_requested_ = true;
}

void EventLoop::HandleTimerEvent(
    EventToken token,
    std::uint32_t events) {
    if (token != timer_token_) {
        return;
    }

    if ((events & EPOLLIN) == 0U) {
        return;
    }

    DrainTimerFd();
    (void)timer_queue_.RunExpired(Now(), 256);
    RearmTimerFd();
}

void EventLoop::DrainTimerFd() {
    for (;;) {
        std::uint64_t expirations = 0;
        const ssize_t result = ::read(
            timer_fd_.Get(),
            &expirations,
            sizeof(expirations));

        if (result == static_cast<ssize_t>(sizeof(expirations))) {
            continue;
        }

        if (result < 0 && errno == EINTR) {
            continue;
        }

        if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return;
        }

        if (result < 0) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "read timerfd");
        }

        throw std::runtime_error("short read from timerfd");
    }
}

void EventLoop::RearmTimerFd() {
    itimerspec specification{};

    const std::optional<TimePoint> next_deadline =
        timer_queue_.NextDeadline();

    if (next_deadline.has_value()) {
        Duration delay = *next_deadline - Now();
        if (delay <= Duration::zero()) {
            delay = std::chrono::nanoseconds(1);
        }

        auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(delay);
        if (nanoseconds <= std::chrono::nanoseconds::zero()) {
            nanoseconds = std::chrono::nanoseconds(1);
        }

        const std::int64_t count = nanoseconds.count();
        specification.it_value.tv_sec =
            static_cast<time_t>(count / kNanosecondsPerSecond);
        specification.it_value.tv_nsec =
            static_cast<long>(count % kNanosecondsPerSecond);
    }

    if (::timerfd_settime(
            timer_fd_.Get(),
            0,
            &specification,
            nullptr) < 0) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "timerfd_settime");
    }
}

}  // namespace chaosproxy
```

### `include/chaosproxy/toxic/random_source.h`

```cpp
#pragma once

#include <cstdint>
#include <random>

namespace chaosproxy {

class RandomSource {
public:
    virtual ~RandomSource() = default;

    [[nodiscard]] virtual std::int64_t UniformInt64(
        std::int64_t min_value,
        std::int64_t max_value) = 0;
};

class SeededRandomSource final : public RandomSource {
public:
    explicit SeededRandomSource(std::uint64_t seed) noexcept;

    [[nodiscard]] std::int64_t UniformInt64(
        std::int64_t min_value,
        std::int64_t max_value) override;

private:
    std::mt19937_64 engine_;
};

}  // namespace chaosproxy
```

### `src/toxic/random_source.cpp`

```cpp
#include "chaosproxy/toxic/random_source.h"

#include <stdexcept>

namespace chaosproxy {

SeededRandomSource::SeededRandomSource(std::uint64_t seed) noexcept
    : engine_(seed) {}

std::int64_t SeededRandomSource::UniformInt64(
    std::int64_t min_value,
    std::int64_t max_value) {
    if (min_value > max_value) {
        throw std::invalid_argument("invalid random interval");
    }

    std::uniform_int_distribution<std::int64_t> distribution(
        min_value,
        max_value);
    return distribution(engine_);
}

}  // namespace chaosproxy
```

### `include/chaosproxy/toxic/direction_runtime.h`

```cpp
#pragma once

#include "chaosproxy/reactor/clock.h"
#include "chaosproxy/toxic/random_source.h"

#include <memory>
#include <optional>

namespace chaosproxy {

struct DirectionRuntimeState final {
    explicit DirectionRuntimeState(
        std::shared_ptr<RandomSource> random_source)
        : random(std::move(random_source)) {}

    std::shared_ptr<RandomSource> random;
    std::optional<TimePoint> latency_release_barrier;
};

}  // namespace chaosproxy
```

### `include/chaosproxy/toxic/toxic.h`

```cpp
#pragma once

#include "chaosproxy/proxy/connection_token.h"
#include "chaosproxy/proxy/endpoint.h"
#include "chaosproxy/reactor/scheduler.h"
#include "chaosproxy/toxic/chunk.h"
#include "chaosproxy/toxic/direction_runtime.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

namespace chaosproxy {

class EventLoop;

class DirectionContext final {
public:
    using ReserveDelayedBytesCallback = std::function<bool(
        ConnectionToken,
        EndpointSide,
        std::size_t)>;

    using ReleaseDelayedBytesCallback = std::function<void(
        ConnectionToken,
        EndpointSide,
        std::size_t)>;

    using RegisterTimerCallback = std::function<bool(
        ConnectionToken,
        EndpointSide,
        TimerId)>;

    using UnregisterTimerCallback = std::function<void(
        ConnectionToken,
        EndpointSide,
        TimerId)>;

    // Compatibility/test constructor. It keeps the stage8 call shape and
    // creates deterministic per-context random/runtime state. Delayed-byte
    // accounting is a no-op unless the extended constructor is used.
    DirectionContext(
        EventLoop& loop,
        ConnectionToken connection_token,
        EndpointSide source_side);

    DirectionContext(
        Scheduler& scheduler,
        ConnectionToken connection_token,
        EndpointSide source_side,
        std::shared_ptr<DirectionRuntimeState> runtime,
        ReserveDelayedBytesCallback reserve_delayed_bytes,
        ReleaseDelayedBytesCallback release_delayed_bytes,
        RegisterTimerCallback register_timer,
        UnregisterTimerCallback unregister_timer);

    [[nodiscard]] ConnectionToken Connection() const noexcept;
    [[nodiscard]] EndpointSide SourceSide() const noexcept;
    [[nodiscard]] TimePoint Now() const noexcept;

    [[nodiscard]] TimerId ScheduleAt(
        TimePoint deadline,
        TimerCallback callback) const;

    [[nodiscard]] TimerId ScheduleAfter(
        Duration delay,
        TimerCallback callback) const;

    [[nodiscard]] bool CancelTimer(TimerId id) const;

    [[nodiscard]] std::int64_t UniformInt64(
        std::int64_t min_value,
        std::int64_t max_value) const;

    // Direction-wide release barrier. It survives PipelineSnapshot replacement
    // because runtime is owned by the Direction, not by LatencyToxic.
    [[nodiscard]] TimePoint ConstrainLatencyDeadline(
        TimePoint candidate) const noexcept;

    [[nodiscard]] bool TryReserveDelayedBytes(std::size_t bytes) const;
    void ReleaseDelayedBytes(std::size_t bytes) const noexcept;

    [[nodiscard]] bool RegisterTimer(TimerId id) const;
    void UnregisterTimer(TimerId id) const noexcept;

private:
    Scheduler* scheduler_;
    ConnectionToken connection_token_;
    EndpointSide source_side_;
    std::shared_ptr<DirectionRuntimeState> runtime_;
    ReserveDelayedBytesCallback reserve_delayed_bytes_;
    ReleaseDelayedBytesCallback release_delayed_bytes_;
    RegisterTimerCallback register_timer_;
    UnregisterTimerCallback unregister_timer_;
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

### `src/toxic/toxic.cpp`

```cpp
#include "chaosproxy/toxic/toxic.h"

#include "chaosproxy/reactor/event_loop.h"
#include "chaosproxy/toxic/random_source.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace chaosproxy {

DirectionContext::DirectionContext(
    EventLoop& loop,
    ConnectionToken connection_token,
    EndpointSide source_side)
    : DirectionContext(
          loop,
          connection_token,
          source_side,
          std::make_shared<DirectionRuntimeState>(
              std::make_shared<SeededRandomSource>(0)),
          [](ConnectionToken, EndpointSide, std::size_t) {
              return true;
          },
          [](ConnectionToken, EndpointSide, std::size_t) {},
          [](ConnectionToken, EndpointSide, TimerId) {
              return true;
          },
          [](ConnectionToken, EndpointSide, TimerId) {}) {}

DirectionContext::DirectionContext(
    Scheduler& scheduler,
    ConnectionToken connection_token,
    EndpointSide source_side,
    std::shared_ptr<DirectionRuntimeState> runtime,
    ReserveDelayedBytesCallback reserve_delayed_bytes,
    ReleaseDelayedBytesCallback release_delayed_bytes,
    RegisterTimerCallback register_timer,
    UnregisterTimerCallback unregister_timer)
    : scheduler_(&scheduler),
      connection_token_(connection_token),
      source_side_(source_side),
      runtime_(std::move(runtime)),
      reserve_delayed_bytes_(std::move(reserve_delayed_bytes)),
      release_delayed_bytes_(std::move(release_delayed_bytes)),
      register_timer_(std::move(register_timer)),
      unregister_timer_(std::move(unregister_timer)) {
    if (scheduler_ == nullptr ||
        !connection_token_.IsValid() ||
        !runtime_ ||
        !runtime_->random ||
        !reserve_delayed_bytes_ ||
        !release_delayed_bytes_ ||
        !register_timer_ ||
        !unregister_timer_) {
        throw std::invalid_argument("invalid direction context");
    }
}

ConnectionToken DirectionContext::Connection() const noexcept {
    return connection_token_;
}

EndpointSide DirectionContext::SourceSide() const noexcept {
    return source_side_;
}

TimePoint DirectionContext::Now() const noexcept {
    return scheduler_->Now();
}

TimerId DirectionContext::ScheduleAt(
    TimePoint deadline,
    TimerCallback callback) const {
    return scheduler_->ScheduleAt(deadline, std::move(callback));
}

TimerId DirectionContext::ScheduleAfter(
    Duration delay,
    TimerCallback callback) const {
    return scheduler_->ScheduleAfter(delay, std::move(callback));
}

bool DirectionContext::CancelTimer(TimerId id) const {
    return scheduler_->CancelTimer(id);
}

std::int64_t DirectionContext::UniformInt64(
    std::int64_t min_value,
    std::int64_t max_value) const {
    return runtime_->random->UniformInt64(min_value, max_value);
}

TimePoint DirectionContext::ConstrainLatencyDeadline(
    TimePoint candidate) const noexcept {
    if (runtime_->latency_release_barrier.has_value() &&
        candidate < *runtime_->latency_release_barrier) {
        candidate = *runtime_->latency_release_barrier;
    }

    runtime_->latency_release_barrier = candidate;
    return candidate;
}

bool DirectionContext::TryReserveDelayedBytes(std::size_t bytes) const {
    return reserve_delayed_bytes_(
        connection_token_,
        source_side_,
        bytes);
}

void DirectionContext::ReleaseDelayedBytes(std::size_t bytes) const noexcept {
    try {
        release_delayed_bytes_(connection_token_, source_side_, bytes);
    } catch (...) {
        // Accounting cleanup must not escape from timer callback destruction.
    }
}

bool DirectionContext::RegisterTimer(TimerId id) const {
    return register_timer_(connection_token_, source_side_, id);
}

void DirectionContext::UnregisterTimer(TimerId id) const noexcept {
    try {
        unregister_timer_(connection_token_, source_side_, id);
    } catch (...) {
        // Timer bookkeeping must not escape from a callback.
    }
}

}  // namespace chaosproxy
```

### `include/chaosproxy/toxic/latency_toxic.h`

```cpp
#pragma once

#include "chaosproxy/toxic/toxic.h"

#include <chrono>

namespace chaosproxy {

using DelayDuration = std::chrono::microseconds;

class LatencyToxic final : public Toxic {
public:
    explicit LatencyToxic(
        DelayDuration latency,
        DelayDuration jitter = DelayDuration::zero());

    [[nodiscard]] DelayDuration Latency() const noexcept;
    [[nodiscard]] DelayDuration Jitter() const noexcept;

    void Process(
        Chunk chunk,
        DirectionContext context,
        ToxicOutput output) override;

private:
    DelayDuration latency_;
    DelayDuration jitter_;
};

}  // namespace chaosproxy
```

### `src/toxic/latency_toxic.cpp`

```cpp
#include "chaosproxy/toxic/latency_toxic.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <utility>

namespace chaosproxy {
namespace {

struct PendingLatencyRelease final {
    PendingLatencyRelease(
        Chunk owned_chunk,
        ToxicOutput continuation,
        DirectionContext direction_context,
        std::size_t owned_bytes)
        : chunk(std::move(owned_chunk)),
          output(std::move(continuation)),
          context(std::move(direction_context)),
          bytes(owned_bytes) {}

    Chunk chunk;
    ToxicOutput output;
    DirectionContext context;
    std::size_t bytes{0};
    TimerId timer_id{0};
};

}  // namespace

LatencyToxic::LatencyToxic(
    DelayDuration latency,
    DelayDuration jitter)
    : latency_(latency),
      jitter_(jitter) {
    if (latency_ < DelayDuration::zero() ||
        jitter_ < DelayDuration::zero()) {
        throw std::invalid_argument(
            "latency and jitter must be non-negative");
    }

    const auto max = std::numeric_limits<std::int64_t>::max();
    if (latency_.count() > max - jitter_.count()) {
        throw std::invalid_argument("latency+jitter is too large");
    }
}

DelayDuration LatencyToxic::Latency() const noexcept {
    return latency_;
}

DelayDuration LatencyToxic::Jitter() const noexcept {
    return jitter_;
}

void LatencyToxic::Process(
    Chunk chunk,
    DirectionContext context,
    ToxicOutput output) {
    if (!output) {
        throw std::invalid_argument("latency output must not be empty");
    }

    const std::int64_t jitter_delta = context.UniformInt64(
        -jitter_.count(),
        jitter_.count());

    std::int64_t delay_us = 0;
    if (jitter_delta < 0 && latency_.count() < -jitter_delta) {
        delay_us = 0;
    } else {
        delay_us = latency_.count() + jitter_delta;
    }

    const TimePoint raw_deadline =
        context.Now() + DelayDuration(delay_us);
    const TimePoint deadline =
        context.ConstrainLatencyDeadline(raw_deadline);

    const std::size_t bytes = chunk.Size();
    if (!context.TryReserveDelayedBytes(bytes)) {
        return;
    }

    auto pending = std::make_shared<PendingLatencyRelease>(
        std::move(chunk),
        std::move(output),
        context,
        bytes);

    try {
        const TimerId id = context.ScheduleAt(
            deadline,
            [pending]() mutable {
                const TimerId timer_id = pending->timer_id;
                pending->context.UnregisterTimer(timer_id);

                // Transfer ownership out of the latency queue before emitting.
                // Backpressure refresh happens in Output/Complete, avoiding a
                // transient EPOLLIN resume between delayed and reorder state.
                pending->context.ReleaseDelayedBytes(pending->bytes);

                Chunk chunk = std::move(pending->chunk);
                ToxicOutput output = std::move(pending->output);
                output(std::move(chunk));
            });

        pending->timer_id = id;

        if (!context.RegisterTimer(id)) {
            (void)context.CancelTimer(id);
            context.ReleaseDelayedBytes(bytes);
        }
    } catch (...) {
        context.ReleaseDelayedBytes(bytes);
        throw;
    }
}

}  // namespace chaosproxy
```

### `include/chaosproxy/proxy/connection.h`

```cpp
#pragma once

#include "chaosproxy/net/buffer.h"
#include "chaosproxy/net/unique_fd.h"
#include "chaosproxy/proxy/connection_token.h"
#include "chaosproxy/proxy/endpoint.h"
#include "chaosproxy/reactor/event_loop.h"
#include "chaosproxy/toxic/chunk.h"
#include "chaosproxy/toxic/direction_runtime.h"
#include "chaosproxy/toxic/toxic_pipeline.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <unordered_set>
#include <vector>

namespace chaosproxy {

using PipelineSequence = std::uint64_t;

enum class CloseReason {
    kNone,
    kGracefulEof,
    kConnectFailed,
    kConnectionReset,
    kReadError,
    kWriteError,
    kSocketError,
    kShutdownError,
    kBufferLimitExceeded,
    kLocalStop,
    kInternalError
};

struct ConnectionLimits {
    std::size_t buffer_capacity{256 * 1024};
    std::size_t high_watermark{192 * 1024};
    std::size_t low_watermark{128 * 1024};
    std::size_t hard_limit{512 * 1024};
    std::size_t read_budget_per_event{64 * 1024};
    std::size_t write_budget_per_event{64 * 1024};
    std::uint64_t random_seed{0x4348414F53505258ULL};
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
        PipelineSequence,
        Chunk)>;

    using PipelineCompletionDispatchCallback = std::function<void(
        ConnectionToken,
        EndpointSide,
        PipelineSequence)>;

    using ReserveDelayedBytesDispatchCallback = std::function<bool(
        ConnectionToken,
        EndpointSide,
        std::size_t)>;

    using ReleaseDelayedBytesDispatchCallback = std::function<void(
        ConnectionToken,
        EndpointSide,
        std::size_t)>;

    using RegisterTimerDispatchCallback = std::function<bool(
        ConnectionToken,
        EndpointSide,
        TimerId)>;

    using UnregisterTimerDispatchCallback = std::function<void(
        ConnectionToken,
        EndpointSide,
        TimerId)>;

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
        ReserveDelayedBytesDispatchCallback reserve_delayed_bytes_dispatch_callback,
        ReleaseDelayedBytesDispatchCallback release_delayed_bytes_dispatch_callback,
        RegisterTimerDispatchCallback register_timer_dispatch_callback,
        UnregisterTimerDispatchCallback unregister_timer_dispatch_callback,
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

    [[nodiscard]] std::size_t QueuedBytes(
        EndpointSide source) const noexcept;

    [[nodiscard]] std::size_t DelayedBytes(
        EndpointSide source) const noexcept;

    [[nodiscard]] std::size_t ReorderBytes(
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

    void OnEvent(
        EndpointSide side,
        EventToken token,
        std::uint32_t events) noexcept;

    // Manager-facing asynchronous continuation entries. They are public so
    // focused tests can provide a lightweight dispatch harness; production
    // callbacks must still reach them through ConnectionManager token checks.
    void OnPipelineOutput(
        EndpointSide source_side,
        PipelineSequence sequence,
        Chunk chunk) noexcept;

    void OnPipelineComplete(
        EndpointSide source_side,
        PipelineSequence sequence) noexcept;

    [[nodiscard]] bool TryReserveDelayedBytes(
        EndpointSide source_side,
        std::size_t bytes) noexcept;

    void ReleaseDelayedBytes(
        EndpointSide source_side,
        std::size_t bytes) noexcept;

    [[nodiscard]] bool RegisterDelayedTimer(
        EndpointSide source_side,
        TimerId id) noexcept;

    void UnregisterDelayedTimer(
        EndpointSide source_side,
        TimerId id) noexcept;

private:
    friend class ConnectionManager;

    struct PipelineResult {
        std::vector<Chunk> outputs;
        std::size_t next_output{0};
        bool complete{false};
    };

    struct Direction {
        Direction(
            EndpointSide source_side,
            EndpointSide destination_side,
            std::size_t capacity,
            std::shared_ptr<DirectionRuntimeState> runtime_state)
            : source(source_side),
              destination(destination_side),
              pending(capacity),
              runtime(std::move(runtime_state)) {}

        EndpointSide source;
        EndpointSide destination;
        Buffer pending;
        ToxicPipeline pipeline;
        std::shared_ptr<DirectionRuntimeState> runtime;

        PipelineSequence next_input_sequence{0};
        PipelineSequence next_commit_sequence{0};
        std::map<PipelineSequence, PipelineResult> pipeline_results;
        std::size_t pipeline_inflight{0};

        std::size_t delayed_bytes{0};
        std::size_t reorder_bytes{0};
        std::unordered_set<TimerId> delayed_timers;

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

    void TryDrainSequenceGate(Direction& direction);

    void FlushDirection(
        Direction& direction,
        std::size_t& write_budget);

    void UpdateBackpressure(Direction& direction) noexcept;
    void TryFinishDirection(Direction& direction);
    void MaybeFinishConnection();

    [[nodiscard]] bool CanUseDirectFastPath(
        const Direction& direction) const noexcept;

    [[nodiscard]] std::size_t QueuedBytes(
        const Direction& direction) const noexcept;

    [[nodiscard]] std::size_t RemainingHardBudget(
        const Direction& direction) const noexcept;

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
    void CancelDirectionTimers(Direction& direction) noexcept;
    void Cleanup() noexcept;

    EventLoop& loop_;
    ConnectionToken token_;
    ConnectionLimits limits_;
    EventDispatchCallback event_dispatch_callback_;
    PipelineOutputDispatchCallback pipeline_output_dispatch_callback_;
    PipelineCompletionDispatchCallback pipeline_completion_dispatch_callback_;
    ReserveDelayedBytesDispatchCallback reserve_delayed_bytes_dispatch_callback_;
    ReleaseDelayedBytesDispatchCallback release_delayed_bytes_dispatch_callback_;
    RegisterTimerDispatchCallback register_timer_dispatch_callback_;
    UnregisterTimerDispatchCallback unregister_timer_dispatch_callback_;
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

### `src/proxy/connection.cpp`

```cpp
#include "chaosproxy/proxy/connection.h"

#include "chaosproxy/net/socket_ops.h"
#include "chaosproxy/toxic/random_source.h"

#include <sys/epoll.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <limits>
#include <stdexcept>
#include <utility>

namespace chaosproxy {
namespace {

constexpr std::size_t kReadChunkSize = 16 * 1024;

void ValidateLimits(const ConnectionLimits& limits) {
    if (limits.buffer_capacity == 0 ||
        limits.low_watermark >= limits.high_watermark ||
        limits.high_watermark > limits.buffer_capacity ||
        limits.hard_limit < limits.high_watermark ||
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
    return error_number == ECONNRESET || error_number == EPIPE
        ? CloseReason::kConnectionReset
        : CloseReason::kWriteError;
}

std::uint64_t MixSeed(std::uint64_t value) noexcept {
    value += 0x9E3779B97F4A7C15ULL;
    value = (value ^ (value >> 30U)) * 0xBF58476D1CE4E5B9ULL;
    value = (value ^ (value >> 27U)) * 0x94D049BB133111EBULL;
    return value ^ (value >> 31U);
}

std::uint64_t DirectionSeed(
    std::uint64_t base_seed,
    ConnectionToken token,
    EndpointSide side) noexcept {
    std::uint64_t value = MixSeed(base_seed ^ token.id);
    value = MixSeed(value ^ token.generation);
    value = MixSeed(value ^ static_cast<std::uint64_t>(side == EndpointSide::kClient ? 1 : 2));
    return value;
}

std::shared_ptr<DirectionRuntimeState> MakeDirectionRuntime(
    std::uint64_t base_seed,
    ConnectionToken token,
    EndpointSide side) {
    return std::make_shared<DirectionRuntimeState>(
        std::make_shared<SeededRandomSource>(
            DirectionSeed(base_seed, token, side)));
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
    ReserveDelayedBytesDispatchCallback reserve_delayed_bytes_dispatch_callback,
    ReleaseDelayedBytesDispatchCallback release_delayed_bytes_dispatch_callback,
    RegisterTimerDispatchCallback register_timer_dispatch_callback,
    UnregisterTimerDispatchCallback unregister_timer_dispatch_callback,
    ClosedCallback closed_callback)
    : loop_(loop),
      token_(token),
      limits_(limits),
      event_dispatch_callback_(std::move(event_dispatch_callback)),
      pipeline_output_dispatch_callback_(
          std::move(pipeline_output_dispatch_callback)),
      pipeline_completion_dispatch_callback_(
          std::move(pipeline_completion_dispatch_callback)),
      reserve_delayed_bytes_dispatch_callback_(
          std::move(reserve_delayed_bytes_dispatch_callback)),
      release_delayed_bytes_dispatch_callback_(
          std::move(release_delayed_bytes_dispatch_callback)),
      register_timer_dispatch_callback_(
          std::move(register_timer_dispatch_callback)),
      unregister_timer_dispatch_callback_(
          std::move(unregister_timer_dispatch_callback)),
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
          limits.buffer_capacity,
          MakeDirectionRuntime(limits.random_seed, token, EndpointSide::kClient)),
      upstream_to_client_(
          EndpointSide::kUpstream,
          EndpointSide::kClient,
          limits.buffer_capacity,
          MakeDirectionRuntime(limits.random_seed, token, EndpointSide::kUpstream)) {
    ValidateLimits(limits_);

    if (!token_.IsValid() ||
        !client_.HasFd() ||
        !upstream_.HasFd() ||
        !event_dispatch_callback_ ||
        !pipeline_output_dispatch_callback_ ||
        !pipeline_completion_dispatch_callback_ ||
        !reserve_delayed_bytes_dispatch_callback_ ||
        !release_delayed_bytes_dispatch_callback_ ||
        !register_timer_dispatch_callback_ ||
        !unregister_timer_dispatch_callback_ ||
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

    const auto add_endpoint = [&](Endpoint& endpoint) {
        const EndpointSide side = endpoint.Side();
        const std::uint32_t interests = DesiredInterests(side);

        const EventToken event_token = loop_.Add(
            endpoint.Fd(),
            interests,
            [connection_token, side, dispatch](
                EventToken observed_token,
                std::uint32_t events) {
                dispatch(connection_token, side, observed_token, events);
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

std::size_t ConnectionPair::QueuedBytes(
    EndpointSide source) const noexcept {
    return QueuedBytes(SourceDirection(source));
}

std::size_t ConnectionPair::DelayedBytes(
    EndpointSide source) const noexcept {
    return SourceDirection(source).delayed_bytes;
}

std::size_t ConnectionPair::ReorderBytes(
    EndpointSide source) const noexcept {
    return SourceDirection(source).reorder_bytes;
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
    PipelineSequence sequence,
    Chunk chunk) noexcept {
    if (closed_ || chunk.Empty()) {
        return;
    }

    try {
        Direction& direction = SourceDirection(source_side);
        auto result_it = direction.pipeline_results.find(sequence);
        if (result_it == direction.pipeline_results.end() ||
            result_it->second.complete) {
            Close(CloseReason::kInternalError);
            return;
        }

        if (chunk.Size() > RemainingHardBudget(direction)) {
            Close(CloseReason::kBufferLimitExceeded);
            return;
        }

        direction.reorder_bytes += chunk.Size();
        result_it->second.outputs.push_back(std::move(chunk));
        UpdateBackpressure(direction);

        if (!closed_) {
            RefreshInterests();
        }
    } catch (...) {
        Close(CloseReason::kInternalError);
    }
}

void ConnectionPair::OnPipelineComplete(
    EndpointSide source_side,
    PipelineSequence sequence) noexcept {
    if (closed_) {
        return;
    }

    try {
        Direction& direction = SourceDirection(source_side);
        auto result_it = direction.pipeline_results.find(sequence);
        if (result_it == direction.pipeline_results.end() ||
            result_it->second.complete ||
            direction.pipeline_inflight == 0) {
            Close(CloseReason::kInternalError);
            return;
        }

        result_it->second.complete = true;
        --direction.pipeline_inflight;
        TryDrainSequenceGate(direction);

        std::size_t write_budget = limits_.write_budget_per_event;
        FlushDirection(direction, write_budget);

        if (closed_) {
            return;
        }

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

bool ConnectionPair::TryReserveDelayedBytes(
    EndpointSide source_side,
    std::size_t bytes) noexcept {
    if (closed_) {
        return false;
    }

    Direction& direction = SourceDirection(source_side);
    if (bytes > RemainingHardBudget(direction)) {
        Close(CloseReason::kBufferLimitExceeded);
        return false;
    }

    direction.delayed_bytes += bytes;
    UpdateBackpressure(direction);

    try {
        if (!closed_) {
            RefreshInterests();
        }
    } catch (...) {
        Close(CloseReason::kInternalError);
        return false;
    }

    return !closed_;
}

void ConnectionPair::ReleaseDelayedBytes(
    EndpointSide source_side,
    std::size_t bytes) noexcept {
    if (closed_) {
        return;
    }

    Direction& direction = SourceDirection(source_side);
    if (bytes > direction.delayed_bytes) {
        Close(CloseReason::kInternalError);
        return;
    }

    // Do not refresh EPOLLIN here. The same continuation will either emit a
    // Chunk or complete/drop; those callbacks refresh backpressure once the
    // ownership transfer is stable.
    direction.delayed_bytes -= bytes;
}

bool ConnectionPair::RegisterDelayedTimer(
    EndpointSide source_side,
    TimerId id) noexcept {
    if (closed_ || id == 0) {
        return false;
    }

    Direction& direction = SourceDirection(source_side);
    return direction.delayed_timers.insert(id).second;
}

void ConnectionPair::UnregisterDelayedTimer(
    EndpointSide source_side,
    TimerId id) noexcept {
    if (closed_ || id == 0) {
        return;
    }

    SourceDirection(source_side).delayed_timers.erase(id);
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
        const std::size_t hard_available = RemainingHardBudget(direction);
        if (hard_available == 0) {
            direction.read_paused = true;
            break;
        }

        std::size_t capacity = std::min(
            {buffer.size(), hard_available, read_budget});

        if (CanUseDirectFastPath(direction)) {
            capacity = std::min(capacity, direction.pending.Available());
            if (capacity == 0) {
                direction.read_paused = true;
                break;
            }
        }

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
    if (direction.next_input_sequence ==
        std::numeric_limits<PipelineSequence>::max()) {
        Close(CloseReason::kInternalError);
        return false;
    }

    const PipelineSequence sequence = direction.next_input_sequence++;

    if (CanUseDirectFastPath(direction)) {
        if (!direction.pending.Append(data, size)) {
            Close(CloseReason::kBufferLimitExceeded);
            return false;
        }

        ++direction.next_commit_sequence;
        return true;
    }

    auto [result_it, inserted] = direction.pipeline_results.emplace(
        sequence,
        PipelineResult{});
    if (!inserted) {
        Close(CloseReason::kInternalError);
        return false;
    }

    if (direction.pipeline.Empty()) {
        if (size > RemainingHardBudget(direction)) {
            Close(CloseReason::kBufferLimitExceeded);
            return false;
        }

        result_it->second.outputs.emplace_back(data, size);
        result_it->second.complete = true;
        direction.reorder_bytes += size;
        TryDrainSequenceGate(direction);
        return !closed_;
    }

    const ConnectionToken connection_token = token_;
    const EndpointSide source_side = direction.source;
    const PipelineOutputDispatchCallback output_dispatch =
        pipeline_output_dispatch_callback_;
    const PipelineCompletionDispatchCallback completion_dispatch =
        pipeline_completion_dispatch_callback_;

    DirectionContext context(
        loop_,
        connection_token,
        source_side,
        direction.runtime,
        reserve_delayed_bytes_dispatch_callback_,
        release_delayed_bytes_dispatch_callback_,
        register_timer_dispatch_callback_,
        unregister_timer_dispatch_callback_);

    Chunk chunk(data, size);
    ++direction.pipeline_inflight;

    direction.pipeline.Process(
        std::move(chunk),
        context,
        [connection_token,
         source_side,
         sequence,
         output_dispatch](Chunk output) mutable {
            output_dispatch(
                connection_token,
                source_side,
                sequence,
                std::move(output));
        },
        [connection_token,
         source_side,
         sequence,
         completion_dispatch] {
            completion_dispatch(
                connection_token,
                source_side,
                sequence);
        });

    return !closed_;
}

void ConnectionPair::TryDrainSequenceGate(Direction& direction) {
    for (;;) {
        auto result_it = direction.pipeline_results.find(
            direction.next_commit_sequence);
        if (result_it == direction.pipeline_results.end() ||
            !result_it->second.complete) {
            return;
        }

        PipelineResult& result = result_it->second;

        while (result.next_output < result.outputs.size()) {
            Chunk& chunk = result.outputs[result.next_output];
            if (chunk.Size() > direction.pending.Capacity()) {
                Close(CloseReason::kBufferLimitExceeded);
                return;
            }

            if (chunk.Size() > direction.pending.Available()) {
                return;
            }

            if (!direction.pending.Append(chunk.Data(), chunk.Size())) {
                Close(CloseReason::kInternalError);
                return;
            }

            if (chunk.Size() > direction.reorder_bytes) {
                Close(CloseReason::kInternalError);
                return;
            }

            direction.reorder_bytes -= chunk.Size();
            ++result.next_output;
        }

        direction.pipeline_results.erase(result_it);
        ++direction.next_commit_sequence;
    }
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

    TryDrainSequenceGate(direction);
    if (closed_) {
        return;
    }

    while (write_budget > 0) {
        if (direction.pending.Empty()) {
            TryDrainSequenceGate(direction);
            if (closed_ || direction.pending.Empty()) {
                break;
            }
        }

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
            TryDrainSequenceGate(direction);
            if (closed_) {
                return;
            }
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

    const std::size_t queued = QueuedBytes(direction);

    if (!direction.read_paused && queued >= limits_.high_watermark) {
        direction.read_paused = true;
        return;
    }

    if (direction.read_paused && queued <= limits_.low_watermark) {
        direction.read_paused = false;
    }
}

void ConnectionPair::TryFinishDirection(Direction& direction) {
    if (!direction.source_eof ||
        direction.pipeline_inflight != 0 ||
        !direction.pipeline_results.empty() ||
        direction.delayed_bytes != 0 ||
        !direction.delayed_timers.empty() ||
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
               direction.pipeline_results.empty() &&
               direction.delayed_bytes == 0 &&
               direction.delayed_timers.empty() &&
               direction.pending.Empty() &&
               direction.write_shutdown;
    };

    if (finished(client_to_upstream_) &&
        finished(upstream_to_client_)) {
        Close(CloseReason::kGracefulEof);
    }
}

bool ConnectionPair::CanUseDirectFastPath(
    const Direction& direction) const noexcept {
    return direction.pipeline.Empty() &&
           direction.pipeline_inflight == 0 &&
           direction.pipeline_results.empty() &&
           direction.next_input_sequence == direction.next_commit_sequence;
}

std::size_t ConnectionPair::QueuedBytes(
    const Direction& direction) const noexcept {
    return direction.pending.Size() +
           direction.delayed_bytes +
           direction.reorder_bytes;
}

std::size_t ConnectionPair::RemainingHardBudget(
    const Direction& direction) const noexcept {
    const std::size_t queued = QueuedBytes(direction);
    return queued >= limits_.hard_limit
        ? 0
        : limits_.hard_limit - queued;
}

Endpoint& ConnectionPair::GetEndpoint(
    EndpointSide side) noexcept {
    return side == EndpointSide::kClient ? client_ : upstream_;
}

const Endpoint& ConnectionPair::GetEndpoint(
    EndpointSide side) const noexcept {
    return side == EndpointSide::kClient ? client_ : upstream_;
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
    if (!read_direction.source_eof && !read_direction.read_paused) {
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

        if (error_number == ENOTCONN || error_number == EPIPE) {
            return false;
        }

        return false;
    }
}

void ConnectionPair::CancelDirectionTimers(Direction& direction) noexcept {
    std::vector<TimerId> timer_ids(
        direction.delayed_timers.begin(),
        direction.delayed_timers.end());

    direction.delayed_timers.clear();

    for (TimerId id : timer_ids) {
        (void)loop_.CancelTimer(id);
    }
}

void ConnectionPair::Cleanup() noexcept {
    CancelDirectionTimers(client_to_upstream_);
    CancelDirectionTimers(upstream_to_client_);

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

### `include/chaosproxy/proxy/connection_manager.h`

```cpp
#pragma once

#include "chaosproxy/net/unique_fd.h"
#include "chaosproxy/proxy/connection.h"
#include "chaosproxy/reactor/event_loop.h"

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
    void CollectPendingClosed() noexcept;

    void EnterDispatch() noexcept;
    void LeaveDispatch() noexcept;

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
        PipelineSequence sequence,
        Chunk chunk) noexcept;

    void DispatchPipelineComplete(
        ConnectionToken connection_token,
        EndpointSide source_side,
        PipelineSequence sequence) noexcept;

    [[nodiscard]] bool DispatchReserveDelayedBytes(
        ConnectionToken connection_token,
        EndpointSide source_side,
        std::size_t bytes) noexcept;

    void DispatchReleaseDelayedBytes(
        ConnectionToken connection_token,
        EndpointSide source_side,
        std::size_t bytes) noexcept;

    [[nodiscard]] bool DispatchRegisterTimer(
        ConnectionToken connection_token,
        EndpointSide source_side,
        TimerId id) noexcept;

    void DispatchUnregisterTimer(
        ConnectionToken connection_token,
        EndpointSide source_side,
        TimerId id) noexcept;

    void OnClosed(
        ConnectionToken token,
        CloseReason reason) noexcept;

    EventLoop& loop_;
    sockaddr_storage upstream_address_{};
    socklen_t upstream_address_length_{0};
    ConnectionLimits limits_;

    std::vector<Slot> slots_;
    std::vector<ConnectionId> free_ids_;
    std::size_t active_count_{0};
    std::size_t dispatch_depth_{0};
};

}  // namespace chaosproxy
```

### `src/proxy/connection_manager.cpp`

```cpp
#include "chaosproxy/proxy/connection_manager.h"

#include "chaosproxy/net/socket_ops.h"

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
                PipelineSequence sequence,
                Chunk chunk) {
                DispatchPipelineOutput(
                    connection_token,
                    source_side,
                    sequence,
                    std::move(chunk));
            },
            [this](
                ConnectionToken connection_token,
                EndpointSide source_side,
                PipelineSequence sequence) {
                DispatchPipelineComplete(
                    connection_token,
                    source_side,
                    sequence);
            },
            [this](
                ConnectionToken connection_token,
                EndpointSide source_side,
                std::size_t bytes) {
                return DispatchReserveDelayedBytes(
                    connection_token,
                    source_side,
                    bytes);
            },
            [this](
                ConnectionToken connection_token,
                EndpointSide source_side,
                std::size_t bytes) {
                DispatchReleaseDelayedBytes(
                    connection_token,
                    source_side,
                    bytes);
            },
            [this](
                ConnectionToken connection_token,
                EndpointSide source_side,
                TimerId id) {
                return DispatchRegisterTimer(
                    connection_token,
                    source_side,
                    id);
            },
            [this](
                ConnectionToken connection_token,
                EndpointSide source_side,
                TimerId id) {
                DispatchUnregisterTimer(
                    connection_token,
                    source_side,
                    id);
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

    if (dispatch_depth_ == 0) {
        CollectClosed(token);
    }
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

    if (dispatch_depth_ == 0) {
        CollectPendingClosed();
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

    if (slots_.size() >= static_cast<std::size_t>(
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

    if (!slot->pending_destroy || !slot->connection->IsClosed()) {
        return;
    }

    ReleaseSlot(token);
}

void ConnectionManager::CollectPendingClosed() noexcept {
    for (std::size_t index = 0; index < slots_.size(); ++index) {
        Slot& slot = slots_[index];
        if (!slot.connection || !slot.pending_destroy ||
            !slot.connection->IsClosed()) {
            continue;
        }

        const ConnectionToken token{
            static_cast<ConnectionId>(index + 1),
            slot.generation
        };
        ReleaseSlot(token);
    }
}

void ConnectionManager::EnterDispatch() noexcept {
    ++dispatch_depth_;
}

void ConnectionManager::LeaveDispatch() noexcept {
    if (dispatch_depth_ == 0) {
        return;
    }

    --dispatch_depth_;
    if (dispatch_depth_ == 0) {
        CollectPendingClosed();
    }
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
    EnterDispatch();

    if (ConnectionPair* connection = Find(connection_token)) {
        connection->OnEvent(side, event_token, events);
    }

    LeaveDispatch();
}

void ConnectionManager::DispatchPipelineOutput(
    ConnectionToken connection_token,
    EndpointSide source_side,
    PipelineSequence sequence,
    Chunk chunk) noexcept {
    EnterDispatch();

    if (ConnectionPair* connection = Find(connection_token)) {
        connection->OnPipelineOutput(
            source_side,
            sequence,
            std::move(chunk));
    }

    LeaveDispatch();
}

void ConnectionManager::DispatchPipelineComplete(
    ConnectionToken connection_token,
    EndpointSide source_side,
    PipelineSequence sequence) noexcept {
    EnterDispatch();

    if (ConnectionPair* connection = Find(connection_token)) {
        connection->OnPipelineComplete(source_side, sequence);
    }

    LeaveDispatch();
}

bool ConnectionManager::DispatchReserveDelayedBytes(
    ConnectionToken connection_token,
    EndpointSide source_side,
    std::size_t bytes) noexcept {
    EnterDispatch();

    bool reserved = false;
    if (ConnectionPair* connection = Find(connection_token)) {
        reserved = connection->TryReserveDelayedBytes(source_side, bytes);
    }

    LeaveDispatch();
    return reserved;
}

void ConnectionManager::DispatchReleaseDelayedBytes(
    ConnectionToken connection_token,
    EndpointSide source_side,
    std::size_t bytes) noexcept {
    EnterDispatch();

    if (ConnectionPair* connection = Find(connection_token)) {
        connection->ReleaseDelayedBytes(source_side, bytes);
    }

    LeaveDispatch();
}

bool ConnectionManager::DispatchRegisterTimer(
    ConnectionToken connection_token,
    EndpointSide source_side,
    TimerId id) noexcept {
    EnterDispatch();

    bool registered = false;
    if (ConnectionPair* connection = Find(connection_token)) {
        registered = connection->RegisterDelayedTimer(source_side, id);
    }

    LeaveDispatch();
    return registered;
}

void ConnectionManager::DispatchUnregisterTimer(
    ConnectionToken connection_token,
    EndpointSide source_side,
    TimerId id) noexcept {
    EnterDispatch();

    if (ConnectionPair* connection = Find(connection_token)) {
        connection->UnregisterDelayedTimer(source_side, id);
    }

    LeaveDispatch();
}

void ConnectionManager::OnClosed(
    ConnectionToken token,
    CloseReason /*reason*/) noexcept {
    Slot* slot = FindSlot(token);
    if (slot == nullptr) {
        return;
    }

    // Reclamation is deferred until the outermost Manager dispatch returns.
    // This covers nested synchronous Pipeline Output/Complete callbacks as
    // well as the original EventLoop callback self-destruction hazard.
    slot->pending_destroy = true;
}

}  // namespace chaosproxy
```

### `tests/helpers/test_connection_harness.h`

```cpp
#pragma once

#include "chaosproxy/proxy/connection.h"

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
                if (Alive(observed_connection)) {
                    connection_->OnEvent(side, event_token, events);
                }
            },
            [this](
                ConnectionToken observed_connection,
                EndpointSide source,
                PipelineSequence sequence,
                Chunk chunk) {
                if (Alive(observed_connection)) {
                    connection_->OnPipelineOutput(
                        source,
                        sequence,
                        std::move(chunk));
                }
            },
            [this](
                ConnectionToken observed_connection,
                EndpointSide source,
                PipelineSequence sequence) {
                if (Alive(observed_connection)) {
                    connection_->OnPipelineComplete(source, sequence);
                }
            },
            [this](
                ConnectionToken observed_connection,
                EndpointSide source,
                std::size_t bytes) {
                return Alive(observed_connection) &&
                       connection_->TryReserveDelayedBytes(source, bytes);
            },
            [this](
                ConnectionToken observed_connection,
                EndpointSide source,
                std::size_t bytes) {
                if (Alive(observed_connection)) {
                    connection_->ReleaseDelayedBytes(source, bytes);
                }
            },
            [this](
                ConnectionToken observed_connection,
                EndpointSide source,
                TimerId id) {
                return Alive(observed_connection) &&
                       connection_->RegisterDelayedTimer(source, id);
            },
            [this](
                ConnectionToken observed_connection,
                EndpointSide source,
                TimerId id) {
                if (Alive(observed_connection)) {
                    connection_->UnregisterDelayedTimer(source, id);
                }
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
    [[nodiscard]] bool Alive(ConnectionToken token) const noexcept {
        return connection_ != nullptr &&
               !connection_->IsClosed() &&
               connection_->Token() == token;
    }

    std::unique_ptr<ConnectionPair> connection_;
};

}  // namespace chaosproxy::test
```

### `tests/unit/reactor/timer_queue_stage9_test.cpp`

```cpp
#include "chaosproxy/reactor/timer_queue.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>

namespace {

using namespace chaosproxy;
using namespace std::chrono_literals;

TEST(TimerQueueStage9Test, ExpiredWorkRespectsBatchBudget) {
    TimerQueue queue;
    const TimePoint now{};
    int fired = 0;

    for (int index = 0; index < 5; ++index) {
        (void)queue.Schedule(now, [&] { ++fired; });
    }

    EXPECT_EQ(queue.RunExpired(now, 2), 2U);
    EXPECT_EQ(fired, 2);
    EXPECT_EQ(queue.RunExpired(now, 2), 2U);
    EXPECT_EQ(fired, 4);
    EXPECT_EQ(queue.RunExpired(now, 2), 1U);
    EXPECT_EQ(fired, 5);
}

TEST(TimerQueueStage9Test, CancelReleasesCallbackCaptureImmediately) {
    TimerQueue queue;
    bool released = false;

    struct Guard final {
        explicit Guard(bool& flag) : flag(flag) {}
        ~Guard() { flag = true; }
        bool& flag;
    };

    auto guard = std::make_shared<Guard>(released);
    const TimerId id = queue.Schedule(
        TimePoint{} + 1h,
        [guard] {});

    guard.reset();
    EXPECT_FALSE(released);

    EXPECT_TRUE(queue.Cancel(id));
    EXPECT_TRUE(released);
    EXPECT_TRUE(queue.Empty());
}

}  // namespace
```

### `tests/unit/toxic/latency_toxic_test.cpp`

```cpp
#include "chaosproxy/reactor/scheduler.h"
#include "chaosproxy/reactor/timer_queue.h"
#include "chaosproxy/toxic/direction_runtime.h"
#include "chaosproxy/toxic/latency_toxic.h"
#include "chaosproxy/toxic/random_source.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace chaosproxy;
using namespace std::chrono_literals;

class FakeScheduler final : public Scheduler {
public:
    [[nodiscard]] TimePoint Now() const noexcept override {
        return now_;
    }

    [[nodiscard]] TimerId ScheduleAt(
        TimePoint deadline,
        TimerCallback callback) override {
        return timers_.Schedule(deadline, std::move(callback));
    }

    [[nodiscard]] bool CancelTimer(TimerId id) override {
        return timers_.Cancel(id);
    }

    void AdvanceTo(TimePoint target) {
        now_ = target;
        (void)timers_.RunExpired(now_);
    }

    [[nodiscard]] std::optional<TimePoint> NextDeadline() {
        return timers_.NextDeadline();
    }

private:
    TimePoint now_{};
    TimerQueue timers_;
};

class SequenceRandomSource final : public RandomSource {
public:
    explicit SequenceRandomSource(std::vector<std::int64_t> values)
        : values_(std::move(values)) {}

    [[nodiscard]] std::int64_t UniformInt64(
        std::int64_t min_value,
        std::int64_t max_value) override {
        if (index_ >= values_.size()) {
            ADD_FAILURE() << "random sequence exhausted";
            return min_value;
        }

        const std::int64_t value = values_[index_++];
        EXPECT_GE(value, min_value);
        EXPECT_LE(value, max_value);
        return value;
    }

private:
    std::vector<std::int64_t> values_;
    std::size_t index_{0};
};

struct ContextAccounting {
    std::size_t delayed_bytes{0};
    std::vector<TimerId> timers;
};

DirectionContext MakeContext(
    FakeScheduler& scheduler,
    std::shared_ptr<RandomSource> random,
    ContextAccounting& accounting) {
    auto runtime = std::make_shared<DirectionRuntimeState>(std::move(random));

    return DirectionContext(
        scheduler,
        ConnectionToken{1, 1},
        EndpointSide::kClient,
        std::move(runtime),
        [&](ConnectionToken, EndpointSide, std::size_t bytes) {
            accounting.delayed_bytes += bytes;
            return true;
        },
        [&](ConnectionToken, EndpointSide, std::size_t bytes) {
            ASSERT_GE(accounting.delayed_bytes, bytes);
            accounting.delayed_bytes -= bytes;
        },
        [&](ConnectionToken, EndpointSide, TimerId id) {
            accounting.timers.push_back(id);
            return true;
        },
        [&](ConnectionToken, EndpointSide, TimerId id) {
            auto it = std::find(
                accounting.timers.begin(),
                accounting.timers.end(),
                id);
            ASSERT_NE(it, accounting.timers.end());
            accounting.timers.erase(it);
        });
}

std::string ToString(const Chunk& chunk) {
    return chunk.Empty() ? std::string{} : std::string(chunk.Data(), chunk.Size());
}

TEST(LatencyToxicTest, DoesNotReleaseBeforeDeadline) {
    FakeScheduler scheduler;
    ContextAccounting accounting;
    DirectionContext context = MakeContext(
        scheduler,
        std::make_shared<SequenceRandomSource>(
            std::vector<std::int64_t>{0}),
        accounting);

    LatencyToxic toxic(200ms, 0ms);
    std::vector<std::string> outputs;

    toxic.Process(
        Chunk("hello", 5),
        context,
        [&](Chunk chunk) {
            outputs.push_back(ToString(chunk));
        });

    EXPECT_EQ(accounting.delayed_bytes, 5U);
    EXPECT_EQ(accounting.timers.size(), 1U);

    scheduler.AdvanceTo(TimePoint{} + 199ms);
    EXPECT_TRUE(outputs.empty());

    scheduler.AdvanceTo(TimePoint{} + 200ms);
    ASSERT_EQ(outputs.size(), 1U);
    EXPECT_EQ(outputs[0], "hello");
    EXPECT_EQ(accounting.delayed_bytes, 0U);
    EXPECT_TRUE(accounting.timers.empty());
}

TEST(LatencyToxicTest, NegativeJitterResultClampsToZeroDelay) {
    FakeScheduler scheduler;
    ContextAccounting accounting;
    DirectionContext context = MakeContext(
        scheduler,
        std::make_shared<SequenceRandomSource>(
            std::vector<std::int64_t>{-80000}),
        accounting);

    LatencyToxic toxic(20ms, 100ms);
    bool released = false;

    toxic.Process(
        Chunk("x", 1),
        context,
        [&](Chunk) { released = true; });

    const auto deadline = scheduler.NextDeadline();
    ASSERT_TRUE(deadline.has_value());
    EXPECT_EQ(*deadline, TimePoint{});

    scheduler.AdvanceTo(TimePoint{});
    EXPECT_TRUE(released);
}

TEST(LatencyToxicTest, ReleaseBarrierPreventsLaterChunkFromOvertaking) {
    FakeScheduler scheduler;
    ContextAccounting accounting;
    auto runtime = std::make_shared<DirectionRuntimeState>(
        std::make_shared<SequenceRandomSource>(
            std::vector<std::int64_t>{100000, -100000}));

    DirectionContext context(
        scheduler,
        ConnectionToken{2, 1},
        EndpointSide::kClient,
        runtime,
        [&](ConnectionToken, EndpointSide, std::size_t bytes) {
            accounting.delayed_bytes += bytes;
            return true;
        },
        [&](ConnectionToken, EndpointSide, std::size_t bytes) {
            accounting.delayed_bytes -= bytes;
        },
        [&](ConnectionToken, EndpointSide, TimerId id) {
            accounting.timers.push_back(id);
            return true;
        },
        [&](ConnectionToken, EndpointSide, TimerId id) {
            accounting.timers.erase(
                std::find(accounting.timers.begin(), accounting.timers.end(), id));
        });

    LatencyToxic toxic(200ms, 100ms);
    std::string order;

    toxic.Process(Chunk("A", 1), context, [&](Chunk) { order += 'A'; });
    scheduler.AdvanceTo(TimePoint{} + 10ms);
    toxic.Process(Chunk("B", 1), context, [&](Chunk) { order += 'B'; });

    // A raw deadline is 300ms. B raw deadline would be 110ms, but the
    // Direction-level release barrier raises B to 300ms.
    EXPECT_EQ(runtime->latency_release_barrier, TimePoint{} + 300ms);

    scheduler.AdvanceTo(TimePoint{} + 299ms);
    EXPECT_TRUE(order.empty());

    scheduler.AdvanceTo(TimePoint{} + 300ms);
    EXPECT_EQ(order, "AB");
}

}  // namespace
```

### `tests/integration/latency_integration_test.cpp`

```cpp
#include "chaosproxy/net/socket_ops.h"
#include "chaosproxy/proxy/connection_manager.h"
#include "chaosproxy/toxic/latency_toxic.h"
#include "helpers/test_socket_utils.h"

#include <arpa/inet.h>
#include <sys/socket.h>

#include <cerrno>
#include <chrono>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <system_error>
#include <utility>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;
using namespace std::chrono_literals;

UniqueFd MakeLatencyListener(sockaddr_in& address) {
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

UniqueFd AcceptLatencyPeer(EventLoop& loop, int listen_fd) {
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

TEST(LatencyIntegrationTest, DynamicRemovalCannotOvertakeOlderDelayedChunk) {
    sockaddr_in upstream_address{};
    UniqueFd listener = MakeLatencyListener(upstream_address);
    EventLoop loop;
    ConnectionLimits limits;
    limits.random_seed = 42;

    ConnectionManager manager(
        loop,
        reinterpret_cast<const sockaddr*>(&upstream_address),
        sizeof(upstream_address),
        limits);

    auto client_pair = MakeSocketPair();
    const CreateConnectionResult created =
        manager.Create(std::move(client_pair.second));
    ASSERT_TRUE(created.Ok());
    UniqueFd upstream = AcceptLatencyPeer(loop, listener.Get());
    ASSERT_TRUE(upstream.IsValid());

    ASSERT_TRUE(manager.ReplacePipeline(
        created.token,
        EndpointSide::kClient,
        {std::make_shared<LatencyToxic>(50ms, 0ms)}));

    ASSERT_EQ(
        TryWrite(client_pair.first.Get(), "A", 1).status,
        WriteStatus::kWritten);

    // A must enter the old snapshot before the pipeline is replaced.
    (void)loop.RunOnce(20);
    EXPECT_TRUE(DrainAvailable(upstream.Get()).empty());

    ASSERT_TRUE(manager.ReplacePipeline(
        created.token,
        EndpointSide::kClient,
        {}));

    ASSERT_EQ(
        TryWrite(client_pair.first.Get(), "B", 1).status,
        WriteStatus::kWritten);

    (void)loop.RunOnce(10);
    EXPECT_TRUE(DrainAvailable(upstream.Get()).empty());

    std::string observed;
    ASSERT_TRUE(PumpUntil(loop, [&] {
        observed += DrainAvailable(upstream.Get());
        return observed.size() == 2U;
    }, 100, 10));

    EXPECT_EQ(observed, "AB");
}

TEST(LatencyIntegrationTest, DelayedBytesParticipateInBackpressure) {
    sockaddr_in upstream_address{};
    UniqueFd listener = MakeLatencyListener(upstream_address);
    EventLoop loop;

    ConnectionLimits limits;
    limits.buffer_capacity = 16 * 1024;
    limits.low_watermark = 4 * 1024;
    limits.high_watermark = 8 * 1024;
    limits.hard_limit = 24 * 1024;
    limits.read_budget_per_event = 16 * 1024;
    limits.write_budget_per_event = 16 * 1024;
    limits.random_seed = 7;

    ConnectionManager manager(
        loop,
        reinterpret_cast<const sockaddr*>(&upstream_address),
        sizeof(upstream_address),
        limits);

    auto client_pair = MakeSocketPair();
    const CreateConnectionResult created =
        manager.Create(std::move(client_pair.second));
    ASSERT_TRUE(created.Ok());
    UniqueFd upstream = AcceptLatencyPeer(loop, listener.Get());

    ASSERT_TRUE(manager.ReplacePipeline(
        created.token,
        EndpointSide::kClient,
        {std::make_shared<LatencyToxic>(80ms, 0ms)}));

    const std::string data(12 * 1024, 'x');
    ASSERT_TRUE(WriteAllWithLoop(loop, client_pair.first.Get(), data));

    ConnectionPair* connection = manager.Find(created.token);
    ASSERT_NE(connection, nullptr);

    ASSERT_TRUE(PumpUntil(loop, [&] {
        return connection->ReadPaused(EndpointSide::kClient) &&
               connection->DelayedBytes(EndpointSide::kClient) >=
                   limits.high_watermark;
    }, 100, 2));

    EXPECT_GE(
        connection->QueuedBytes(EndpointSide::kClient),
        limits.high_watermark);
    EXPECT_TRUE(DrainAvailable(upstream.Get()).empty());

    std::string observed;
    ASSERT_TRUE(PumpUntil(loop, [&] {
        observed += DrainAvailable(upstream.Get());
        return observed.size() == data.size();
    }, 150, 10));

    EXPECT_EQ(observed, data);
    EXPECT_EQ(connection->QueuedBytes(EndpointSide::kClient), 0U);
    EXPECT_FALSE(connection->ReadPaused(EndpointSide::kClient));
}

TEST(LatencyIntegrationTest, CloseCancelsOldLatencyBeforeIdReuse) {
    sockaddr_in upstream_address{};
    UniqueFd listener = MakeLatencyListener(upstream_address);
    EventLoop loop;
    ConnectionManager manager(
        loop,
        reinterpret_cast<const sockaddr*>(&upstream_address),
        sizeof(upstream_address));

    auto first_client = MakeSocketPair();
    const CreateConnectionResult first =
        manager.Create(std::move(first_client.second));
    ASSERT_TRUE(first.Ok());
    UniqueFd first_upstream = AcceptLatencyPeer(loop, listener.Get());

    ASSERT_TRUE(manager.ReplacePipeline(
        first.token,
        EndpointSide::kClient,
        {std::make_shared<LatencyToxic>(100ms, 0ms)}));

    ASSERT_EQ(
        TryWrite(first_client.first.Get(), "old", 3).status,
        WriteStatus::kWritten);

    ASSERT_TRUE(PumpUntil(loop, [&] {
        ConnectionPair* connection = manager.Find(first.token);
        return connection != nullptr &&
               connection->DelayedBytes(EndpointSide::kClient) == 3U;
    }));

    ASSERT_TRUE(manager.Close(first.token));
    EXPECT_EQ(manager.Find(first.token), nullptr);

    auto second_client = MakeSocketPair();
    const CreateConnectionResult second =
        manager.Create(std::move(second_client.second));
    ASSERT_TRUE(second.Ok());
    UniqueFd second_upstream = AcceptLatencyPeer(loop, listener.Get());

    ASSERT_EQ(second.token.id, first.token.id);
    ASSERT_NE(second.token.generation, first.token.generation);

    // Pump beyond the old deadline. The cancelled callback cannot emit old
    // bytes, and generation validation is still a second safety net.
    for (int round = 0; round < 20; ++round) {
        (void)loop.RunOnce(10);
    }

    EXPECT_TRUE(DrainAvailable(second_upstream.Get()).empty());
    EXPECT_NE(manager.Find(second.token), nullptr);
}

}  // namespace
```

---

## 12. 阶段9测试目标

### LatencyToxic 单元测试

```text
[x] latency未到不释放
[x] deadline到达后释放
[x] jitter结果在上下界内
[x] latency+jitter为负时截断到0
[x] 固定seed可重复
[x] release barrier保证deadline不倒退
[x] delayed bytes reserve / release正确
[x] Timer注册/取消生命周期正确
```

### TimerQueue Stage9 回归

```text
[x] Cancel立即释放callback capture
[x] heap tombstone不会执行
[x] RunExpired budget限制callback数量
[x] 剩余过期Timer仍可在下一轮继续执行
```

### Latency 集成测试

```text
[x] 真实EventLoop中Latency不会提前透传
[x] V1 Latency → V2 Empty 仍保持A→B
[x] delayed bytes达到high触发source backpressure
[x] 真正写出后queued下降并恢复source读取
[x] half-close不会在异步pipeline结束前提前SHUT_WR
[x] stale token/generation不会把旧输出写入新连接
```

---

## 13. 本阶段实际验证

已在重建的阶段8重构基线上执行：

```text
[x] CMake + Ninja + BUILD_TESTING=OFF 构建通过
[x] chaosproxy_core 构建通过
[x] chaosproxy 链接通过
[x] 全部生产源码 -Wall -Wextra -Wpedantic -Werror 编译通过
[x] stage9_smoke：固定 Latency 不提前释放
[x] stage9_smoke：V1 Latency → V2 Empty 仍输出 A→B
[x] stage9_smoke：delayed bytes 达到 high 后触发背压
[x] stage9_smoke：数据真正写出后 queued 降低并恢复读取
[x] stage9_smoke：Timer batch budget 生效
[x] stage9_smoke：Cancel 立即释放 callback capture
[x] ASan + UBSan stage9_smoke 通过（LeakSanitizer关闭）
[x] 20个现有/新增 GTest 源文件使用最小 stub 做 -Werror 语法检查通过
[ ] 正式 GoogleTest / CTest：当前执行环境未安装 GoogleTest，需要在用户仓库执行
```

正式质量门：

```bash
cmake -S . -B build-debug \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DBUILD_TESTING=ON

cmake --build build-debug -j

ctest \
    --test-dir build-debug \
    --output-on-failure
```

只跑 Stage9：

```bash
./build-debug/chaosproxy_tests \
  --gtest_filter='LatencyToxicTest.*:LatencyIntegrationTest.*:TimerQueueStage9Test.*'
```

---

## 14. 阶段9新增/强化不变量

1. Reactor 线程禁止用 `sleep` 实现 Latency / Jitter。
2. LatencyToxic 只能延迟 continuation，不能自己直接 `send()`。
3. Toxic 的长期异步状态不得保存裸 `ConnectionPair* / Direction*`。
4. 连接长期身份必须继续使用 `ConnectionToken{id,generation}`。
5. PipelineSnapshot 负责策略版本一致性；ConnectionToken 负责连接实例身份；两者职责不能混用。
6. 每个 Direction 必须独立维护随机状态和 Latency release barrier。
7. Jitter 必须有明确上下界，随机源必须支持固定 seed。
8. 除非显式数据破坏策略，同一 Direction 的字节顺序不能改变。
9. release barrier 不能替代 Sequence Gate；跨 Pipeline Replace 的最终顺序由 Direction Sequence Gate 保证。
10. 所有输入 Chunk 必须分配单调 PipelineSequence；不能只给经过 Latency 的 Chunk 分配序号。
11. `pipeline_results` 中后序 sequence 即使先完成，也不能越过 `next_commit_sequence`。
12. 空 Pipeline 只有在不存在更老的异步 sequence 时才可直接走原 pending Fast Path；否则必须进入顺序门。
13. `queued = pending + delayed + reorder`，不能只统计 pending。
14. `delayed → reorder → pending` 只是状态迁移，不应减少总 queued。
15. 真正 `send()` 成功消费字节后才能正常减少总排队压力。
16. `queued >= high` 暂停 source；`queued <= low` 才恢复，避免阈值抖动。
17. `hard_limit` 是最终安全边界，超过时必须执行明确过载策略。
18. Connection 关闭时必须主动 Cancel Direction 持有的 TimerId。
19. Timer Cancel 必须释放 callback 捕获的 Chunk/continuation，不能让长延迟任务取消后继续占大块内存。
20. generation 是 stale callback 的身份安全兜底，不能替代正常 Cancel。
21. TimerQueue 一次不能无限运行所有 expired callback；必须有工作预算。
22. Timer budget 用尽后必须把控制权还给 EventLoop；剩余已过期 Timer 应尽快再次唤醒。
23. Timer callback 必须保持轻量，不在 callback 中无限循环 flush 网络数据。
24. source EOF 后，只要 pipeline / delayed / reorder / pending 中仍有数据，就不能提前 `shutdown(destination, SHUT_WR)`。
25. Manager 中任何可能同步嵌套的 Event/Pipeline dispatch 都不能在内层调用栈中析构正在执行的 ConnectionPair。
26. `dispatch_depth == 0` 才允许真正 CollectPendingClosed。
27. 两个 Direction 的 Latency/Jitter/sequence/runtime 完全独立，不能互相阻塞或共享随机序列状态。
28. 当前所有 EventLoop/TimerQueue/ConnectionManager 状态仍按单线程 Reactor 所有权使用，不宣称线程安全。

---

## 15. 阶段9为什么正确

可以把阶段9正确性压缩为五条：

```text
1. 时间正确性：
   Chunk不会早于Latency deadline继续；Reactor线程不sleep。

2. 版本正确性：
   PipelineSnapshot让旧Chunk继续旧Pipeline版本。

3. 顺序正确性：
   PipelineSequence + Direction Sequence Gate保证跨异步、跨配置更新仍按输入顺序提交。

4. 生命周期正确性：
   Timer只保存安全context/token；Connection关闭Cancel；generation拒绝stale callback；Manager延迟析构避免UAF。

5. 资源正确性：
   delayed/reorder/pending全部计入统一内存预算；high/low形成背压；hard limit限制异常增长；Timer batch budget避免callback storm。
```

因此阶段9不只是“实现一个延迟函数”，而是把真实异步故障策略接入了现有 Reactor / Pipeline / Connection 生命周期闭环。

---

## 16. 阶段状态

```text
阶段9理论主线：完成
Scheduler抽象：完成
SeededRandomSource：完成
Direction Runtime：完成
LatencyToxic：完成
Jitter：完成
release barrier：完成
PipelineSequence：完成
Sequence Gate：完成
跨Pipeline Replace保序：完成
delayed/reorder/pending统一预算：完成
high/low背压升级：完成
hard limit：完成
TimerId注册与关闭Cancel：完成
Cancel立即释放callback：完成
Timer RunExpired budget：完成
ConnectionManager dispatch_depth生命周期修复：完成
生产源码严格编译：通过
CMake/Ninja核心构建：通过
manual stage9 smoke：通过
ASan/UBSan smoke：通过
正式GTest/CTest：待用户仓库执行
```

---

## 17. 下一阶段入口

下一阶段：

```text
阶段10：Bandwidth 与 Slicer
```

阶段10直接复用阶段9：

```text
Scheduler
DirectionRuntimeState
Timer注册/取消
PipelineSnapshot
PipelineSequence / Sequence Gate
queued = pending + delayed + reorder
ConnectionToken{id,generation}
```

下一唯一工程问题：

```text
如何用 Token Bucket 按时间产生发送额度，
没有 token 时通过 Timer 等待而不 busy loop，
并让 Slicer 的 1→N 输出在保持字节顺序的同时真正形成可观察的限速发送。
```

阶段10开始时要特别重新评估阶段9 Sequence Gate 的“sequence complete 后整体提交”语义：
Bandwidth 需要当前最老 sequence 的 output 能够流式进入 pending，否则会出现最后一次性突发发送。这属于阶段10增量，不反向修改本文记录的 Stage9 完成状态。

---

## 18. 长期记忆摘要

```text
Stage9 = Latency/Jitter真正进入异步Pipeline。

Latency不是sleep：
Chunk + continuation → Timer → deadline恢复原PipelineSnapshot。

Scheduler隔离生产EventLoop与测试FakeScheduler。
Jitter使用固定seed有界均匀分布。

DirectionRuntime保存release barrier和随机源，跨Pipeline Replace存活。
release barrier减少Latency内部乱序；最终顺序由PipelineSequence + Sequence Gate兜底。

所有输入Chunk都分配sequence：
后序Chunk即使先完成也不能越过前序。

内存预算升级为：
queued = pending + delayed + reorder。
high暂停source，low恢复source，hard limit执行过载关闭。

Timer Cancel在Stage9改为立即释放callback capture；
TimerQueue RunExpired增加budget，避免timer storm长期占住Reactor。

ConnectionManager增加dispatch_depth，最外层dispatch返回后才真正析构pending_destroy连接，防止同步Pipeline回调导致self-destruction/UAF。

Stage9生产构建、严格编译、manual smoke、ASan/UBSan已通过；
正式GTest/CTest仍需在用户仓库运行。

下一阶段：Bandwidth + Slicer。
```
