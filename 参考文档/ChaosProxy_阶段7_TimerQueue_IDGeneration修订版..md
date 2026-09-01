# ChaosProxy 阶段7：TimerQueue 与 timerfd 工程长期记忆

> 文档定位：本文建立在阶段4～6工程基线之上，只记录阶段7新增/修改内容。阶段2～6已有 `UniqueFd`、`EventLoop`、`Listener`、`ConnectionPair`、`ConnectionManager`、固定容量 Buffer、背压和关闭语义不重写。
> **2026-08-25 架构修订：** 阶段4～6连接身份统一为 `ConnectionToken{ConnectionId,generation}`。阶段7不再使用 `weak_ptr<ConnectionPair>` 作为连接级 Timer 方案；Timer 保存 ConnectionToken，到期后通过 ConnectionManager 重新解析并校验 generation。


## 1. 当前项目位置

```text
项目：ChaosProxy
完成层级：L1 故障注入基础设施（开始）
前置工程：M3核心实现已具备
当前阶段：阶段7——定时器基础设施
本轮核心产出：steady_clock 时间类型 + TimerQueue 最小堆 + TimerId/Cancel + timerfd 接入 EventLoop + 确定性 Timer 单测
下一阶段：阶段8 ToxicPipeline
```

阶段7唯一解决的问题：

```text
如何让单线程 EventLoop 在未来某个 deadline 被唤醒执行任务，
同时不使用 sleep 阻塞 Reactor，并保证长期回调不会强行延长 ConnectionPair 生命周期。
```

## 2. 本轮工程决定

1. `TimerQueue` 是纯逻辑容器，内部使用最小堆；不拥有 EventLoop。
2. `EventLoop` 拥有一个 `timerfd`，只把“最近 deadline”映射到这个 fd。
3. 所有逻辑 Timer 共用一个 `timerfd`，不是一个 Timer 一个 fd。
4. TimerQueue 核心接口操作绝对 `TimePoint`，单元测试直接传入逻辑时间，不 `sleep_for`。
5. 阶段4～6已修订为 `ConnectionManager -> unique_ptr<ConnectionPair>` 与 `ConnectionToken{id,generation}` 身份模型；未来连接级 Timer 只保存 ConnectionToken，到期后由 Manager 校验后再借用 ConnectionPair。
6. `Schedule/Cancel` 与 EventLoop 一样按当前单线程 Reactor 约束使用；阶段7不引入跨线程加锁和 eventfd 命令队列。
7. 相同 deadline 使用递增 TimerId 作为第二排序键，使执行顺序确定。

## 3. 新增/修改目录

```text
include/chaosproxy/
├── clock.h                 # 新增
├── timer_queue.h           # 新增
└── event_loop.h            # 修改：增加 Timer API 与 timerfd 成员

src/
├── timer_queue.cpp         # 新增
└── event_loop.cpp          # 修改：timerfd 创建、读取、rearm

tests/unit/
├── timer_queue_test.cpp    # 新增：纯逻辑确定性测试
├── event_loop_timer_test.cpp # 新增：timerfd 与 EventLoop 集成测试
└── ../integration/connection_generation_test.cpp # 新增：旧Timer不能命中新generation
```

## 4. 运行路径

```text
EventLoop::ScheduleAfter/At
→ TimerQueue::Schedule
→ TimerQueue::NextDeadline
→ EventLoop::RearmTimerFd
→ timerfd_settime
→ epoll_wait
→ timerfd EPOLLIN
→ read(timerfd)
→ TimerQueue::RunExpired(steady_clock::now())
→ callback
→ 根据新的 NextDeadline 再次 rearm
```

取消路径：

```text
CancelTimer(id)
→ TimerQueue::Cancel(id)
→ 惰性标记 cancelled
→ NextDeadline 清理已取消堆顶
→ 重新设置 timerfd
```

## 5. Timer 与 ConnectionPair 生命周期（ID + generation 修订版）

阶段4～6现在统一采用：

```text
ConnectionManager
→ unique_ptr<ConnectionPair> 唯一拥有

长期异步身份
→ ConnectionToken { id, generation }
```

因此阶段7的连接级 Timer 不保存裸 `ConnectionPair*`，也不保存 `shared_ptr/weak_ptr`。正确形状是：

```cpp
const ConnectionToken token = connection.Token();

TimerId timer_id = loop.ScheduleAfter(
    timeout,
    [&manager, token] {
        if (ConnectionPair* connection = manager.Find(token)) {
            connection->Close(CloseReason::kLocalStop);
        }
    });
```

`Find(token)` 必须同时检查：

```text
slot id 是否存在
+
slot 当前 generation 是否等于 token.generation
```

例如：

```text
旧连接 A = {id=7, generation=3}
A关闭
新连接 B复用id=7 = {id=7, generation=4}

旧Timer携带 {7,3}
→ manager.Find({7,3})
→ generation不匹配
→ nullptr
→ 旧Timer丢弃
```

### Cancel 与 generation 不是一回事

正常生命周期仍要保存 `TimerId` 并在任务失效时调用：

```cpp
loop.CancelTimer(timer_id);
```

两者职责：

```text
Cancel(timer_id)
= 这个任务逻辑上已经不需要

generation
= 即使旧任务漏取消/晚到，也不能误操作复用同一id的新连接
```

所以 generation 是身份安全兜底，不能替代 Cancel。

### Manager 生命周期边界

上面的 Timer wrapper 会调用 Manager，因此拥有该 Timer 的模块必须在 Manager 销毁前取消 Timer。当前单线程工程中 `ConnectionManager::~ConnectionManager()` 会先关闭所有连接并注销它们的 EventToken；后续阶段9开始真正把 Connection Timer 接入业务时，具体 Toxic/Direction 还要保存自己的 TimerId 并在关闭/配置失效时取消。

## 6. CMake 增量

`chaosproxy_core` 增加：

```cmake
src/timer_queue.cpp
```

`chaosproxy_tests` 增加：

```cmake
tests/unit/timer_queue_test.cpp
tests/unit/event_loop_timer_test.cpp
tests/integration/connection_generation_test.cpp
```

完整修改可直接查看同目录交付的 `stage7_existing_files.patch`。

## 7. 完整阶段7代码

### `include/chaosproxy/clock.h`

```cpp
#pragma once

#include <chrono>

namespace chaosproxy {

using MonotonicClock = std::chrono::steady_clock;
using TimePoint = MonotonicClock::time_point;
using Duration = MonotonicClock::duration;

}  // namespace chaosproxy
```

### `include/chaosproxy/timer_queue.h`

```cpp
#pragma once

#include "chaosproxy/clock.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <queue>
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

    [[nodiscard]] bool Cancel(TimerId id) noexcept;

    [[nodiscard]] std::optional<TimePoint> NextDeadline();

    std::size_t RunExpired(TimePoint now);

    [[nodiscard]] std::size_t Size() const noexcept;
    [[nodiscard]] bool Empty() const noexcept;

private:
    struct Timer {
        TimePoint deadline;
        TimerId id;
        TimerCallback callback;
    };

    struct TimerCompare {
        bool operator()(const Timer& lhs, const Timer& rhs) const noexcept;
    };

    [[nodiscard]] TimerId NextId();
    void PurgeCancelledTop() noexcept;

    std::priority_queue<
        Timer,
        std::vector<Timer>,
        TimerCompare> timers_;

    std::unordered_set<TimerId> active_ids_;
    std::unordered_set<TimerId> cancelled_ids_;
    TimerId next_id_{1};
};

}  // namespace chaosproxy
```

### `src/timer_queue.cpp`

```cpp
#include "chaosproxy/timer_queue.h"

#include <stdexcept>
#include <utility>

namespace chaosproxy {

bool TimerQueue::TimerCompare::operator()(
    const Timer& lhs,
    const Timer& rhs) const noexcept {
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
    active_ids_.insert(id);
    timers_.push(Timer{deadline, id, std::move(callback)});
    return id;
}

bool TimerQueue::Cancel(TimerId id) noexcept {
    if (id == 0 || active_ids_.find(id) == active_ids_.end() ||
        cancelled_ids_.find(id) != cancelled_ids_.end()) {
        return false;
    }

    cancelled_ids_.insert(id);
    return true;
}

std::optional<TimePoint> TimerQueue::NextDeadline() {
    PurgeCancelledTop();

    if (timers_.empty()) {
        return std::nullopt;
    }

    return timers_.top().deadline;
}

std::size_t TimerQueue::RunExpired(TimePoint now) {
    std::size_t executed = 0;

    for (;;) {
        PurgeCancelledTop();

        if (timers_.empty() || timers_.top().deadline > now) {
            break;
        }

        Timer timer = timers_.top();
        timers_.pop();
        active_ids_.erase(timer.id);

        timer.callback();
        ++executed;
    }

    return executed;
}

std::size_t TimerQueue::Size() const noexcept {
    return active_ids_.size() - cancelled_ids_.size();
}

bool TimerQueue::Empty() const noexcept {
    return Size() == 0;
}

TimerId TimerQueue::NextId() {
    for (;;) {
        const TimerId candidate = next_id_++;
        if (candidate != 0 &&
            active_ids_.find(candidate) == active_ids_.end()) {
            return candidate;
        }
    }
}

void TimerQueue::PurgeCancelledTop() noexcept {
    while (!timers_.empty()) {
        const TimerId id = timers_.top().id;
        const auto cancelled = cancelled_ids_.find(id);
        if (cancelled == cancelled_ids_.end()) {
            break;
        }

        timers_.pop();
        cancelled_ids_.erase(cancelled);
        active_ids_.erase(id);
    }
}

}  // namespace chaosproxy
```

### `include/chaosproxy/event_loop.h`

```cpp
#pragma once

#include "chaosproxy/clock.h"
#include "chaosproxy/timer_queue.h"
#include "chaosproxy/unique_fd.h"

#include <sys/epoll.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <vector>

namespace chaosproxy {

using EventToken = std::uint64_t;
using EventCallback = std::function<void(EventToken, std::uint32_t)>;

class EventLoop final {
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

    [[nodiscard]] TimerId ScheduleAt(
        TimePoint deadline,
        TimerCallback callback);

    [[nodiscard]] TimerId ScheduleAfter(
        Duration delay,
        TimerCallback callback);

    [[nodiscard]] bool CancelTimer(TimerId id);

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

### `src/event_loop.cpp`

```cpp
#include "chaosproxy/event_loop.h"

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
        MonotonicClock::now() + delay,
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
    (void)timer_queue_.RunExpired(MonotonicClock::now());
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
        Duration delay = *next_deadline - MonotonicClock::now();
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

### `tests/unit/timer_queue_test.cpp`

```cpp
#include "chaosproxy/timer_queue.h"

#include <gtest/gtest.h>

#include <chrono>
#include <vector>

namespace {

using namespace chaosproxy;
using namespace std::chrono_literals;

TEST(TimerQueueTest, DoesNotRunBeforeDeadlineAndRunsAtDeadline) {
    TimerQueue queue;
    const TimePoint t0{};
    bool fired = false;

    (void)queue.Schedule(t0 + 5s, [&] { fired = true; });

    EXPECT_EQ(queue.RunExpired(t0 + 4999ms), 0U);
    EXPECT_FALSE(fired);

    EXPECT_EQ(queue.RunExpired(t0 + 5s), 1U);
    EXPECT_TRUE(fired);
    EXPECT_TRUE(queue.Empty());
}

TEST(TimerQueueTest, RunsInDeadlineOrderAndSkipsCancelledTimer) {
    TimerQueue queue;
    const TimePoint t0{};
    std::vector<int> order;

    const TimerId cancelled = queue.Schedule(
        t0 + 200ms,
        [&] { order.push_back(2); });

    (void)queue.Schedule(
        t0 + 300ms,
        [&] { order.push_back(3); });

    (void)queue.Schedule(
        t0 + 100ms,
        [&] { order.push_back(1); });

    ASSERT_TRUE(queue.Cancel(cancelled));
    EXPECT_FALSE(queue.Cancel(cancelled));

    EXPECT_EQ(queue.RunExpired(t0 + 1s), 2U);
    EXPECT_EQ(order, (std::vector<int>{1, 3}));
    EXPECT_TRUE(queue.Empty());
}

TEST(TimerQueueTest, NextDeadlineIgnoresCancelledHeapTop) {
    TimerQueue queue;
    const TimePoint t0{};

    const TimerId first = queue.Schedule(t0 + 100ms, [] {});
    (void)queue.Schedule(t0 + 500ms, [] {});

    ASSERT_TRUE(queue.Cancel(first));

    const auto next = queue.NextDeadline();
    ASSERT_TRUE(next.has_value());
    EXPECT_EQ(*next, t0 + 500ms);
}

TEST(TimerQueueTest, SameDeadlineUsesRegistrationOrder) {
    TimerQueue queue;
    const TimePoint deadline = TimePoint{} + 100ms;
    std::vector<int> order;

    (void)queue.Schedule(deadline, [&] { order.push_back(1); });
    (void)queue.Schedule(deadline, [&] { order.push_back(2); });
    (void)queue.Schedule(deadline, [&] { order.push_back(3); });

    EXPECT_EQ(queue.RunExpired(deadline), 3U);
    EXPECT_EQ(order, (std::vector<int>{1, 2, 3}));
}

TEST(TimerQueueTest, CallbackCanScheduleAnotherAlreadyExpiredTimer) {
    TimerQueue queue;
    const TimePoint t0{};
    std::vector<int> order;

    (void)queue.Schedule(t0 + 100ms, [&] {
        order.push_back(1);
        (void)queue.Schedule(t0 + 50ms, [&] {
            order.push_back(2);
        });
    });

    EXPECT_EQ(queue.RunExpired(t0 + 100ms), 2U);
    EXPECT_EQ(order, (std::vector<int>{1, 2}));
}

}  // namespace
```

### `tests/unit/event_loop_timer_test.cpp`

```cpp
#include "chaosproxy/event_loop.h"

#include <gtest/gtest.h>

#include <chrono>

namespace {

using namespace chaosproxy;
using namespace std::chrono_literals;

TEST(EventLoopTimerTest, TimerFdWakesEventLoopAndRunsCallback) {
    EventLoop loop;
    bool fired = false;

    (void)loop.ScheduleAfter(5ms, [&] { fired = true; });

    EXPECT_GE(loop.RunOnce(1000), 1);
    EXPECT_TRUE(fired);
}

TEST(EventLoopTimerTest, CancelledTimerDoesNotRun) {
    EventLoop loop;
    bool fired = false;

    const TimerId id = loop.ScheduleAfter(5ms, [&] { fired = true; });
    ASSERT_TRUE(loop.CancelTimer(id));

    EXPECT_EQ(loop.RunOnce(20), 0);
    EXPECT_FALSE(fired);
}

}  // namespace
```

### `tests/integration/connection_generation_test.cpp`

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

## 8. 正式测试目标

### TimerQueue 纯逻辑

```text
[x] deadline 未到不执行
[x] deadline 到达执行
[x] 多 Timer 按 deadline 顺序执行
[x] Cancel 后不执行
[x] 最早 Timer 被取消后 NextDeadline 跳到下一个有效 Timer
[x] 相同 deadline 按注册顺序执行
[x] callback 中新增已经到期 Timer 可在同一 RunExpired 中继续执行
```

### EventLoop + timerfd

```text
[x] timerfd 能唤醒 RunOnce
[x] CancelTimer 后 callback 不执行
```

### ConnectionToken + generation

```text
[x] ConnectionId关闭后可以被新连接复用
[x] 同一id复用时generation递增
[x] manager.Find(old_token)返回nullptr
[x] 旧Timer携带old_token时不能关闭新generation连接
```

## 9. 当前已执行验证

本轮在阶段2～6工程基线上重新切换到 ID+generation 后执行：

```text
1. 阶段4～7生产源码：
   g++ -std=c++17 -Wall -Wextra -Wpedantic -Werror
   → 通过

2. CMake + Ninja + BUILD_TESTING=OFF：
   → chaosproxy_core 构建成功
   → chaosproxy 链接成功

3. ID+generation manual smoke：
   → 连接A关闭后ConnectionId被B复用
   → B的generation与A不同
   → manager.Find(A旧token)==nullptr
   → 旧Timer携带A token运行后无法Close B
   → 通过

4. AddressSanitizer + UndefinedBehaviorSanitizer：
   → 上述smoke通过（LeakSanitizer在当前环境关闭）

5. 更新后的阶段4～7 GTest源码：
   → 使用最小GTest stub做C++语法检查通过
```

当前环境没有正式 GoogleTest 头文件/库，因此不能写成 CTest 已通过。用户仓库仍应执行：

```bash
cmake -S . -B build-debug \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DBUILD_TESTING=ON

cmake --build build-debug -j
ctest --test-dir build-debug --output-on-failure
```

只跑阶段7与generation回归：

```bash
./build-debug/chaosproxy_tests \
  --gtest_filter='TimerQueueTest.*:EventLoopTimerTest.*:ConnectionGenerationTest.*'
```

## 10. 阶段7不变量（ID + generation 修订版）

1. Reactor 线程中禁止用 `sleep` 实现延迟。
2. TimerQueue 只管理逻辑 Timer；一个 EventLoop 只需要一个 timerfd。
3. TimerQueue 使用 `steady_clock` 时间类型，不使用 `system_clock` 计算相对超时。
4. Cancel 是逻辑取消，允许堆中暂存已取消节点，由惰性删除清理。
5. Cancelled Timer 的 callback 永远不能执行。
6. `RunExpired(now)` 执行全部 `deadline <= now` 的有效任务。
7. 新的最早 deadline 出现或最早 Timer 被取消后，timerfd 必须重新 arm。
8. timerfd EPOLLIN 后先 read 消费就绪，再运行到期任务，再安排下一次唤醒。
9. Timer callback 仍运行在 EventLoop 线程，不能阻塞或做长计算。
10. 连接级 Timer 只保存 `ConnectionToken{id,generation}`，不能长期保存裸 `ConnectionPair*`。
11. Timer 到期后必须通过 `ConnectionManager::Find(token)` 或 Manager 的 token-aware API 重新解析对象。
12. `ConnectionId` 可以复用，generation 不匹配时旧 Timer 必须无条件失效。
13. TimerId/Cancel 负责取消不再需要的任务；generation 负责 stale callback 身份安全，两者不能替代。
14. EventLoop 的 `EventToken` 防 fd/epoll注册复用；ConnectionToken 防 Connection slot 复用，两层身份必须同时保留。
15. 当前 EventLoop/TimerQueue/ConnectionManager 均按单线程 Reactor 约束使用，不宣称线程安全。

## 11. 阶段状态

```text
阶段4～6连接身份模型：已修订为 ConnectionId + generation
shared_ptr/weak_ptr连接身份方案：作废
阶段7理论：完成主线学习
阶段7 TimerQueue/timerfd生产实现：完成
阶段7 ID+generation连接Timer安全模型：完成
严格编译：通过
CMake/Ninja核心构建：通过
ID复用 + stale timer smoke：通过
ASan/UBSan smoke：通过
正式GTest/CTest：待用户仓库运行
阶段7综合工程验收：仍待正式CTest质量门
下一阶段入口：阶段8 ToxicPipeline
```

