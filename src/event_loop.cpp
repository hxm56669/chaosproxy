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

        if (result < 0 &&
            (errno == EAGAIN || errno == EWOULDBLOCK)) {
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

        auto nanoseconds =
            std::chrono::duration_cast<std::chrono::nanoseconds>(delay);
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
