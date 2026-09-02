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
    if (id == 0 ||
        active_ids_.find(id) == active_ids_.end() ||
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
