#include "chaosproxy/proxy/timer_queue.h"

#include <utility>

namespace chaosproxy {

TimerId TimerQueue::Schedule(TimePoint deadline, std::function<void()> callback) {
    if (next_id_ == 0 || next_sequence_ == 0 || !callback) {
        return {};
    }
    const TimerId id{next_id_++};
    heap_.push(Entry{deadline, next_sequence_++, id, std::move(callback)});
    active_ids_.insert(id.value);
    return id;
}

bool TimerQueue::Cancel(TimerId id) {
    if (id.value == 0 || !active_ids_.erase(id.value)) {
        return false;
    }
    cancelled_ids_.insert(id.value);
    MaybeCompact();
    return true;
}

void TimerQueue::DiscardCancelledTop() const {
    while (!heap_.empty() && cancelled_ids_.contains(heap_.top().id.value)) {
        cancelled_ids_.erase(heap_.top().id.value);
        heap_.pop();
    }
}

std::optional<TimePoint> TimerQueue::NextDeadline() const {
    DiscardCancelledTop();
    if (heap_.empty()) {
        return std::nullopt;
    }
    return heap_.top().deadline;
}

std::size_t TimerQueue::RunExpired(TimePoint now, std::size_t callback_budget) {
    std::size_t ran = 0;
    while (ran < callback_budget) {
        DiscardCancelledTop();
        if (heap_.empty() || heap_.top().deadline > now) {
            break;
        }
        Entry entry = heap_.top();
        heap_.pop();
        active_ids_.erase(entry.id.value);
        entry.callback();
        ++ran;
    }
    MaybeCompact();
    return ran;
}

void TimerQueue::MaybeCompact() {
    if (cancelled_ids_.size() < 32 ||
        cancelled_ids_.size() * 2 < heap_.size()) {
        return;
    }
    std::vector<Entry> retained;
    retained.reserve(active_ids_.size());
    while (!heap_.empty()) {
        Entry entry = heap_.top();
        heap_.pop();
        if (!cancelled_ids_.contains(entry.id.value)) {
            retained.push_back(std::move(entry));
        }
    }
    cancelled_ids_.clear();
    for (Entry& entry : retained) {
        heap_.push(std::move(entry));
    }
}

}  // namespace chaosproxy
