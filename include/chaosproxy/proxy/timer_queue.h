#pragma once

#include "chaosproxy/proxy/clock.h"

#include <cstdint>
#include <functional>
#include <optional>
#include <queue>
#include <unordered_set>
#include <vector>

namespace chaosproxy {

struct TimerId {
    std::uint64_t value = 0;
    bool operator==(const TimerId&) const noexcept = default;
};

class TimerQueue {
public:
    TimerId Schedule(TimePoint deadline, std::function<void()> callback);
    bool Cancel(TimerId id);
    std::optional<TimePoint> NextDeadline() const;
    std::size_t RunExpired(TimePoint now, std::size_t callback_budget);
    std::size_t Size() const noexcept { return active_ids_.size(); }

private:
    struct Entry {
        TimePoint deadline;
        std::uint64_t sequence = 0;
        TimerId id;
        std::function<void()> callback;
    };
    struct Earlier {
        bool operator()(const Entry& left, const Entry& right) const noexcept {
            if (left.deadline != right.deadline) {
                return left.deadline > right.deadline;
            }
            return left.sequence > right.sequence;
        }
    };

    void DiscardCancelledTop() const;
    void MaybeCompact();

    mutable std::priority_queue<Entry, std::vector<Entry>, Earlier> heap_;
    std::unordered_set<std::uint64_t> active_ids_;
    mutable std::unordered_set<std::uint64_t> cancelled_ids_;
    std::uint64_t next_id_ = 1;
    std::uint64_t next_sequence_ = 1;
};

}  // namespace chaosproxy
