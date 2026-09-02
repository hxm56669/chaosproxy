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
        bool operator()(
            const Timer& lhs,
            const Timer& rhs) const noexcept;
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
