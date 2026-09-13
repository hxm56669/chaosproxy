#pragma once

#include <chrono>

namespace chaosproxy {

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
using Duration = Clock::duration;

class ClockSource {
public:
    virtual ~ClockSource() = default;
    virtual TimePoint Now() const noexcept = 0;
};

class SteadyClockSource final : public ClockSource {
public:
    TimePoint Now() const noexcept override { return Clock::now(); }
};

}  // namespace chaosproxy
