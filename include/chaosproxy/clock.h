#pragma once

#include <chrono>

namespace chaosproxy {

using MonotonicClock = std::chrono::steady_clock;
using TimePoint = MonotonicClock::time_point;
using Duration = MonotonicClock::duration;

}  // namespace chaosproxy
