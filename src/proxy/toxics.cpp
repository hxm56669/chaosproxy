#include "chaosproxy/proxy/toxics.h"

#include <algorithm>
#include <cmath>
#include <limits>

namespace chaosproxy {

std::size_t FixedRandomSource::Uniform(std::size_t inclusive_max) {
    return std::min(value_, inclusive_max);
}

TokenBucket::TokenBucket(std::size_t rate_bytes_per_second,
                         std::size_t capacity) {
    Configure(rate_bytes_per_second, capacity);
}

void TokenBucket::Configure(std::size_t rate_bytes_per_second,
                            std::size_t capacity) {
    rate_ = rate_bytes_per_second;
    capacity_ = capacity;
    tokens_ = static_cast<double>(capacity);
    last_refill_ = TimePoint{};
    initialized_ = false;
}

void TokenBucket::Refill(TimePoint now) {
    if (!initialized_) {
        last_refill_ = now;
        initialized_ = true;
        return;
    }
    if (now <= last_refill_ || rate_ == 0 || capacity_ == 0) {
        last_refill_ = std::max(last_refill_, now);
        return;
    }
    const auto elapsed = std::chrono::duration<double>(now - last_refill_).count();
    tokens_ = std::min(static_cast<double>(capacity_),
                       tokens_ + elapsed * static_cast<double>(rate_));
    last_refill_ = now;
}

std::size_t TokenBucket::Available(TimePoint now) const {
    static_cast<void>(now);
    if (tokens_ <= 0.0) {
        return 0;
    }
    return static_cast<std::size_t>(std::min(
        tokens_, static_cast<double>(std::numeric_limits<std::size_t>::max())));
}

void TokenBucket::Consume(std::size_t actual_bytes) {
    tokens_ = std::max(0.0, tokens_ - static_cast<double>(actual_bytes));
}

TimePoint TokenBucket::NextEligibleTime(std::size_t bytes, TimePoint now) const {
    if (bytes == 0 || static_cast<double>(bytes) <= tokens_) {
        return now;
    }
    if (rate_ == 0) {
        return TimePoint::max();
    }
    const double seconds =
        (static_cast<double>(bytes) - tokens_) / static_cast<double>(rate_);
    const auto nanos = static_cast<std::int64_t>(std::ceil(seconds * 1e9));
    return now + std::chrono::nanoseconds(std::max<std::int64_t>(1, nanos));
}

}  // namespace chaosproxy
