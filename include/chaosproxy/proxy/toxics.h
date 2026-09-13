#pragma once

#include "chaosproxy/proxy/clock.h"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chaosproxy {

class RandomSource {
public:
    virtual ~RandomSource() = default;
    virtual std::size_t Uniform(std::size_t inclusive_max) = 0;
};

class FixedRandomSource final : public RandomSource {
public:
    explicit FixedRandomSource(std::size_t value) : value_(value) {}
    std::size_t Uniform(std::size_t inclusive_max) override;

private:
    std::size_t value_;
};

class TokenBucket {
public:
    TokenBucket() = default;
    TokenBucket(std::size_t rate_bytes_per_second, std::size_t capacity);

    void Configure(std::size_t rate_bytes_per_second, std::size_t capacity);
    void Refill(TimePoint now);
    std::size_t Available(TimePoint now) const;
    void Consume(std::size_t actual_bytes);
    TimePoint NextEligibleTime(std::size_t bytes, TimePoint now) const;

private:
    std::size_t rate_ = 0;
    std::size_t capacity_ = 0;
    double tokens_ = 0.0;
    TimePoint last_refill_{};
    bool initialized_ = false;
};

enum class ToxicKind {
    kLatency,
    kJitter,
    kBandwidth,
    kSlicer,
    kPause,
    kTimeout,
    kClose,
    kReset,
    kLimitData,
};

struct ToxicRule {
    ToxicKind kind = ToxicKind::kLatency;
    Duration duration{};
    std::size_t rate_bytes_per_second = 0;
    std::size_t capacity = 0;
    std::size_t slice_bytes = 0;
    std::size_t limit_bytes = 0;
    bool abort = false;
};

struct PolicySnapshot {
    std::uint64_t version = 0;
    std::vector<ToxicRule> stages;
};

}  // namespace chaosproxy
