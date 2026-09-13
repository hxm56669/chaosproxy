#include "chaosproxy/proxy/toxic_pipeline.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

namespace chaosproxy {
namespace {

class FakeClock final : public ClockSource {
public:
    TimePoint Now() const noexcept override { return now; }
    TimePoint now{};
};

Chunk MakeChunk(const std::string& text, std::shared_ptr<BufferBudget> budget,
                std::shared_ptr<const PolicySnapshot> policy) {
    auto block = BufferBlock::Allocate(budget, text.size());
    EXPECT_TRUE(block.ok());
    std::transform(text.begin(), text.end(), block.value()->bytes.begin(),
                   [](char value) { return static_cast<std::byte>(value); });
    return Chunk{std::move(block).value(), 0, text.size(), 0, 0, std::move(policy)};
}

TEST(TokenBucketTest, RefillIsExplicitAndNextEligibleUsesCurrentTokens) {
    TokenBucket bucket(100, 100);
    const TimePoint start{};
    bucket.Refill(start);
    bucket.Consume(100);
    EXPECT_EQ(bucket.Available(start + std::chrono::seconds(1)), 0U);
    bucket.Refill(start + std::chrono::milliseconds(250));
    EXPECT_EQ(bucket.Available(start + std::chrono::milliseconds(250)), 25U);
    EXPECT_EQ(bucket.NextEligibleTime(50, start + std::chrono::milliseconds(250)),
              start + std::chrono::milliseconds(500));
}

TEST(ToxicPipelineTest, LatencyResumesInOrderAndSlicerSharesViews) {
    auto budget = std::make_shared<BufferBudget>(1024);
    auto policy = std::make_shared<PolicySnapshot>();
    policy->version = 7;
    policy->stages = {{ToxicKind::kLatency, std::chrono::milliseconds(20)},
                      {ToxicKind::kSlicer, {}, 0, 0, 2}};
    TimerQueue timers;
    FakeClock clock;
    std::vector<std::string> pieces;
    DirectionContext context;
    context.clock = &clock;
    context.timers = &timers;
    context.emit = [&](const Chunk& chunk) {
        pieces.emplace_back(reinterpret_cast<const char*>(chunk.storage->bytes.data()) +
                                chunk.offset,
                            chunk.length);
        return Status::Ok();
    };
    ToxicPipeline pipeline;
    ASSERT_TRUE(pipeline.Accept(MakeChunk("abcd", budget, policy), context).ok());
    EXPECT_EQ(pipeline.PendingCount(), 1U);
    clock.now += std::chrono::milliseconds(20);
    EXPECT_EQ(timers.RunExpired(clock.now, 8), 1U);
    ASSERT_EQ(pieces.size(), 2U);
    EXPECT_EQ(pieces[0], "ab");
    EXPECT_EQ(pieces[1], "cd");
}

TEST(ToxicPipelineTest, LimitDataCountsSuccessfulEmitAndClosesAtBoundary) {
    auto budget = std::make_shared<BufferBudget>(1024);
    auto policy = std::make_shared<PolicySnapshot>();
    policy->stages = {{ToxicKind::kLimitData, {}, 0, 0, 0, 3}};
    bool closed = false;
    DirectionContext context;
    context.emit = [](const Chunk&) { return Status::Ok(); };
    context.close = [&] { closed = true; };
    ToxicPipeline pipeline;
    ASSERT_TRUE(pipeline.Accept(MakeChunk("abcd", budget, policy), context).ok());
    EXPECT_TRUE(closed);
    EXPECT_EQ(*context.successful_bytes, 3U);
}

TEST(ToxicPipelineTest, JitterAddsBoundedDelayWithoutReordering) {
    auto budget = std::make_shared<BufferBudget>(1024);
    auto policy = std::make_shared<PolicySnapshot>();
    policy->stages = {{ToxicKind::kJitter, std::chrono::milliseconds(10)}};
    TimerQueue timers;
    FakeClock clock;
    FixedRandomSource random(5'000'000);
    std::size_t emitted = 0;
    DirectionContext context;
    context.clock = &clock;
    context.timers = &timers;
    context.random = &random;
    context.emit = [&](const Chunk& chunk) {
        emitted += chunk.length;
        return Status::Ok();
    };
    ToxicPipeline pipeline;
    ASSERT_TRUE(pipeline.Accept(MakeChunk("x", budget, policy), context).ok());
    clock.now += std::chrono::milliseconds(14);
    EXPECT_EQ(timers.RunExpired(clock.now, 8), 0U);
    EXPECT_EQ(emitted, 0U);
    clock.now += std::chrono::milliseconds(1);
    EXPECT_EQ(timers.RunExpired(clock.now, 8), 1U);
    EXPECT_EQ(emitted, 1U);
}

TEST(ToxicPipelineTest, BandwidthPartiallyEmitsAndSchedulesRemainingBytes) {
    auto budget = std::make_shared<BufferBudget>(1024);
    auto policy = std::make_shared<PolicySnapshot>();
    policy->stages = {{ToxicKind::kBandwidth, {}, 100, 2}};
    TimerQueue timers;
    FakeClock clock;
    std::size_t emitted = 0;
    DirectionContext context;
    context.clock = &clock;
    context.timers = &timers;
    context.emit = [&](const Chunk& chunk) {
        emitted += chunk.length;
        return Status::Ok();
    };
    ToxicPipeline pipeline;
    ASSERT_TRUE(pipeline.Accept(MakeChunk("abcd", budget, policy), context).ok());
    EXPECT_EQ(emitted, 2U);
    for (int index = 0; index < 2; ++index) {
        clock.now += std::chrono::milliseconds(10);
        EXPECT_EQ(timers.RunExpired(clock.now, 8), 1U);
    }
    EXPECT_EQ(emitted, 4U);
    EXPECT_EQ(pipeline.PendingCount(), 0U);
}

TEST(ToxicPipelineTest, TimeoutClosesPendingOperationAtDeadline) {
    auto budget = std::make_shared<BufferBudget>(1024);
    auto policy = std::make_shared<PolicySnapshot>();
    policy->stages = {{ToxicKind::kTimeout, std::chrono::milliseconds(5)}};
    TimerQueue timers;
    FakeClock clock;
    bool closed = false;
    DirectionContext context;
    context.clock = &clock;
    context.timers = &timers;
    context.emit = [](const Chunk&) { return Status::Ok(); };
    context.close = [&] { closed = true; };
    ToxicPipeline pipeline;
    ASSERT_TRUE(pipeline.Accept(MakeChunk("x", budget, policy), context).ok());
    EXPECT_EQ(pipeline.PendingCount(), 1U);
    clock.now += std::chrono::milliseconds(5);
    EXPECT_EQ(timers.RunExpired(clock.now, 8), 1U);
    EXPECT_TRUE(closed);
    EXPECT_EQ(pipeline.PendingCount(), 0U);
}

}  // namespace
}  // namespace chaosproxy
