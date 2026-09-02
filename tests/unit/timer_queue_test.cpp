#include "chaosproxy/timer_queue.h"

#include <gtest/gtest.h>

#include <chrono>
#include <vector>

namespace {

using namespace chaosproxy;
using namespace std::chrono_literals;

TEST(TimerQueueTest, DoesNotRunBeforeDeadlineAndRunsAtDeadline) {
    TimerQueue queue;
    const TimePoint t0{};
    bool fired = false;

    (void)queue.Schedule(t0 + 5s, [&] { fired = true; });

    EXPECT_EQ(queue.RunExpired(t0 + 4999ms), 0U);
    EXPECT_FALSE(fired);

    EXPECT_EQ(queue.RunExpired(t0 + 5s), 1U);
    EXPECT_TRUE(fired);
    EXPECT_TRUE(queue.Empty());
}

TEST(TimerQueueTest, RunsInDeadlineOrderAndSkipsCancelledTimer) {
    TimerQueue queue;
    const TimePoint t0{};
    std::vector<int> order;

    const TimerId cancelled = queue.Schedule(
        t0 + 200ms,
        [&] { order.push_back(2); });

    (void)queue.Schedule(
        t0 + 300ms,
        [&] { order.push_back(3); });

    (void)queue.Schedule(
        t0 + 100ms,
        [&] { order.push_back(1); });

    ASSERT_TRUE(queue.Cancel(cancelled));
    EXPECT_FALSE(queue.Cancel(cancelled));

    EXPECT_EQ(queue.RunExpired(t0 + 1s), 2U);
    EXPECT_EQ(order, (std::vector<int>{1, 3}));
    EXPECT_TRUE(queue.Empty());
}

TEST(TimerQueueTest, NextDeadlineIgnoresCancelledHeapTop) {
    TimerQueue queue;
    const TimePoint t0{};

    const TimerId first = queue.Schedule(t0 + 100ms, [] {});
    (void)queue.Schedule(t0 + 500ms, [] {});

    ASSERT_TRUE(queue.Cancel(first));

    const auto next = queue.NextDeadline();
    ASSERT_TRUE(next.has_value());
    EXPECT_EQ(*next, t0 + 500ms);
}

TEST(TimerQueueTest, SameDeadlineUsesRegistrationOrder) {
    TimerQueue queue;
    const TimePoint deadline = TimePoint{} + 100ms;
    std::vector<int> order;

    (void)queue.Schedule(deadline, [&] { order.push_back(1); });
    (void)queue.Schedule(deadline, [&] { order.push_back(2); });
    (void)queue.Schedule(deadline, [&] { order.push_back(3); });

    EXPECT_EQ(queue.RunExpired(deadline), 3U);
    EXPECT_EQ(order, (std::vector<int>{1, 2, 3}));
}

TEST(TimerQueueTest, CallbackCanScheduleAnotherAlreadyExpiredTimer) {
    TimerQueue queue;
    const TimePoint t0{};
    std::vector<int> order;

    (void)queue.Schedule(t0 + 100ms, [&] {
        order.push_back(1);
        (void)queue.Schedule(t0 + 50ms, [&] {
            order.push_back(2);
        });
    });

    EXPECT_EQ(queue.RunExpired(t0 + 100ms), 2U);
    EXPECT_EQ(order, (std::vector<int>{1, 2}));
}

TEST(TimerQueueTest, RejectsEmptyCallbackAndUnknownCancellation) {
    TimerQueue queue;

    EXPECT_THROW(
        (void)queue.Schedule(TimePoint{}, TimerCallback{}),
        std::invalid_argument);
    EXPECT_FALSE(queue.Cancel(0));
    EXPECT_FALSE(queue.Cancel(42));
}

}  // namespace
