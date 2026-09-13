#include "chaosproxy/proxy/event_loop.h"
#include "chaosproxy/proxy/timer_queue.h"

#include <gtest/gtest.h>

#include <chrono>

namespace chaosproxy {
namespace {

TEST(TimerQueueTest, OrdersByDeadlineAndSequenceWithBudget) {
    TimerQueue timers;
    const TimePoint start{};
    std::vector<int> fired;
    timers.Schedule(start + std::chrono::seconds(2), [&] { fired.push_back(2); });
    timers.Schedule(start + std::chrono::seconds(1), [&] { fired.push_back(1); });
    timers.Schedule(start + std::chrono::seconds(1), [&] { fired.push_back(3); });

    EXPECT_EQ(timers.RunExpired(start + std::chrono::seconds(2), 2), 2U);
    ASSERT_EQ(fired.size(), 2U);
    EXPECT_EQ(fired[0], 1);
    EXPECT_EQ(fired[1], 3);
    EXPECT_EQ(timers.Size(), 1U);
    EXPECT_EQ(timers.RunExpired(start + std::chrono::seconds(2), 2), 1U);
    ASSERT_EQ(fired.size(), 3U);
    EXPECT_EQ(fired[2], 2);
}

TEST(TimerQueueTest, CancelIsLazyAndDoesNotInvokeCallback) {
    TimerQueue timers;
    const TimePoint start{};
    bool fired = false;
    const TimerId id = timers.Schedule(start + std::chrono::seconds(1),
                                       [&] { fired = true; });
    ASSERT_TRUE(timers.Cancel(id));
    EXPECT_FALSE(timers.Cancel(id));
    EXPECT_FALSE(timers.NextDeadline().has_value());
    EXPECT_EQ(timers.RunExpired(start + std::chrono::seconds(2), 8), 0U);
    EXPECT_FALSE(fired);
}

TEST(EventLoopTimerTest, TimerFdWakesLoopAndRunsExpiredCallback) {
    TimerQueue timers;
    EventLoop loop;
    ASSERT_TRUE(loop.AttachTimerQueue(&timers).ok());
    bool fired = false;
    timers.Schedule(Clock::now() + std::chrono::milliseconds(10),
                    [&] { fired = true; loop.RequestStop(); });
    loop.RunOnce(1000);
    EXPECT_TRUE(fired);
}

}  // namespace
}  // namespace chaosproxy
