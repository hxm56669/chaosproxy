#include "chaosproxy/event_loop.h"

#include <gtest/gtest.h>

#include <chrono>

namespace {

using namespace chaosproxy;
using namespace std::chrono_literals;

TEST(EventLoopTimerTest, TimerFdWakesEventLoopAndRunsCallback) {
    EventLoop loop;
    bool fired = false;

    (void)loop.ScheduleAfter(5ms, [&] { fired = true; });

    EXPECT_GE(loop.RunOnce(1000), 1);
    EXPECT_TRUE(fired);
}

TEST(EventLoopTimerTest, CancelledTimerDoesNotRun) {
    EventLoop loop;
    bool fired = false;

    const TimerId id = loop.ScheduleAfter(5ms, [&] { fired = true; });
    ASSERT_TRUE(loop.CancelTimer(id));

    EXPECT_EQ(loop.RunOnce(20), 0);
    EXPECT_FALSE(fired);
}

TEST(EventLoopTimerTest, NegativeDelayRunsWithoutBlocking) {
    EventLoop loop;
    bool fired = false;

    (void)loop.ScheduleAfter(-1ms, [&] { fired = true; });

    EXPECT_GE(loop.RunOnce(100), 1);
    EXPECT_TRUE(fired);
}

}  // namespace
