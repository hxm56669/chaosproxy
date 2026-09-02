#include "chaosproxy/event_loop.h"
#include "chaosproxy/socket_ops.h"
#include "helpers/test_socket_utils.h"

#include <gtest/gtest.h>
#include <sys/epoll.h>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;

TEST(EventLoopTest, DispatchesReadinessByToken) {
    auto pair = MakeSocketPair();
    ASSERT_TRUE(pair.first.IsValid());

    EventLoop loop;
    EventToken observed_token = 0;
    std::uint32_t observed_events = 0;

    const EventToken token = loop.Add(
        pair.first.Get(),
        EPOLLIN,
        [&](EventToken current, std::uint32_t events) {
            observed_token = current;
            observed_events = events;
        });

    const char byte = 'x';
    ASSERT_EQ(
        TryWrite(pair.second.Get(), &byte, 1).status,
        WriteStatus::kWritten);

    ASSERT_GT(loop.RunOnce(100), 0);
    EXPECT_EQ(observed_token, token);
    EXPECT_NE(observed_events & EPOLLIN, 0U);
    EXPECT_TRUE(loop.Contains(token));
    EXPECT_EQ(loop.Remove(token), 0);
    EXPECT_FALSE(loop.Contains(token));
}

TEST(EventLoopTest, IgnoresStaleTokenAfterRemoval) {
    auto first = MakeSocketPair();
    auto second = MakeSocketPair();
    EventLoop loop;
    int first_calls = 0;
    int second_calls = 0;

    const EventToken old_token = loop.Add(
        first.first.Get(),
        EPOLLIN,
        [&](EventToken, std::uint32_t) { ++first_calls; });
    EXPECT_EQ(loop.Remove(old_token), 0);

    const EventToken new_token = loop.Add(
        second.first.Get(),
        EPOLLIN,
        [&](EventToken, std::uint32_t) { ++second_calls; });
    EXPECT_NE(old_token, new_token);

    const char byte = 'y';
    ASSERT_EQ(
        TryWrite(second.second.Get(), &byte, 1).status,
        WriteStatus::kWritten);
    ASSERT_GT(loop.RunOnce(100), 0);

    EXPECT_EQ(first_calls, 0);
    EXPECT_EQ(second_calls, 1);
}

}  // namespace
