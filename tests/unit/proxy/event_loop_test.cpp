#include "chaosproxy/proxy/event_loop.h"

#include <gtest/gtest.h>

namespace chaosproxy {
namespace {

TEST(ReadyQueueTest, DeduplicatesByConnectionGenerationAndDirection) {
    EventLoop loop;
    const ConnectionToken connection{3, 7};
    loop.EnqueueReady(connection, DirectionId::kClientToUpstream,
                      WakeReason::kReadable);
    loop.EnqueueReady(connection, DirectionId::kClientToUpstream,
                      WakeReason::kWritable);
    loop.EnqueueReady(connection, DirectionId::kUpstreamToClient,
                      WakeReason::kReadable);
    EXPECT_EQ(loop.ReadyCount(), 2U);

    const auto first = loop.PopReady();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->connection.slot, 3U);
    EXPECT_EQ(first->direction, DirectionId::kClientToUpstream);
    EXPECT_EQ(first->reason, WakeReason::kReadable);

    const auto second = loop.PopReady();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->direction, DirectionId::kUpstreamToClient);
    EXPECT_EQ(loop.ReadyCount(), 0U);
    EXPECT_FALSE(loop.PopReady().has_value());
}

TEST(ReadyQueueTest, GenerationIsPartOfDeduplicationKey) {
    EventLoop loop;
    loop.EnqueueReady(ConnectionToken{1, 1}, DirectionId::kClientToUpstream,
                      WakeReason::kReadable);
    loop.EnqueueReady(ConnectionToken{1, 2}, DirectionId::kClientToUpstream,
                      WakeReason::kReadable);
    EXPECT_EQ(loop.ReadyCount(), 2U);
}

}  // namespace
}  // namespace chaosproxy
