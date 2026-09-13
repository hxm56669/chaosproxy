#include "chaosproxy/proxy/connection_pair.h"
#include "chaosproxy/proxy/connection_table.h"

#include <sys/socket.h>

#include <gtest/gtest.h>

namespace chaosproxy {
namespace {

std::unique_ptr<ConnectionPair> MakePair() {
    int raw[2] = {-1, -1};
    EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                           0, raw),
              0);
    int other[2] = {-1, -1};
    EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                           0, other),
              0);
    return std::make_unique<ConnectionPair>(UniqueFd(raw[0]),
                                             UniqueFd(other[0]));
}

TEST(ConnectionTableTest, RetireInvalidatesTokenBeforeReclaim) {
    ConnectionTable table;
    auto first = table.Insert(MakePair());
    ASSERT_TRUE(first.ok());
    const ConnectionToken old_token = first.value();
    ASSERT_NE(table.Find(old_token), nullptr);

    table.Retire(old_token);
    EXPECT_EQ(table.Find(old_token), nullptr);
    table.ReclaimRetired();

    auto second = table.Insert(MakePair());
    ASSERT_TRUE(second.ok());
    EXPECT_EQ(second.value().slot, old_token.slot);
    EXPECT_NE(second.value().generation, old_token.generation);
    EXPECT_EQ(table.Find(old_token), nullptr);
    EXPECT_NE(table.Find(second.value()), nullptr);
}

TEST(ConnectionTableTest, RejectsNullPair) {
    ConnectionTable table;
    auto result = table.Insert(nullptr);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace chaosproxy
