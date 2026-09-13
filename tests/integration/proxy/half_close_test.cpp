#include "chaosproxy/proxy/connection_pair.h"

#include <array>
#include <string>
#include <sys/socket.h>

#include <gtest/gtest.h>

namespace chaosproxy {
namespace {

TEST(ConnectionPairTest, FinIsForwardedAfterQueuedBytesAreDrained) {
    int client_sides[2] = {-1, -1};
    int upstream_sides[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX,
                           SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                           client_sides),
              0);
    ASSERT_EQ(::socketpair(AF_UNIX,
                           SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                           upstream_sides),
              0);
    UniqueFd client_peer(client_sides[0]);
    UniqueFd upstream_peer(upstream_sides[1]);
    ConnectionPair pair{UniqueFd(client_sides[1]), UniqueFd(upstream_sides[0])};

    const std::string message = "tail-before-fin";
    ASSERT_EQ(::send(client_peer.get(), message.data(), message.size(),
                     MSG_NOSIGNAL),
              static_cast<ssize_t>(message.size()));
    ASSERT_EQ(::shutdown(client_peer.get(), SHUT_WR), 0);
    pair.OnReadable(EndpointSide::kClient);

    std::array<char, 64> bytes{};
    const ssize_t count =
        ::recv(upstream_peer.get(), bytes.data(), bytes.size(), 0);
    ASSERT_EQ(count, static_cast<ssize_t>(message.size()));
    EXPECT_EQ(std::string(bytes.data(), static_cast<std::size_t>(count)),
              message);
    EXPECT_EQ(::recv(upstream_peer.get(), bytes.data(), bytes.size(), 0), 0);
}

}  // namespace
}  // namespace chaosproxy
