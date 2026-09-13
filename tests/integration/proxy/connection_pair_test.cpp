#include "chaosproxy/proxy/connection_pair.h"

#include <array>
#include <cstring>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

namespace chaosproxy {
namespace {

TEST(ConnectionPairTest, PumpsBothDirectionsThroughOneImplementation) {
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
    ConnectionPair pair{UniqueFd(client_sides[1]),
                        UniqueFd(upstream_sides[0])};

    const char client_message[] = "client-to-upstream";
    ASSERT_GT(::send(client_peer.get(), client_message,
                     sizeof(client_message) - 1, MSG_NOSIGNAL), 0);
    pair.OnReadable(EndpointSide::kClient);
    std::array<char, 64> received{};
    const ssize_t upstream_count =
        ::recv(upstream_peer.get(), received.data(), received.size(), 0);
    ASSERT_EQ(upstream_count,
              static_cast<ssize_t>(sizeof(client_message) - 1));
    EXPECT_EQ(std::string(received.data(), static_cast<std::size_t>(
                                               upstream_count)),
              client_message);

    const char upstream_message[] = "upstream-to-client";
    ASSERT_GT(::send(upstream_peer.get(), upstream_message,
                     sizeof(upstream_message) - 1, MSG_NOSIGNAL), 0);
    pair.OnReadable(EndpointSide::kUpstream);
    received.fill('\0');
    const ssize_t client_count =
        ::recv(client_peer.get(), received.data(), received.size(), 0);
    ASSERT_EQ(client_count,
              static_cast<ssize_t>(sizeof(upstream_message) - 1));
    EXPECT_EQ(std::string(received.data(), static_cast<std::size_t>(
                                               client_count)),
              upstream_message);
}

TEST(EventLoopTest, DispatchesRegisteredTokenAndCanModifyAndRemove) {
    int raw[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX,
                           SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                           raw),
              0);
    UniqueFd reader(raw[0]);
    UniqueFd writer(raw[1]);
    std::uint64_t received_token = 0;
    std::uint32_t received_events = 0;
    EventLoop loop([&](EventToken token, std::uint32_t events) {
        received_token = token.packed;
        received_events = events;
        std::array<std::byte, 8> buffer{};
        (void)::recv(reader.get(), buffer.data(), buffer.size(), 0);
    });
    ASSERT_TRUE(loop.AddFd(reader.get(), EPOLLIN | EPOLLET,
                           EventToken{0xabc}).ok());
    ASSERT_TRUE(loop.ModifyFd(reader.get(), EPOLLIN | EPOLLET).ok());
    const std::byte value{0x2a};
    ASSERT_EQ(::send(writer.get(), &value, sizeof(value), MSG_NOSIGNAL),
              static_cast<ssize_t>(sizeof(value)));
    loop.RunOnce(1000);
    EXPECT_EQ(received_token, 0xabcU);
    EXPECT_NE(received_events & EPOLLIN, 0U);
    ASSERT_TRUE(loop.RemoveFd(reader.get()).ok());
    EXPECT_EQ(loop.RemoveFd(reader.get()).code(), StatusCode::kNotFound);
}

}  // namespace
}  // namespace chaosproxy
