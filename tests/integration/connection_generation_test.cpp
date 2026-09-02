#include "chaosproxy/connection_manager.h"
#include "chaosproxy/event_loop.h"
#include "chaosproxy/socket_ops.h"
#include "helpers/test_socket_utils.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <sys/socket.h>

#include <cerrno>
#include <system_error>
#include <utility>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;

UniqueFd MakeGenerationTestListener(sockaddr_in& address) {
    UniqueFd listen_fd(::socket(
        AF_INET,
        SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
        0));

    if (!listen_fd.IsValid()) {
        throw std::system_error(errno, std::generic_category(), "socket");
    }

    address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;

    if (::bind(
            listen_fd.Get(),
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)) < 0 ||
        ::listen(listen_fd.Get(), SOMAXCONN) < 0) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "listen setup");
    }

    socklen_t length = sizeof(address);
    if (::getsockname(
            listen_fd.Get(),
            reinterpret_cast<sockaddr*>(&address),
            &length) < 0) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "getsockname");
    }

    return listen_fd;
}

UniqueFd AcceptGenerationPeer(EventLoop& loop, int listen_fd) {
    UniqueFd accepted;

    EXPECT_TRUE(PumpUntil(loop, [&] {
        AcceptResult result = TryAccept(listen_fd);
        if (result.status == AcceptStatus::kAccepted) {
            accepted = std::move(result.client_fd);
            return true;
        }

        EXPECT_EQ(result.status, AcceptStatus::kWouldBlock);
        return false;
    }));

    return accepted;
}

TEST(ConnectionGenerationTest, ReusedIdGetsNewGeneration) {
    sockaddr_in upstream_address{};
    UniqueFd upstream_listener =
        MakeGenerationTestListener(upstream_address);

    EventLoop loop;
    ConnectionManager manager(
        loop,
        reinterpret_cast<const sockaddr*>(&upstream_address),
        sizeof(upstream_address));

    auto first_pair = MakeSocketPair();
    const CreateConnectionResult first =
        manager.Create(std::move(first_pair.second));
    ASSERT_TRUE(first.Ok());
    UniqueFd first_upstream =
        AcceptGenerationPeer(loop, upstream_listener.Get());
    ASSERT_TRUE(first_upstream.IsValid());

    ASSERT_TRUE(manager.Close(first.token));
    EXPECT_EQ(manager.Size(), 0U);
    EXPECT_EQ(manager.Find(first.token), nullptr);

    auto second_pair = MakeSocketPair();
    const CreateConnectionResult second =
        manager.Create(std::move(second_pair.second));
    ASSERT_TRUE(second.Ok());
    UniqueFd second_upstream =
        AcceptGenerationPeer(loop, upstream_listener.Get());
    ASSERT_TRUE(second_upstream.IsValid());

    EXPECT_EQ(second.token.id, first.token.id);
    EXPECT_NE(second.token.generation, first.token.generation);
    EXPECT_EQ(manager.Find(first.token), nullptr);
    EXPECT_NE(manager.Find(second.token), nullptr);
}

}  // namespace
