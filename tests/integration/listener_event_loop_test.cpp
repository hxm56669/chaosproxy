#include "chaosproxy/listener.h"
#include "chaosproxy/socket_ops.h"
#include "helpers/test_socket_utils.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <sys/socket.h>

#include <cerrno>
#include <system_error>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;

UniqueFd MakeListenerSocket(sockaddr_in& address) {
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
            "listener setup");
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

TEST(ListenerEventLoopTest, AcceptsAllReadyClients) {
    sockaddr_in address{};
    UniqueFd listen_fd = MakeListenerSocket(address);
    EventLoop loop;
    int accepted_count = 0;
    int listener_error = 0;

    Listener listener(
        loop,
        std::move(listen_fd),
        [&](UniqueFd client_fd) {
            EXPECT_TRUE(client_fd.IsValid());
            ++accepted_count;
        },
        [&](int error_number) {
            listener_error = error_number;
        });

    UniqueFd first(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
    UniqueFd second(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(first.IsValid());
    ASSERT_TRUE(second.IsValid());
    ASSERT_EQ(
        ::connect(
            first.Get(),
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)),
        0);
    ASSERT_EQ(
        ::connect(
            second.Get(),
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)),
        0);

    ASSERT_TRUE(PumpUntil(loop, [&] {
        return accepted_count == 2;
    }));
    EXPECT_EQ(listener_error, 0);
}

}  // namespace
