#include "chaosproxy/connection.h"
#include "helpers/test_connection_harness.h"
#include "helpers/test_socket_utils.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <sys/socket.h>

#include <cerrno>
#include <system_error>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;

struct TcpPair {
    UniqueFd client;
    UniqueFd server;
};

TcpPair MakeTcpPair() {
    UniqueFd listen_fd(::socket(
        AF_INET,
        SOCK_STREAM | SOCK_CLOEXEC,
        0));
    if (!listen_fd.IsValid()) {
        throw std::system_error(errno, std::generic_category(), "socket");
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;

    if (::bind(
            listen_fd.Get(),
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)) < 0 ||
        ::listen(listen_fd.Get(), 8) < 0) {
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

    UniqueFd client(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
    if (!client.IsValid()) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "socket client");
    }

    if (::connect(
            client.Get(),
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)) < 0) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "connect");
    }

    UniqueFd server(::accept4(
        listen_fd.Get(),
        nullptr,
        nullptr,
        SOCK_NONBLOCK | SOCK_CLOEXEC));
    if (!server.IsValid()) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "accept4");
    }

    return {std::move(client), std::move(server)};
}

TEST(ConnectionErrorTest, TcpResetClosesWholeConnectionExactlyOnce) {
    TcpPair client_side = MakeTcpPair();
    auto upstream_pair = MakeSocketPair();
    EventLoop loop;

    int close_count = 0;
    CloseReason reason = CloseReason::kNone;

    DirectConnectionHarness harness(
        loop,
        ConnectionToken{1, 1},
        std::move(client_side.server),
        std::move(upstream_pair.first),
        EndpointState::kEstablished,
        ConnectionLimits{},
        [&](ConnectionToken, CloseReason observed) {
            ++close_count;
            reason = observed;
        });
    harness.Start();
    ConnectionPair& connection = harness.Connection();

    linger reset_linger{};
    reset_linger.l_onoff = 1;
    reset_linger.l_linger = 0;
    ASSERT_EQ(
        ::setsockopt(
            client_side.client.Get(),
            SOL_SOCKET,
            SO_LINGER,
            &reset_linger,
            sizeof(reset_linger)),
        0);

    client_side.client.Reset();

    ASSERT_TRUE(PumpUntil(loop, [&] {
        return connection.IsClosed();
    }));

    EXPECT_EQ(close_count, 1);
    EXPECT_EQ(reason, CloseReason::kConnectionReset);
}

}  // namespace
