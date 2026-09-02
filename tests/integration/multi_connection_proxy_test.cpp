#include "chaosproxy/connection_manager.h"
#include "chaosproxy/socket_ops.h"
#include "helpers/test_socket_utils.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <sys/socket.h>

#include <cerrno>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;

UniqueFd MakeLoopbackListener(sockaddr_in& address) {
    UniqueFd listen_fd(::socket(
        AF_INET,
        SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
        0));

    if (!listen_fd.IsValid()) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "socket");
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

TEST(MultiConnectionProxyTest, ManagerRunsSeveralConnectionsOnOneEventLoop) {
    sockaddr_in upstream_address{};
    UniqueFd upstream_listener = MakeLoopbackListener(upstream_address);

    EventLoop loop;
    ConnectionManager manager(
        loop,
        reinterpret_cast<const sockaddr*>(&upstream_address),
        sizeof(upstream_address));

    std::vector<UniqueFd> client_apps;
    client_apps.reserve(3);

    for (int index = 0; index < 3; ++index) {
        auto pair = MakeSocketPair();
        ASSERT_TRUE(pair.first.IsValid());
        ASSERT_TRUE(pair.second.IsValid());

        const CreateConnectionResult result =
            manager.Create(std::move(pair.second));
        ASSERT_TRUE(result.Ok());
        client_apps.push_back(std::move(pair.first));
    }

    EXPECT_EQ(manager.Size(), 3U);

    std::vector<UniqueFd> upstream_apps;
    upstream_apps.reserve(3);

    ASSERT_TRUE(PumpUntil(loop, [&] {
        for (;;) {
            AcceptResult result = TryAccept(upstream_listener.Get());
            if (result.status == AcceptStatus::kAccepted) {
                upstream_apps.push_back(std::move(result.client_fd));
                continue;
            }
            EXPECT_EQ(result.status, AcceptStatus::kWouldBlock);
            break;
        }
        return upstream_apps.size() == 3U;
    }));

    for (int index = 0; index < 3; ++index) {
        const char byte = static_cast<char>('a' + index);
        ASSERT_EQ(
            TryWrite(client_apps[index].Get(), &byte, 1).status,
            WriteStatus::kWritten);
    }

    std::string observed;
    ASSERT_TRUE(PumpUntil(loop, [&] {
        for (auto& upstream : upstream_apps) {
            observed += DrainAvailable(upstream.Get());
        }
        return observed.size() == 3U;
    }));

    EXPECT_NE(observed.find('a'), std::string::npos);
    EXPECT_NE(observed.find('b'), std::string::npos);
    EXPECT_NE(observed.find('c'), std::string::npos);
}

}  // namespace
