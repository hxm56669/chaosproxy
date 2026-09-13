#include "chaosproxy/common/unique_fd.h"
#include "chaosproxy/proxy/socket_ops.h"

#include <array>
#include <cerrno>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <utility>

#include <gtest/gtest.h>

namespace chaosproxy {
namespace {

struct BoundListener {
    UniqueFd fd;
    SocketAddress address;
};

BoundListener MakeBoundListener() {
    auto requested = SocketAddress::Parse("127.0.0.1", 0);
    EXPECT_TRUE(requested.ok());
    auto listener = CreateListener(requested.value(), 8);
    EXPECT_TRUE(listener.ok());

    sockaddr_storage native{};
    socklen_t length = sizeof(native);
    if (::getsockname(listener.value().get(),
                      reinterpret_cast<sockaddr*>(&native), &length) != 0) {
        ADD_FAILURE() << "getsockname failed: " << errno;
        return BoundListener{};
    }
    return BoundListener{std::move(listener).value(),
                         SocketAddress::FromNative(
                             reinterpret_cast<const sockaddr*>(&native),
                             length)};
}

TEST(UniqueFdTest, MoveAndReleaseHaveOneOwner) {
    int raw[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, raw), 0);
    UniqueFd first(raw[0]);
    UniqueFd second(std::move(first));
    EXPECT_FALSE(first);
    ASSERT_TRUE(second);
    const int released = second.release();
    EXPECT_FALSE(second);
    EXPECT_EQ(::close(released), 0);
    EXPECT_EQ(::close(raw[1]), 0);
}

TEST(SocketOpsTest, ListenerIsNonBlockingAndAcceptReportsWouldBlock) {
    BoundListener listener = MakeBoundListener();
    ASSERT_TRUE(listener.fd);
    const int flags = ::fcntl(listener.fd.get(), F_GETFL);
    ASSERT_NE(flags, -1);
    EXPECT_NE(flags & O_NONBLOCK, 0);
    const int descriptor_flags = ::fcntl(listener.fd.get(), F_GETFD);
    ASSERT_NE(descriptor_flags, -1);
    EXPECT_NE(descriptor_flags & FD_CLOEXEC, 0);

    auto accepted = TryAccept(listener.fd.get());
    ASSERT_TRUE(accepted.ok());
    EXPECT_FALSE(accepted.value().has_value());
}

TEST(SocketOpsTest, ConnectAndAcceptUseNonBlockingDescriptors) {
    BoundListener listener = MakeBoundListener();
    auto connection = StartConnect(listener.address);
    ASSERT_TRUE(connection.ok()) << connection.status().message();
    if (connection.value().progress == ConnectProgress::kInProgress) {
        pollfd descriptor{connection.value().fd.get(), POLLOUT, 0};
        ASSERT_EQ(::poll(&descriptor, 1, 1000), 1);
        ASSERT_TRUE(FinishConnect(connection.value().fd.get()).ok());
    }

    std::optional<AcceptResult> accepted;
    for (int attempt = 0; attempt < 20 && !accepted.has_value(); ++attempt) {
        auto result = TryAccept(listener.fd.get());
        ASSERT_TRUE(result.ok()) << result.status().message();
        accepted = std::move(result).value();
        if (!accepted.has_value()) {
            pollfd descriptor{listener.fd.get(), POLLIN, 0};
            ASSERT_EQ(::poll(&descriptor, 1, 1000), 1);
        }
    }
    ASSERT_TRUE(accepted.has_value());
    EXPECT_TRUE(accepted->fd);
}

TEST(SocketOpsTest, ReadWriteDistinguishProgressWouldBlockAndEof) {
    int raw[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                           0, raw),
              0);
    UniqueFd reader(raw[0]);
    UniqueFd writer(raw[1]);

    std::array<std::byte, 16> buffer{};
    EXPECT_EQ(TryRead(reader.get(), std::span<std::byte>(buffer)).code,
              IoCode::kWouldBlock);

    const std::array<std::byte, 3> payload{
        std::byte{0x01}, std::byte{0x02}, std::byte{0x03}};
    const IoResult write_result =
        TryWrite(writer.get(), std::span<const std::byte>(payload));
    ASSERT_EQ(write_result.code, IoCode::kProgress);
    ASSERT_EQ(write_result.bytes, payload.size());

    const IoResult read_result =
        TryRead(reader.get(), std::span<std::byte>(buffer));
    ASSERT_EQ(read_result.code, IoCode::kProgress);
    ASSERT_EQ(read_result.bytes, payload.size());
    EXPECT_EQ(buffer[0], payload[0]);
    EXPECT_EQ(buffer[2], payload[2]);

    writer.reset();
    EXPECT_EQ(TryRead(reader.get(), std::span<std::byte>(buffer)).code,
              IoCode::kEof);
}

}  // namespace
}  // namespace chaosproxy
