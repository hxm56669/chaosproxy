#include "chaosproxy/connection.h"
#include "chaosproxy/socket_ops.h"
#include "helpers/test_connection_harness.h"
#include "helpers/test_socket_utils.h"

#include <gtest/gtest.h>
#include <sys/socket.h>

#include <array>
#include <string>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;

TEST(BackpressureTest, PausesSourceAtHighWaterAndResumesAtLowWater) {
    auto client_pair = MakeSocketPair();
    auto upstream_pair = MakeSocketPair();
    ASSERT_TRUE(client_pair.first.IsValid());
    ASSERT_TRUE(upstream_pair.first.IsValid());

    int send_buffer = 1024;
    ASSERT_EQ(
        ::setsockopt(
            upstream_pair.first.Get(),
            SOL_SOCKET,
            SO_SNDBUF,
            &send_buffer,
            sizeof(send_buffer)),
        0);

    std::array<char, 4096> filler{};
    bool upstream_blocked = false;
    for (int index = 0; index < 1024; ++index) {
        const WriteResult result = TryWrite(
            upstream_pair.first.Get(),
            filler.data(),
            filler.size());

        if (result.status == WriteStatus::kWouldBlock) {
            upstream_blocked = true;
            break;
        }
        ASSERT_EQ(result.status, WriteStatus::kWritten);
    }
    ASSERT_TRUE(upstream_blocked);

    ConnectionLimits limits;
    limits.buffer_capacity = 8192;
    limits.high_watermark = 4096;
    limits.low_watermark = 2048;
    limits.read_budget_per_event = 8192;
    limits.write_budget_per_event = 8192;

    EventLoop loop;
    DirectConnectionHarness harness(
        loop,
        ConnectionToken{1, 1},
        std::move(client_pair.second),
        std::move(upstream_pair.first),
        EndpointState::kEstablished,
        limits,
        [](ConnectionToken, CloseReason) {});
    harness.Start();
    ConnectionPair& connection = harness.Connection();

    const std::string data(8192, 'x');
    ASSERT_TRUE(WriteAllWithLoop(loop, client_pair.first.Get(), data));

    ASSERT_TRUE(PumpUntil(loop, [&] {
        return connection.ReadPaused(EndpointSide::kClient);
    }));

    EXPECT_GE(
        connection.PendingBytes(EndpointSide::kClient),
        limits.high_watermark);
    EXPECT_LE(
        connection.PendingBytes(EndpointSide::kClient),
        limits.buffer_capacity);

    for (int round = 0; round < 200; ++round) {
        (void)DrainAvailable(upstream_pair.second.Get());
        (void)loop.RunOnce(1);
        if (!connection.ReadPaused(EndpointSide::kClient) &&
            connection.PendingBytes(EndpointSide::kClient) == 0) {
            break;
        }
    }

    EXPECT_FALSE(connection.ReadPaused(EndpointSide::kClient));
    EXPECT_EQ(connection.PendingBytes(EndpointSide::kClient), 0U);
}

}  // namespace
