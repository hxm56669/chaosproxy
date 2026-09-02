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

TEST(HalfCloseTest, ClientFinIsPropagatedOnlyAfterRequestIsForwarded) {
    auto client_pair = MakeSocketPair();
    auto upstream_pair = MakeSocketPair();
    ASSERT_TRUE(client_pair.first.IsValid());
    ASSERT_TRUE(upstream_pair.first.IsValid());

    ConnectionLimits limits;
    limits.read_budget_per_event = 256 * 1024;

    EventLoop loop;
    CloseReason close_reason = CloseReason::kNone;

    DirectConnectionHarness harness(
        loop,
        ConnectionToken{1, 1},
        std::move(client_pair.second),
        std::move(upstream_pair.first),
        EndpointState::kEstablished,
        limits,
        [&](ConnectionToken, CloseReason reason) {
            close_reason = reason;
        });
    harness.Start();
    ConnectionPair& connection = harness.Connection();

    const std::string request(64 * 1024, 'r');
    ASSERT_TRUE(WriteAllWithLoop(loop, client_pair.first.Get(), request));
    ASSERT_EQ(::shutdown(client_pair.first.Get(), SHUT_WR), 0);

    std::string upstream_received;
    bool upstream_eof = false;

    ASSERT_TRUE(PumpUntil(loop, [&] {
        std::array<char, 8192> buffer{};
        for (;;) {
            const ReadResult result = TryRead(
                upstream_pair.second.Get(),
                buffer.data(),
                buffer.size());

            if (result.status == ReadStatus::kData) {
                upstream_received.append(
                    buffer.data(),
                    result.bytes_transferred);
                continue;
            }

            if (result.status == ReadStatus::kEof) {
                upstream_eof = true;
            }
            break;
        }

        return upstream_eof;
    }, 500, 2));

    EXPECT_EQ(upstream_received, request);
    EXPECT_TRUE(connection.SourceEof(EndpointSide::kClient));
    EXPECT_TRUE(connection.WriteShutdownFor(EndpointSide::kClient));

    const std::string response = "response-after-half-close";
    ASSERT_EQ(
        TryWrite(
            upstream_pair.second.Get(),
            response.data(),
            response.size()).status,
        WriteStatus::kWritten);
    ASSERT_EQ(::shutdown(upstream_pair.second.Get(), SHUT_WR), 0);

    std::string client_received;
    bool client_eof = false;

    ASSERT_TRUE(PumpUntil(loop, [&] {
        std::array<char, 128> buffer{};
        for (;;) {
            const ReadResult result = TryRead(
                client_pair.first.Get(),
                buffer.data(),
                buffer.size());

            if (result.status == ReadStatus::kData) {
                client_received.append(
                    buffer.data(),
                    result.bytes_transferred);
                continue;
            }

            if (result.status == ReadStatus::kEof) {
                client_eof = true;
            }
            break;
        }

        return client_eof && connection.IsClosed();
    }, 500, 2));

    EXPECT_EQ(client_received, response);
    EXPECT_EQ(close_reason, CloseReason::kGracefulEof);
}

}  // namespace
