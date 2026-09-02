#include "chaosproxy/connection.h"
#include "chaosproxy/socket_ops.h"
#include "helpers/test_connection_harness.h"
#include "helpers/test_socket_utils.h"

#include <gtest/gtest.h>

#include <string>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;

TEST(ConnectionTest, ForwardsBytesInBothDirections) {
    auto client_pair = MakeSocketPair();
    auto upstream_pair = MakeSocketPair();
    ASSERT_TRUE(client_pair.first.IsValid());
    ASSERT_TRUE(upstream_pair.first.IsValid());

    EventLoop loop;
    CloseReason close_reason = CloseReason::kNone;

    DirectConnectionHarness harness(
        loop,
        ConnectionToken{1, 1},
        std::move(client_pair.second),
        std::move(upstream_pair.first),
        EndpointState::kEstablished,
        ConnectionLimits{},
        [&](ConnectionToken, CloseReason reason) {
            close_reason = reason;
        });

    harness.Start();

    const std::string request = "client-to-upstream";
    ASSERT_EQ(
        TryWrite(
            client_pair.first.Get(),
            request.data(),
            request.size()).status,
        WriteStatus::kWritten);

    std::string upstream_received;
    ASSERT_TRUE(PumpUntil(loop, [&] {
        upstream_received += DrainAvailable(upstream_pair.second.Get());
        return upstream_received.size() == request.size();
    }));
    EXPECT_EQ(upstream_received, request);

    const std::string response = "upstream-to-client";
    ASSERT_EQ(
        TryWrite(
            upstream_pair.second.Get(),
            response.data(),
            response.size()).status,
        WriteStatus::kWritten);

    std::string client_received;
    ASSERT_TRUE(PumpUntil(loop, [&] {
        client_received += DrainAvailable(client_pair.first.Get());
        return client_received.size() == response.size();
    }));
    EXPECT_EQ(client_received, response);
    EXPECT_EQ(close_reason, CloseReason::kNone);
}

TEST(ConnectionTest, CloseIsIdempotent) {
    auto client_pair = MakeSocketPair();
    auto upstream_pair = MakeSocketPair();
    EventLoop loop;
    int close_callback_count = 0;

    DirectConnectionHarness harness(
        loop,
        ConnectionToken{7, 3},
        std::move(client_pair.second),
        std::move(upstream_pair.first),
        EndpointState::kEstablished,
        ConnectionLimits{},
        [&](ConnectionToken token, CloseReason) {
            EXPECT_EQ(token, (ConnectionToken{7, 3}));
            ++close_callback_count;
        });

    harness.Start();
    harness.Connection().Close(CloseReason::kLocalStop);
    harness.Connection().Close(CloseReason::kSocketError);

    EXPECT_TRUE(harness.Connection().IsClosed());
    EXPECT_EQ(harness.Connection().Reason(), CloseReason::kLocalStop);
    EXPECT_EQ(close_callback_count, 1);
}

TEST(ConnectionTest, RejectsInvalidLimits) {
    auto client_pair = MakeSocketPair();
    auto upstream_pair = MakeSocketPair();
    EventLoop loop;
    ConnectionLimits limits;
    limits.low_watermark = limits.high_watermark;

    EXPECT_THROW(
        DirectConnectionHarness(
            loop,
            ConnectionToken{1, 1},
            std::move(client_pair.second),
            std::move(upstream_pair.first),
            EndpointState::kEstablished,
            limits,
            [](ConnectionToken, CloseReason) {}),
        std::invalid_argument);
}

}  // namespace
