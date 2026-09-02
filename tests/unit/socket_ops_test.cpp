#include "chaosproxy/socket_ops.h"
#include "helpers/test_socket_utils.h"

#include <gtest/gtest.h>
#include <sys/socket.h>

#include <array>
#include <string>

namespace {

using namespace chaosproxy;
using namespace chaosproxy::test;

TEST(SocketOpsTest, ReportsDataWouldBlockAndEof) {
    auto pair = MakeSocketPair();
    ASSERT_TRUE(pair.first.IsValid());

    std::array<char, 16> buffer{};
    EXPECT_EQ(
        TryRead(pair.first.Get(), buffer.data(), buffer.size()).status,
        ReadStatus::kWouldBlock);

    const std::string data = "hello";
    ASSERT_EQ(
        TryWrite(pair.second.Get(), data.data(), data.size()).status,
        WriteStatus::kWritten);

    const ReadResult read = TryRead(
        pair.first.Get(),
        buffer.data(),
        buffer.size());
    ASSERT_EQ(read.status, ReadStatus::kData);
    EXPECT_EQ(
        std::string(buffer.data(), read.bytes_transferred),
        data);

    ASSERT_EQ(::shutdown(pair.second.Get(), SHUT_WR), 0);
    EXPECT_EQ(
        TryRead(pair.first.Get(), buffer.data(), buffer.size()).status,
        ReadStatus::kEof);
}

TEST(SocketOpsTest, InvalidFdReturnsError) {
    char byte = 0;
    EXPECT_EQ(TryRead(-1, &byte, 1).status, ReadStatus::kError);
    EXPECT_EQ(TryWrite(-1, &byte, 1).status, WriteStatus::kError);
    EXPECT_EQ(TryAccept(-1).status, AcceptStatus::kError);
}

}  // namespace
