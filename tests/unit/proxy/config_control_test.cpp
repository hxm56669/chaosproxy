#include "chaosproxy/control/control_protocol.h"
#include "chaosproxy/control/control_server.h"
#include "chaosproxy/proxy/config.h"
#include "chaosproxy/proxy/proxy_server.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <span>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <string>

namespace chaosproxy {
namespace {

TEST(ConfigTest, LoadsAndValidatesMultipleListeners) {
    const auto path = std::filesystem::temp_directory_path() / "chaosproxy-a7.json";
    {
        std::ofstream output(path);
        output << R"({"policy_version":4,"listeners":[)"
                  R"({"listen_host":"127.0.0.1","listen_port":0,"upstream_host":"127.0.0.1","upstream_port":9})]})";
    }
    auto config = LoadProxyConfig(path);
    std::filesystem::remove(path);
    EXPECT_FALSE(config.ok());

    ProxyConfig valid;
    valid.listeners.push_back({"127.0.0.1", 12345, "127.0.0.1", 9});
    EXPECT_TRUE(ValidateProxyConfig(valid).ok());
}

TEST(ConfigTest, LoadsValidJsonConfiguration) {
    const auto path = std::filesystem::temp_directory_path() / "chaosproxy-a7-valid.json";
    {
        std::ofstream output(path);
        output << R"({"policy_version":4,"listeners":[)"
                  R"({"listen_host":"127.0.0.1","listen_port":12345,"upstream_host":"127.0.0.1","upstream_port":9}]})";
    }
    auto config = LoadProxyConfig(path);
    std::filesystem::remove(path);
    ASSERT_TRUE(config.ok());
    EXPECT_EQ(config.value().listeners.size(), 1U);
    EXPECT_EQ(config.value().policy_version, 4U);
}

TEST(ControlProtocolTest, HandlesPartialFrameAndEncodesAck) {
    ControlProtocol protocol;
    const std::string first = R"({"operation":"get_metrics","request_id":7,)";
    const std::string second = R"("expected_policy_version":3})" "\n";
    ASSERT_TRUE(protocol.Feed(std::as_bytes(std::span(first.data(), first.size()))).ok());
    auto partial = protocol.NextCommand();
    ASSERT_TRUE(partial.ok());
    EXPECT_FALSE(partial.value().has_value());
    ASSERT_TRUE(protocol.Feed(std::as_bytes(std::span(second.data(), second.size()))).ok());
    auto command = protocol.NextCommand();
    ASSERT_TRUE(command.ok());
    ASSERT_TRUE(command.value().has_value());
    EXPECT_EQ(command.value()->operation, "get_metrics");
    EXPECT_EQ(command.value()->expected_policy_version, 3U);
    const std::string reply = protocol.EncodeReply({7, true, "OK", "applied", 4, 2, {}});
    EXPECT_NE(reply.find("\"applied_version\":4"), std::string::npos);
    EXPECT_EQ(reply.back(), '\n');
}

TEST(ControlProtocolTest, RejectsOversizedFrame) {
    ControlProtocol protocol;
    std::string oversized(kMaxControlFrameBytes + 1, 'x');
    EXPECT_EQ(protocol.Feed(std::as_bytes(std::span(oversized.data(), oversized.size()))).code(),
              StatusCode::kResourceExhausted);
}

TEST(ProxyServerTest, AppliesOnlyExpectedPolicyVersion) {
    ProxyConfig config;
    config.policy_version = 4;
    config.listeners.push_back({"127.0.0.1", 12346, "127.0.0.1", 9});
    ProxyServer server;
    ASSERT_TRUE(server.Start(config).ok());
    auto policy = std::make_shared<PolicySnapshot>();
    auto conflict = server.ApplyPolicy({1, 3, {}, policy});
    EXPECT_EQ(conflict.status().code(), StatusCode::kConflict);
    auto applied = server.ApplyPolicy({2, 4, {}, policy});
    ASSERT_TRUE(applied.ok());
    EXPECT_EQ(applied.value().applied_version, 5U);
}

TEST(ControlServerTest, CreatesAndRemovesUnixSocket) {
    const auto path = std::filesystem::temp_directory_path() / "chaosproxy-a7.sock";
    ControlServer server;
    ASSERT_TRUE(server.Start(path).ok());
    EXPECT_GE(server.listen_fd(), 0);
    EXPECT_TRUE(std::filesystem::exists(path));
    server.StopAccepting();
    EXPECT_FALSE(std::filesystem::exists(path));
}

TEST(ControlServerTest, AcceptsSplitRequestAndQueuesAppliedAck) {
    const auto path = std::filesystem::temp_directory_path() / "chaosproxy-a7-roundtrip.sock";
    ControlServer server([](const ControlCommand& command) {
        return ControlReply{command.request_id, true, "OK", "applied", 8, 1, {}};
    });
    ASSERT_TRUE(server.Start(path).ok());
    const int client = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    const std::string name = path.string();
    std::copy(name.c_str(), name.c_str() + name.size() + 1, address.sun_path);
    ASSERT_EQ(::connect(client, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
    const auto accepted = server.AcceptPending();
    ASSERT_EQ(accepted.size(), 1U);
    const std::string first = R"({"operation":"apply_policy","request_id":9,)";
    const std::string second = R"("expected_policy_version":7})" "\n";
    ASSERT_EQ(::send(client, first.data(), first.size(), 0),
              static_cast<ssize_t>(first.size()));
    server.OnReadable(accepted[0]);
    char response[256]{};
    EXPECT_EQ(::recv(client, response, sizeof(response), MSG_DONTWAIT), -1);
    ASSERT_EQ(::send(client, second.data(), second.size(), 0),
              static_cast<ssize_t>(second.size()));
    server.OnReadable(accepted[0]);
    server.OnWritable(accepted[0]);
    const ssize_t count = ::recv(client, response, sizeof(response) - 1, 0);
    ASSERT_GT(count, 0);
    response[count] = '\0';
    EXPECT_NE(std::string(response).find("\"applied_version\":8"), std::string::npos);
    ::close(client);
}

}  // namespace
}  // namespace chaosproxy
