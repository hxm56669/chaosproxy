#pragma once

#include "chaosproxy/common/status_or.h"
#include "chaosproxy/proxy/clock.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>

namespace chaosproxy {

constexpr std::size_t kMaxControlFrameBytes = 64 * 1024;

struct ControlCommand {
    std::string operation;
    std::uint64_t request_id = 0;
    std::uint64_t expected_policy_version = 0;
    std::string payload;
};

struct ControlReply {
    std::uint64_t request_id = 0;
    bool ok = false;
    std::string code;
    std::string message;
    std::uint64_t applied_version = 0;
    std::size_t affected_connections = 0;
    TimePoint started_at{};
};

class ControlProtocol {
public:
    Status Feed(std::span<const std::byte> bytes);
    StatusOr<std::optional<ControlCommand>> NextCommand();
    std::string EncodeReply(const ControlReply& reply) const;

private:
    std::string input_;
};

}  // namespace chaosproxy
