#include "chaosproxy/control/control_protocol.h"

#include <algorithm>

#include <json/json.h>

namespace chaosproxy {

Status ControlProtocol::Feed(std::span<const std::byte> bytes) {
    if (bytes.size() > kMaxControlFrameBytes - std::min(input_.size(), kMaxControlFrameBytes)) {
        return Status(StatusCode::kResourceExhausted, "control frame exceeds 64 KiB");
    }
    input_.append(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    const auto newline = input_.find('\n');
    if (newline == std::string::npos && input_.size() > kMaxControlFrameBytes) {
        return Status(StatusCode::kResourceExhausted, "control frame exceeds 64 KiB");
    }
    return Status::Ok();
}

StatusOr<std::optional<ControlCommand>> ControlProtocol::NextCommand() {
    const auto newline = input_.find('\n');
    if (newline == std::string::npos) return std::optional<ControlCommand>{};
    std::string frame = input_.substr(0, newline);
    input_.erase(0, newline + 1);
    if (frame.empty() || frame.size() > kMaxControlFrameBytes) {
        return Status(StatusCode::kInvalidArgument, "invalid control frame size");
    }
    Json::CharReaderBuilder builder;
    Json::Value root;
    std::string errors;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    if (!reader->parse(frame.data(), frame.data() + frame.size(), &root, &errors) ||
        !root.isObject() || !root["operation"].isString() ||
        !root["request_id"].isUInt64() ||
        !root["expected_policy_version"].isUInt64()) {
        return Status(StatusCode::kInvalidArgument, "invalid control command: " + errors);
    }
    ControlCommand command;
    command.operation = root["operation"].asString();
    command.request_id = root["request_id"].asUInt64();
    command.expected_policy_version = root["expected_policy_version"].asUInt64();
    if (root.isMember("payload")) command.payload = root["payload"].toStyledString();
    return std::optional<ControlCommand>(std::move(command));
}

std::string ControlProtocol::EncodeReply(const ControlReply& reply) const {
    Json::Value root;
    root["request_id"] = Json::UInt64(reply.request_id);
    root["ok"] = reply.ok;
    root["code"] = reply.code;
    root["message"] = reply.message;
    root["applied_version"] = Json::UInt64(reply.applied_version);
    root["affected_connections"] = Json::UInt64(reply.affected_connections);
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, root) + "\n";
}

}  // namespace chaosproxy
