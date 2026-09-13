#include "chaosproxy/proxy/config.h"

#include <fstream>

#include <json/json.h>

namespace chaosproxy {

namespace {

Status Invalid(std::string message) {
    return Status(StatusCode::kInvalidArgument, std::move(message));
}

StatusOr<std::uint16_t> ReadPort(const Json::Value& object,
                                 const char* field) {
    if (!object.isMember(field) || !object[field].isUInt() ||
        object[field].asUInt() == 0 || object[field].asUInt() > 65535) {
        return Invalid(std::string("invalid ") + field);
    }
    return static_cast<std::uint16_t>(object[field].asUInt());
}

}  // namespace

StatusOr<ProxyConfig> LoadProxyConfig(const std::filesystem::path& path) {
    std::ifstream input(path);
    if (!input) {
        return Status(StatusCode::kNotFound, "cannot open proxy config");
    }
    Json::CharReaderBuilder builder;
    Json::Value root;
    std::string errors;
    if (!Json::parseFromStream(builder, input, &root, &errors) || !root.isObject()) {
        return Invalid("config is not valid JSON: " + errors);
    }
    if (!root["listeners"].isArray()) {
        return Invalid("listeners must be an array");
    }
    ProxyConfig config;
    if (root.isMember("policy_version")) {
        if (!root["policy_version"].isUInt64() || root["policy_version"].asUInt64() == 0) {
            return Invalid("policy_version must be a positive integer");
        }
        config.policy_version = root["policy_version"].asUInt64();
    }
    if (root.isMember("max_connections")) {
        if (!root["max_connections"].isUInt() || root["max_connections"].asUInt() == 0) {
            return Invalid("max_connections must be positive");
        }
        config.max_connections = root["max_connections"].asUInt();
    }
    for (const Json::Value& value : root["listeners"]) {
        if (!value.isObject() || !value["listen_host"].isString() ||
            !value["upstream_host"].isString()) {
            return Invalid("listener hosts must be strings");
        }
        auto listen_port = ReadPort(value, "listen_port");
        auto upstream_port = ReadPort(value, "upstream_port");
        if (!listen_port.ok()) return listen_port.status();
        if (!upstream_port.ok()) return upstream_port.status();
        config.listeners.push_back(ListenerConfig{
            value["listen_host"].asString(), listen_port.value(),
            value["upstream_host"].asString(), upstream_port.value()});
    }
    Status status = ValidateProxyConfig(config);
    if (!status.ok()) return status;
    return config;
}

Status ValidateProxyConfig(const ProxyConfig& config) {
    if (config.listeners.empty()) {
        return Invalid("at least one listener is required");
    }
    if (config.policy_version == 0 || config.max_connections == 0) {
        return Invalid("policy_version and max_connections must be positive");
    }
    for (const ListenerConfig& listener : config.listeners) {
        if (listener.listen_host.empty() || listener.upstream_host.empty() ||
            listener.listen_port == 0 || listener.upstream_port == 0) {
            return Invalid("listener host and ports must be set");
        }
    }
    return Status::Ok();
}

}  // namespace chaosproxy
