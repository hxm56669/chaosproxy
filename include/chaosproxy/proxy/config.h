#pragma once

#include "chaosproxy/common/status_or.h"
#include "chaosproxy/proxy/toxics.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace chaosproxy {

struct ListenerConfig {
    std::string listen_host;
    std::uint16_t listen_port = 0;
    std::string upstream_host;
    std::uint16_t upstream_port = 0;
};

struct ProxyConfig {
    std::vector<ListenerConfig> listeners;
    std::uint64_t policy_version = 1;
    std::size_t max_connections = 1024;
};

StatusOr<ProxyConfig> LoadProxyConfig(const std::filesystem::path& path);
Status ValidateProxyConfig(const ProxyConfig& config);

}  // namespace chaosproxy
