#include "chaosproxy/proxy/proxy_server.h"

#include "chaosproxy/proxy/socket_ops.h"

namespace chaosproxy {

ProxyServer::ProxyServer() { static_cast<void>(loop_.AttachTimerQueue(&timers_)); }

Status ProxyServer::Start(const ProxyConfig& config) {
    Status valid = ValidateProxyConfig(config);
    if (!valid.ok()) return valid;
    std::vector<UniqueFd> new_listeners;
    for (const ListenerConfig& listener : config.listeners) {
        auto address = SocketAddress::Parse(listener.listen_host, listener.listen_port);
        if (!address.ok()) return address.status();
        auto fd = CreateListener(address.value(), 128);
        if (!fd.ok()) return fd.status();
        new_listeners.push_back(std::move(fd).value());
    }
    listeners_ = std::move(new_listeners);
    config_ = config;
    policy_version_ = config.policy_version;
    started_ = true;
    draining_ = false;
    return Status::Ok();
}

StatusOr<AppliedPolicy> ProxyServer::ApplyPolicy(const ApplyPolicyCommand& command) {
    if (!started_) return Status(StatusCode::kUnavailable, "proxy is not started");
    if (command.expected_policy_version != policy_version_) {
        return Status(StatusCode::kConflict, "expected policy version does not match");
    }
    if (!command.policy) {
        return Status(StatusCode::kInvalidArgument, "policy snapshot is required");
    }
    policy_version_ = policy_version_ + 1;
    return AppliedPolicy{policy_version_, 0, Clock::now()};
}

Status ProxyServer::RestoreBaseline(ProxyId proxy) {
    static_cast<void>(proxy);
    if (!started_) return Status(StatusCode::kUnavailable, "proxy is not started");
    policy_version_ = config_.policy_version;
    return Status::Ok();
}

ProxyMetrics ProxyServer::SnapshotMetrics() const {
    return ProxyMetrics{listeners_.size(), policy_version_, 0, draining_};
}

void ProxyServer::Run() { loop_.Run(); }

void ProxyServer::BeginDrain(TimePoint deadline) {
    draining_ = true;
    timers_.Schedule(deadline, [this] { loop_.RequestStop(); });
}

}  // namespace chaosproxy
