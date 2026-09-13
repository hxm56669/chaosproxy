#pragma once

#include "chaosproxy/common/status_or.h"
#include "chaosproxy/proxy/config.h"
#include "chaosproxy/proxy/event_loop.h"
#include "chaosproxy/proxy/timer_queue.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chaosproxy {

struct ApplyPolicyCommand {
    std::uint64_t request_id = 0;
    std::uint64_t expected_policy_version = 0;
    ProxyId proxy;
    std::shared_ptr<const PolicySnapshot> policy;
};

struct AppliedPolicy {
    std::uint64_t applied_version = 0;
    std::size_t affected_connections = 0;
    TimePoint started_at{};
};

struct ProxyMetrics {
    std::size_t listener_count = 0;
    std::uint64_t policy_version = 0;
    std::size_t active_connections = 0;
    bool draining = false;
};

class ProxyServer {
public:
    ProxyServer();
    Status Start(const ProxyConfig& config);
    StatusOr<AppliedPolicy> ApplyPolicy(const ApplyPolicyCommand& command);
    Status RestoreBaseline(ProxyId proxy);
    ProxyMetrics SnapshotMetrics() const;
    void Run();
    void BeginDrain(TimePoint deadline);

private:
    EventLoop loop_;
    TimerQueue timers_;
    std::vector<UniqueFd> listeners_;
    ProxyConfig config_;
    std::uint64_t policy_version_ = 0;
    bool started_ = false;
    bool draining_ = false;
};

}  // namespace chaosproxy
