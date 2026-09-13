#include "phototask/dependency_guard.h"

namespace phototask {

DependencyGuard::DependencyGuard(std::size_t permit_limit, std::size_t failure_threshold,
                                 std::chrono::milliseconds cooldown)
    : permit_limit_(permit_limit), failure_threshold_(failure_threshold), cooldown_(cooldown) {}

bool DependencyGuard::breaker_open() const noexcept {
  std::lock_guard lock(mutex_);
  if (opened_at_ == std::chrono::steady_clock::time_point{}) return false;
  return std::chrono::steady_clock::now() - opened_at_ < cooldown_;
}

chaosproxy::Status DependencyGuard::Run(const std::function<chaosproxy::Status()>& operation) {
  {
    std::lock_guard lock(mutex_);
    if (opened_at_ != std::chrono::steady_clock::time_point{} &&
        std::chrono::steady_clock::now() - opened_at_ < cooldown_)
      return {chaosproxy::StatusCode::kUnavailable, "dependency circuit is open"};
    if (in_flight_ >= permit_limit_) return {chaosproxy::StatusCode::kResourceExhausted, "dependency permit limit reached"};
    ++in_flight_;
  }
  const chaosproxy::Status result = operation();
  std::lock_guard lock(mutex_);
  --in_flight_;
  if (result.ok()) { failures_ = 0; opened_at_ = {}; }
  else if (++failures_ >= failure_threshold_) opened_at_ = std::chrono::steady_clock::now();
  return result;
}

}  // namespace phototask
