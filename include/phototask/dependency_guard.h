#pragma once

#include "chaosproxy/common/status.h"

#include <chrono>
#include <functional>
#include <mutex>

namespace phototask {

class DependencyGuard {
 public:
  DependencyGuard(std::size_t permit_limit, std::size_t failure_threshold,
                  std::chrono::milliseconds cooldown);
  chaosproxy::Status Run(const std::function<chaosproxy::Status()>& operation);
  bool breaker_open() const noexcept;

 private:
  const std::size_t permit_limit_;
  const std::size_t failure_threshold_;
  const std::chrono::milliseconds cooldown_;
  mutable std::mutex mutex_;
  std::size_t in_flight_ = 0;
  std::size_t failures_ = 0;
  std::chrono::steady_clock::time_point opened_at_{};
};

}  // namespace phototask
