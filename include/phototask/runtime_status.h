#pragma once

#include <atomic>
#include <cstdint>
#include <string>

namespace phototask {

class RuntimeStatus {
 public:
  void SetReady(bool ready) noexcept { ready_.store(ready); }
  void CountRequest() noexcept { requests_.fetch_add(1); }
  std::string Livez() const { return "ok\n"; }
  std::string Readyz() const { return ready_.load() ? "ready\n" : "not ready\n"; }
  std::string Metrics() const;

 private:
  std::atomic<bool> ready_{false};
  std::atomic<std::uint64_t> requests_{0};
};

}  // namespace phototask
