#pragma once

#include "phototask/task_admission.h"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <string>

namespace phototask {

class KafkaConsumerCoordinator {
 public:
  explicit KafkaConsumerCoordinator(std::size_t queue_limit) : queue_limit_(queue_limit) {}
  void Assign(std::uint64_t assignment_epoch) noexcept { assignment_epoch_ = assignment_epoch; revoked_ = false; }
  void Revoke(std::uint64_t assignment_epoch) noexcept;
  chaosproxy::Status OnRecord(std::uint64_t assignment_epoch, KafkaRecord record);
  chaosproxy::Status AdmitReady(InboxLedger& ledger, std::int64_t& committed_offset);
  std::size_t QuarantineSize() const noexcept { return quarantine_.size(); }
  std::size_t QueueSize() const noexcept { return queue_.size(); }

 private:
  const std::size_t queue_limit_;
  std::uint64_t assignment_epoch_ = 0;
  bool revoked_ = true;
  std::deque<KafkaRecord> queue_;
  std::deque<KafkaRecord> quarantine_;
};

}  // namespace phototask
