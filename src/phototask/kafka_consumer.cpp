#include "phototask/kafka_consumer.h"

namespace phototask {

void KafkaConsumerCoordinator::Revoke(std::uint64_t assignment_epoch) noexcept {
  if (assignment_epoch >= assignment_epoch_) { assignment_epoch_ = assignment_epoch; revoked_ = true; }
}

chaosproxy::Status KafkaConsumerCoordinator::OnRecord(std::uint64_t assignment_epoch, KafkaRecord record) {
  if (revoked_ || assignment_epoch != assignment_epoch_)
    return {chaosproxy::StatusCode::kStaleOwner, "record belongs to a stale assignment"};
  if (record.event_id.empty() || record.task_id.empty()) {
    if (quarantine_.size() >= queue_limit_) return {chaosproxy::StatusCode::kResourceExhausted, "quarantine is full"};
    quarantine_.push_back(std::move(record));
    return {chaosproxy::StatusCode::kInvalidArgument, "record moved to quarantine"};
  }
  if (queue_.size() >= queue_limit_) return {chaosproxy::StatusCode::kResourceExhausted, "consumer queue is full"};
  queue_.push_back(std::move(record));
  return chaosproxy::Status::Ok();
}

chaosproxy::Status KafkaConsumerCoordinator::AdmitReady(InboxLedger& ledger, std::int64_t& committed_offset) {
  if (revoked_) return {chaosproxy::StatusCode::kStaleOwner, "assignment was revoked"};
  while (!queue_.empty()) {
    const auto record = std::move(queue_.front());
    queue_.pop_front();
    const auto admitted = ledger.Admit(record);
    if (!admitted.ok()) return admitted;
  }
  return ledger.CommitContiguous(0, committed_offset);
}

}  // namespace phototask
