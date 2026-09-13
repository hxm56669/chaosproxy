#include "phototask/task_admission.h"

namespace phototask {

chaosproxy::Status InboxLedger::Admit(const KafkaRecord& record) {
  if (record.event_id.empty() || record.task_id.empty() || record.partition < 0 || record.offset < 0)
    return {chaosproxy::StatusCode::kInvalidArgument, "malformed Kafka record"};
  const auto existing = event_ids_.find(record.event_id);
  if (existing != event_ids_.end()) return existing->second.task_id == record.task_id ? chaosproxy::Status::Ok() : chaosproxy::Status(chaosproxy::StatusCode::kConflict, "event id reused for another task");
  event_ids_.emplace(record.event_id, record);
  admitted_[record.partition][record.offset] = record.event_id;
  return chaosproxy::Status::Ok();
}

bool InboxLedger::Contains(const std::string& event_id) const { return event_ids_.find(event_id) != event_ids_.end(); }

chaosproxy::Status InboxLedger::CommitContiguous(std::int32_t partition, std::int64_t& committed_offset) {
  std::int64_t next = committed_.contains(partition) ? committed_[partition] + 1 : 0;
  const auto& offsets = admitted_[partition];
  while (offsets.contains(next)) ++next;
  committed_offset = next - 1;
  committed_[partition] = committed_offset;
  return chaosproxy::Status::Ok();
}

}  // namespace phototask
