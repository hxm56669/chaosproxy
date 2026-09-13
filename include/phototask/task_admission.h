#pragma once

#include "chaosproxy/common/status.h"

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace phototask {

struct KafkaRecord { std::string event_id; std::string task_id; std::int32_t partition = 0; std::int64_t offset = 0; std::string payload; };

class InboxLedger {
 public:
  chaosproxy::Status Admit(const KafkaRecord& record);
  bool Contains(const std::string& event_id) const;
  chaosproxy::Status CommitContiguous(std::int32_t partition, std::int64_t& committed_offset);
  std::size_t size() const noexcept { return event_ids_.size(); }

 private:
  std::map<std::int32_t, std::map<std::int64_t, std::string>> admitted_;
  std::map<std::int32_t, std::int64_t> committed_;
  std::map<std::string, KafkaRecord> event_ids_;
};

}  // namespace phototask
