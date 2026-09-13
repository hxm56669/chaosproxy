#pragma once

#include "chaosproxy/common/status_or.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace phototask {

using PhotoId = std::string;
using TaskId = std::string;

enum class TaskState {
  kPendingDispatch,
  kReady,
  kRunning,
  kSucceeded,
  kRetryable,
  kFailed,
};

struct PhotoAsset {
  PhotoId id;
  std::string original_path;
  std::string sha256;
  std::uint64_t bytes = 0;
  std::uint32_t width = 0;
  std::uint32_t height = 0;
  std::string metadata_json = "{}";
};

struct TaskSpec {
  std::string request_key;
  PhotoId photo_id;
  std::vector<std::string> operations;
  std::uint32_t max_width = 256;
  std::uint32_t max_height = 256;
};

struct NormalizedTaskSpec {
  TaskSpec spec;
  std::string canonical;
  std::string fingerprint;
};

chaosproxy::StatusOr<NormalizedTaskSpec> NormalizeTaskSpec(TaskSpec spec);
std::string FingerprintV1(const NormalizedTaskSpec& spec);
std::string TaskStateName(TaskState state);
bool IsValidTransition(TaskState from, TaskState to);

}  // namespace phototask
