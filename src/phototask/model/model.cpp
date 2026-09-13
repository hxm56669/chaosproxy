#include "phototask/model/model.h"

#include <openssl/sha.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <iomanip>
#include <sstream>

namespace phototask {
namespace {

std::string Trim(std::string value) {
  const auto first = std::find_if_not(value.begin(), value.end(),
                                      [](unsigned char c) { return std::isspace(c) != 0; });
  const auto last = std::find_if_not(value.rbegin(), value.rend(),
                                     [](unsigned char c) { return std::isspace(c) != 0; }).base();
  if (first >= last) return {};
  return std::string(first, last);
}

std::string Lower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}

std::string Sha256(std::string_view input) {
  std::array<unsigned char, SHA256_DIGEST_LENGTH> digest{};
  SHA256(reinterpret_cast<const unsigned char*>(input.data()), input.size(), digest.data());
  std::ostringstream out;
  out << std::hex << std::setfill('0');
  for (unsigned char byte : digest) out << std::setw(2) << static_cast<unsigned int>(byte);
  return out.str();
}

}  // namespace

chaosproxy::StatusOr<NormalizedTaskSpec> NormalizeTaskSpec(TaskSpec spec) {
  spec.request_key = Trim(std::move(spec.request_key));
  spec.photo_id = Trim(std::move(spec.photo_id));
  if (spec.request_key.empty() || spec.photo_id.empty()) {
    return chaosproxy::Status(chaosproxy::StatusCode::kInvalidArgument,
                              "request_key and photo_id are required");
  }
  if (spec.max_width == 0 || spec.max_height == 0 || spec.max_width > 4096 ||
      spec.max_height > 4096) {
    return chaosproxy::Status(chaosproxy::StatusCode::kInvalidArgument,
                              "thumbnail dimensions are outside the allowed range");
  }
  for (std::string& operation : spec.operations) {
    operation = Lower(Trim(std::move(operation)));
    if (operation != "thumbnail" && operation != "extract_metadata") {
      return chaosproxy::Status(chaosproxy::StatusCode::kInvalidArgument,
                                "unsupported photo operation");
    }
  }
  if (spec.operations.empty()) spec.operations.push_back("thumbnail");
  std::sort(spec.operations.begin(), spec.operations.end());
  spec.operations.erase(std::unique(spec.operations.begin(), spec.operations.end()),
                        spec.operations.end());

  std::ostringstream canonical;
  canonical << "v1|photo=" << spec.photo_id << "|size=" << spec.max_width << 'x'
            << spec.max_height << "|ops=";
  for (std::size_t i = 0; i < spec.operations.size(); ++i) {
    if (i != 0) canonical << ',';
    canonical << spec.operations[i];
  }
  NormalizedTaskSpec result{std::move(spec), canonical.str(), {}};
  result.fingerprint = FingerprintV1(result);
  return result;
}

std::string FingerprintV1(const NormalizedTaskSpec& spec) {
  return "sha256:" + Sha256(spec.canonical);
}

std::string TaskStateName(TaskState state) {
  switch (state) {
    case TaskState::kPendingDispatch: return "PENDING_DISPATCH";
    case TaskState::kReady: return "READY";
    case TaskState::kRunning: return "RUNNING";
    case TaskState::kSucceeded: return "SUCCEEDED";
    case TaskState::kRetryable: return "RETRYABLE";
    case TaskState::kFailed: return "FAILED";
  }
  return "UNKNOWN";
}

bool IsValidTransition(TaskState from, TaskState to) {
  if (from == to) return true;
  if (from == TaskState::kPendingDispatch && to == TaskState::kReady) return true;
  if (from == TaskState::kReady && to == TaskState::kRunning) return true;
  if (from == TaskState::kRunning &&
      (to == TaskState::kSucceeded || to == TaskState::kRetryable || to == TaskState::kFailed))
    return true;
  if (from == TaskState::kRetryable && to == TaskState::kReady) return true;
  return false;
}

}  // namespace phototask
