#pragma once

#include "chaosproxy/common/status_or.h"
#include "phototask/model/model.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace phototask {

using Clock = std::chrono::steady_clock;

struct TaskRecord {
  TaskId id;
  std::string request_key;
  std::string fingerprint;
  PhotoId photo_id;
  TaskState state = TaskState::kPendingDispatch;
  std::uint32_t attempt = 0;
  std::string owner;
  std::uint64_t epoch = 0;
  Clock::time_point available_at{};
  Clock::time_point lease_until{};
  std::string last_error;
  std::string output_path;
};

struct CreateTaskResult { TaskRecord task; bool created = false; };

class TaskRepository {
 public:
  chaosproxy::Status PutPhoto(PhotoAsset asset);
  chaosproxy::StatusOr<PhotoAsset> GetPhoto(const PhotoId& id) const;
  chaosproxy::StatusOr<CreateTaskResult> CreateAtomic(const NormalizedTaskSpec& spec,
                                                       bool commit_unknown = false);
  chaosproxy::StatusOr<TaskRecord> FindRequest(const std::string& request_key) const;
  std::vector<TaskRecord> ClaimDispatchBatch(std::size_t limit);
  chaosproxy::StatusOr<TaskRecord> ClaimReady(const std::string& owner,
                                               Clock::time_point now,
                                               std::chrono::seconds lease);
  chaosproxy::Status Renew(const TaskId& id, const std::string& owner, std::uint64_t epoch,
                           Clock::time_point now, std::chrono::seconds lease);
  chaosproxy::Status ScheduleRetry(const TaskId& id, const std::string& owner, std::uint64_t epoch,
                                   Clock::time_point now, std::chrono::seconds delay,
                                   std::string error);
  std::size_t RequeueExpired(Clock::time_point now);
  chaosproxy::Status Complete(const TaskId& id, const std::string& owner, std::uint64_t epoch,
                              std::string output_path);
  chaosproxy::StatusOr<TaskRecord> GetTask(const TaskId& id) const;
  std::size_t outbox_pending() const;

 private:
  mutable std::mutex mutex_;
  std::vector<PhotoAsset> photos_;
  std::vector<TaskRecord> tasks_;
  std::size_t outbox_pending_ = 0;
};

}  // namespace phototask
