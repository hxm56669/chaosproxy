#include "phototask/image_processors.h"
#include "phototask/outbox_dispatcher.h"
#include "phototask/task_executor.h"
#include "phototask/task_service.h"

#include <json/json.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <fstream>
#include <sstream>

namespace phototask {
namespace {

TaskRecord* Find(std::vector<TaskRecord>& tasks, const TaskId& id) {
  const auto it = std::find_if(tasks.begin(), tasks.end(), [&](const TaskRecord& task) { return task.id == id; });
  return it == tasks.end() ? nullptr : &*it;
}

const TaskRecord* Find(const std::vector<TaskRecord>& tasks, const TaskId& id) {
  const auto it = std::find_if(tasks.begin(), tasks.end(), [&](const TaskRecord& task) { return task.id == id; });
  return it == tasks.end() ? nullptr : &*it;
}

}  // namespace

chaosproxy::Status TaskRepository::PutPhoto(PhotoAsset asset) {
  std::lock_guard lock(mutex_);
  const auto it = std::find_if(photos_.begin(), photos_.end(), [&](const PhotoAsset& photo) { return photo.id == asset.id; });
  if (it != photos_.end()) { *it = std::move(asset); return chaosproxy::Status::Ok(); }
  photos_.push_back(std::move(asset));
  return chaosproxy::Status::Ok();
}

chaosproxy::StatusOr<PhotoAsset> TaskRepository::GetPhoto(const PhotoId& id) const {
  std::lock_guard lock(mutex_);
  const auto it = std::find_if(photos_.begin(), photos_.end(), [&](const PhotoAsset& photo) { return photo.id == id; });
  if (it == photos_.end()) return chaosproxy::Status(chaosproxy::StatusCode::kNotFound, "photo not found");
  return *it;
}

chaosproxy::StatusOr<CreateTaskResult> TaskRepository::CreateAtomic(const NormalizedTaskSpec& spec,
                                                                     bool commit_unknown) {
  std::lock_guard lock(mutex_);
  for (const TaskRecord& existing : tasks_) {
    if (existing.request_key != spec.spec.request_key) continue;
    if (existing.fingerprint != spec.fingerprint) {
      return chaosproxy::Status(chaosproxy::StatusCode::kConflict, "request key has a different fingerprint");
    }
    return CreateTaskResult{existing, false};
  }
  TaskRecord task;
  task.id = "task-" + spec.fingerprint.substr(spec.fingerprint.size() - 24);
  task.request_key = spec.spec.request_key;
  task.fingerprint = spec.fingerprint;
  task.photo_id = spec.spec.photo_id;
  tasks_.push_back(task);
  ++outbox_pending_;
  if (commit_unknown) {
    return chaosproxy::Status(chaosproxy::StatusCode::kCommitUnknown, "commit result was intentionally hidden");
  }
  return CreateTaskResult{task, true};
}

chaosproxy::StatusOr<TaskRecord> TaskRepository::FindRequest(const std::string& request_key) const {
  std::lock_guard lock(mutex_);
  const auto it = std::find_if(tasks_.begin(), tasks_.end(), [&](const TaskRecord& task) { return task.request_key == request_key; });
  if (it == tasks_.end()) return chaosproxy::Status(chaosproxy::StatusCode::kNotFound, "request key not found");
  return *it;
}

std::vector<TaskRecord> TaskRepository::ClaimDispatchBatch(std::size_t limit) {
  std::lock_guard lock(mutex_);
  std::vector<TaskRecord> claimed;
  for (TaskRecord& task : tasks_) {
    if (claimed.size() >= limit) break;
    if (task.state != TaskState::kPendingDispatch) continue;
    task.state = TaskState::kReady;
    if (outbox_pending_ > 0) --outbox_pending_;
    claimed.push_back(task);
  }
  return claimed;
}

chaosproxy::StatusOr<TaskRecord> TaskRepository::ClaimReady(const std::string& owner,
                                                            Clock::time_point now,
                                                            std::chrono::seconds lease) {
  std::lock_guard lock(mutex_);
  for (TaskRecord& task : tasks_) {
    if ((task.state != TaskState::kReady && task.state != TaskState::kRetryable) || task.available_at > now) continue;
    task.state = TaskState::kRunning;
    task.owner = owner;
    ++task.epoch;
    ++task.attempt;
    task.lease_until = now + lease;
    return task;
  }
  return chaosproxy::Status(chaosproxy::StatusCode::kNotFound, "no ready task");
}

chaosproxy::Status TaskRepository::Renew(const TaskId& id, const std::string& owner, std::uint64_t epoch,
                                         Clock::time_point now, std::chrono::seconds lease) {
  std::lock_guard lock(mutex_);
  TaskRecord* task = Find(tasks_, id);
  if (task == nullptr || task->state != TaskState::kRunning || task->owner != owner || task->epoch != epoch)
    return {chaosproxy::StatusCode::kStaleOwner, "lease owner or epoch is stale"};
  task->lease_until = now + lease;
  return chaosproxy::Status::Ok();
}

chaosproxy::Status TaskRepository::ScheduleRetry(const TaskId& id, const std::string& owner, std::uint64_t epoch,
                                                  Clock::time_point now, std::chrono::seconds delay,
                                                  std::string error) {
  std::lock_guard lock(mutex_);
  TaskRecord* task = Find(tasks_, id);
  if (task == nullptr || task->state != TaskState::kRunning || task->owner != owner || task->epoch != epoch)
    return {chaosproxy::StatusCode::kStaleOwner, "lease owner or epoch is stale"};
  task->state = TaskState::kRetryable;
  task->owner.clear();
  task->available_at = now + delay;
  task->last_error = std::move(error);
  return chaosproxy::Status::Ok();
}

std::size_t TaskRepository::RequeueExpired(Clock::time_point now) {
  std::lock_guard lock(mutex_);
  std::size_t count = 0;
  for (TaskRecord& task : tasks_) {
    if (task.state == TaskState::kRunning && task.lease_until <= now) {
      task.state = TaskState::kRetryable;
      task.owner.clear();
      task.available_at = now;
      ++count;
    }
  }
  return count;
}

chaosproxy::Status TaskRepository::Complete(const TaskId& id, const std::string& owner, std::uint64_t epoch,
                                            std::string output_path) {
  std::lock_guard lock(mutex_);
  TaskRecord* task = Find(tasks_, id);
  if (task == nullptr || task->state != TaskState::kRunning || task->owner != owner || task->epoch != epoch)
    return {chaosproxy::StatusCode::kStaleOwner, "completion owner or epoch is stale"};
  task->state = TaskState::kSucceeded;
  task->output_path = std::move(output_path);
  task->owner.clear();
  return chaosproxy::Status::Ok();
}

chaosproxy::StatusOr<TaskRecord> TaskRepository::GetTask(const TaskId& id) const {
  std::lock_guard lock(mutex_);
  const TaskRecord* task = Find(tasks_, id);
  if (task == nullptr) return chaosproxy::Status(chaosproxy::StatusCode::kNotFound, "task not found");
  return *task;
}

std::size_t TaskRepository::outbox_pending() const { std::lock_guard lock(mutex_); return outbox_pending_; }

chaosproxy::StatusOr<TaskRecord> TaskService::Create(TaskSpec spec) {
  auto normalized = NormalizeTaskSpec(std::move(spec));
  if (!normalized.ok()) return normalized.status();
  auto created = repository_.CreateAtomic(normalized.value(), commit_unknown_once_);
  commit_unknown_once_ = false;
  if (created.ok()) return created.value().task;
  if (created.status().code() != chaosproxy::StatusCode::kCommitUnknown) return created.status();
  return repository_.FindRequest(normalized.value().spec.request_key);
}

HttpResponse HttpHandlers::PostTask(TaskSpec spec) {
  const auto task = service_.Create(std::move(spec));
  if (!task.ok()) {
    const int code = task.status().code() == chaosproxy::StatusCode::kConflict ? 409 :
                     task.status().code() == chaosproxy::StatusCode::kInvalidArgument ? 400 : 503;
    return {code, task.status().message()};
  }
  return {202, "{\"task_id\":\"" + task.value().id + "\",\"state\":\"" +
                    TaskStateName(task.value().state) + "\"}"};
}

HttpResponse HttpHandlers::GetTask(const TaskId& id) const {
  const auto task = service_.Lookup(id);
  if (!task.ok()) return {404, task.status().message()};
  return {200, "{\"task_id\":\"" + task.value().id + "\",\"state\":\"" +
                    TaskStateName(task.value().state) + "\"}"};
}

std::size_t OutboxDispatcher::Dispatch(std::size_t limit) { return repository_.ClaimDispatchBatch(limit).size(); }

chaosproxy::StatusOr<ImageData> ImageProcessors::Thumbnail(const std::string& input,
                                                            std::uint32_t max_width,
                                                            std::uint32_t max_height) {
  if (input.rfind("P6\n", 0) != 0)
    return chaosproxy::Status(chaosproxy::StatusCode::kInvalidArgument, "only binary PPM P6 is supported");
  const std::size_t header_end = input.find("\n255\n", 3);
  if (header_end == std::string::npos)
    return chaosproxy::Status(chaosproxy::StatusCode::kInvalidArgument, "invalid PPM header");
  std::istringstream header(input.substr(3, header_end - 3));
  std::uint32_t width = 0;
  std::uint32_t height = 0;
  header >> width >> height;
  const std::size_t pixels_at = header_end + 5;
  if (width == 0 || height == 0 || input.size() - pixels_at != static_cast<std::size_t>(width) * height * 3U)
    return chaosproxy::Status(chaosproxy::StatusCode::kInvalidArgument, "invalid PPM pixel payload");
  const double scale = std::min(1.0, std::min(static_cast<double>(max_width) / width,
                                               static_cast<double>(max_height) / height));
  const std::uint32_t out_width = std::max(1U, static_cast<std::uint32_t>(std::floor(width * scale)));
  const std::uint32_t out_height = std::max(1U, static_cast<std::uint32_t>(std::floor(height * scale)));
  std::string output = "P6\n" + std::to_string(out_width) + " " + std::to_string(out_height) + "\n255\n";
  const std::size_t output_pixels = static_cast<std::size_t>(out_width) * out_height * 3U;
  const std::size_t output_start = output.size();
  output.resize(output_start + output_pixels);
  for (std::uint32_t y = 0; y < out_height; ++y) {
    for (std::uint32_t x = 0; x < out_width; ++x) {
      const std::uint32_t source_x = x * width / out_width;
      const std::uint32_t source_y = y * height / out_height;
      const std::size_t source = pixels_at + (static_cast<std::size_t>(source_y) * width + source_x) * 3U;
      const std::size_t target = output_start + (static_cast<std::size_t>(y) * out_width + x) * 3U;
      std::memcpy(output.data() + target, input.data() + source, 3U);
    }
  }
  return ImageData{std::move(output), out_width, out_height};
}

chaosproxy::StatusOr<std::string> ImageProcessors::ExtractMetadata(const ImageData& image) {
  Json::Value value;
  value["width"] = image.width;
  value["height"] = image.height;
  value["format"] = "ppm";
  Json::StreamWriterBuilder builder;
  return Json::writeString(builder, value);
}

chaosproxy::Status TaskExecutor::ExecuteOne(const std::string& owner, Clock::time_point now,
                                            std::chrono::seconds lease) {
  const auto claim = repository_.ClaimReady(owner, now, lease);
  if (!claim.ok()) return claim.status();
  const auto photo = repository_.GetPhoto(claim.value().photo_id);
  if (!photo.ok()) return repository_.ScheduleRetry(claim.value().id, owner, claim.value().epoch, now,
                                                     std::chrono::seconds(1), photo.status().message());
  const auto input = files_.Read(photo.value().original_path);
  if (!input.ok()) return repository_.ScheduleRetry(claim.value().id, owner, claim.value().epoch, now,
                                                     std::chrono::seconds(1), input.status().message());
  const auto image = ImageProcessors::Thumbnail(input.value(), 256, 256);
  if (!image.ok()) return repository_.ScheduleRetry(claim.value().id, owner, claim.value().epoch, now,
                                                     std::chrono::seconds(1), image.status().message());
  const std::filesystem::path output = files_.root() / "attempts" / claim.value().id /
                                       std::to_string(claim.value().attempt) / "thumbnail.ppm";
  std::filesystem::create_directories(output.parent_path());
  std::ofstream file(output, std::ios::binary | std::ios::trunc);
  file.write(image.value().bytes.data(), static_cast<std::streamsize>(image.value().bytes.size()));
  if (!file) return repository_.ScheduleRetry(claim.value().id, owner, claim.value().epoch, now,
                                               std::chrono::seconds(1), "cannot publish attempt");
  const std::string relative = std::filesystem::relative(output, files_.root()).generic_string();
  return repository_.Complete(claim.value().id, owner, claim.value().epoch, relative);
}

}  // namespace phototask
