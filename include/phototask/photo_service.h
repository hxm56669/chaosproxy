#pragma once

#include "phototask/dependency_guard.h"
#include "phototask/ports.h"
#include "phototask/redis_cache.h"

namespace phototask {

class PhotoService {
 public:
  PhotoService(TaskRepository& repository, RedisPhotoCache& cache)
      : repository_(repository), cache_(cache), guard_(4, 3, std::chrono::milliseconds(100)) {}
  chaosproxy::StatusOr<PhotoAsset> Get(const PhotoId& id, bool strong = false);
  chaosproxy::Status Update(const PhotoAsset& asset);
  chaosproxy::Status FlushInvalidations();
  std::size_t pending_invalidations() const noexcept { return pending_invalidations_.size(); }

 private:
  TaskRepository& repository_;
  RedisPhotoCache& cache_;
  DependencyGuard guard_;
  std::vector<PhotoId> pending_invalidations_;
};

}  // namespace phototask
