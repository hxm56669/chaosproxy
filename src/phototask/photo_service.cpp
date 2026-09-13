#include "phototask/photo_service.h"

namespace phototask {

chaosproxy::StatusOr<PhotoAsset> PhotoService::Get(const PhotoId& id, bool strong) {
  if (!strong) {
    const auto cached = cache_.Get(id);
    if (cached.ok() && cached.value().has_value()) return *cached.value();
  }
  PhotoAsset result;
  const auto guarded = guard_.Run([&]() {
    const auto db = repository_.GetPhoto(id);
    if (!db.ok()) return db.status();
    result = db.value();
    return chaosproxy::Status::Ok();
  });
  if (!guarded.ok()) return guarded;
  if (!strong) (void)cache_.Put(result);
  return result;
}

chaosproxy::Status PhotoService::Update(const PhotoAsset& asset) {
  const auto stored = repository_.PutPhoto(asset);
  if (!stored.ok()) return stored;
  const auto invalidated = cache_.Invalidate(asset.id);
  if (!invalidated.ok()) pending_invalidations_.push_back(asset.id);
  return invalidated.ok() ? chaosproxy::Status::Ok() : stored;
}

chaosproxy::Status PhotoService::FlushInvalidations() {
  std::vector<PhotoId> remaining;
  for (const PhotoId& id : pending_invalidations_) {
    if (!cache_.Invalidate(id).ok()) remaining.push_back(id);
  }
  pending_invalidations_ = std::move(remaining);
  return pending_invalidations_.empty() ? chaosproxy::Status::Ok()
                                        : chaosproxy::Status(chaosproxy::StatusCode::kUnavailable, "invalidation outbox remains");
}

}  // namespace phototask
