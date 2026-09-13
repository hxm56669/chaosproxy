#pragma once

#include "chaosproxy/common/status_or.h"
#include "phototask/model/model.h"

#include <cstdint>
#include <optional>
#include <string>

namespace phototask {

struct RedisOptions { std::string host = "127.0.0.1"; std::uint16_t port = 6379; int ttl_seconds = 30; };

class RedisPhotoCache {
 public:
  explicit RedisPhotoCache(RedisOptions options = {});
  ~RedisPhotoCache();
  RedisPhotoCache(const RedisPhotoCache&) = delete;
  RedisPhotoCache& operator=(const RedisPhotoCache&) = delete;

  chaosproxy::Status Ping() const;
  chaosproxy::StatusOr<std::optional<PhotoAsset>> Get(const PhotoId& id) const;
  chaosproxy::Status Put(const PhotoAsset& asset);
  chaosproxy::Status Invalidate(const PhotoId& id);
  void SetAvailable(bool available) noexcept { available_ = available; }

 private:
  RedisOptions options_;
  mutable void* context_ = nullptr;
  bool available_ = true;
};

}  // namespace phototask
