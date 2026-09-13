#include "phototask/redis_cache.h"

#include <hiredis/hiredis.h>

#include <cstdlib>
#include <sstream>

namespace phototask {

RedisPhotoCache::RedisPhotoCache(RedisOptions options) : options_(std::move(options)) {
  const timeval timeout{1, 0};
  context_ = redisConnectWithTimeout(options_.host.c_str(), options_.port, timeout);
  if (context_ != nullptr && static_cast<redisContext*>(context_)->err != 0) {
    redisFree(static_cast<redisContext*>(context_));
    context_ = nullptr;
  }
}

RedisPhotoCache::~RedisPhotoCache() {
  if (context_ != nullptr) redisFree(static_cast<redisContext*>(context_));
}

chaosproxy::Status RedisPhotoCache::Ping() const {
  if (!available_ || context_ == nullptr) return {chaosproxy::StatusCode::kUnavailable, "redis unavailable"};
  redisReply* reply = static_cast<redisReply*>(redisCommand(static_cast<redisContext*>(context_), "PING"));
  if (reply == nullptr) return {chaosproxy::StatusCode::kUnavailable, "redis ping failed"};
  const bool ok = reply->type == REDIS_REPLY_STATUS && std::string(reply->str, reply->len) == "PONG";
  freeReplyObject(reply);
  return ok ? chaosproxy::Status::Ok() : chaosproxy::Status(chaosproxy::StatusCode::kUnavailable, "redis ping rejected");
}

chaosproxy::StatusOr<std::optional<PhotoAsset>> RedisPhotoCache::Get(const PhotoId& id) const {
  if (!available_ || context_ == nullptr) return chaosproxy::Status(chaosproxy::StatusCode::kUnavailable, "redis unavailable");
  redisReply* reply = static_cast<redisReply*>(redisCommand(static_cast<redisContext*>(context_), "GET phototask:%s", id.c_str()));
  if (reply == nullptr) return chaosproxy::Status(chaosproxy::StatusCode::kUnavailable, "redis get failed");
  if (reply->type == REDIS_REPLY_NIL) { freeReplyObject(reply); return std::optional<PhotoAsset>{}; }
  if (reply->type != REDIS_REPLY_STRING) { freeReplyObject(reply); return chaosproxy::Status(chaosproxy::StatusCode::kUnavailable, "redis returned invalid value"); }
  std::istringstream value(std::string(reply->str, reply->len));
  PhotoAsset asset;
  std::getline(value, asset.id, '|');
  std::getline(value, asset.original_path, '|');
  std::getline(value, asset.sha256, '|');
  std::string bytes;
  std::getline(value, bytes, '|');
  asset.bytes = std::strtoull(bytes.c_str(), nullptr, 10);
  std::string width;
  std::getline(value, width, '|');
  asset.width = static_cast<std::uint32_t>(std::strtoul(width.c_str(), nullptr, 10));
  std::string height;
  std::getline(value, height, '|');
  asset.height = static_cast<std::uint32_t>(std::strtoul(height.c_str(), nullptr, 10));
  std::getline(value, asset.metadata_json);
  freeReplyObject(reply);
  return std::optional<PhotoAsset>(std::move(asset));
}

chaosproxy::Status RedisPhotoCache::Put(const PhotoAsset& asset) {
  if (!available_ || context_ == nullptr) return {chaosproxy::StatusCode::kUnavailable, "redis unavailable"};
  const std::string serialized = asset.id + "|" + asset.original_path + "|" + asset.sha256 + "|" +
      std::to_string(asset.bytes) + "|" + std::to_string(asset.width) + "|" + std::to_string(asset.height) + "|" + asset.metadata_json;
  redisReply* reply = static_cast<redisReply*>(redisCommand(static_cast<redisContext*>(context_),
                                                            "SETEX phototask:%s %d %b", asset.id.c_str(), options_.ttl_seconds,
                                                            serialized.data(), serialized.size()));
  if (reply == nullptr) return {chaosproxy::StatusCode::kUnavailable, "redis put failed"};
  const bool ok = reply->type == REDIS_REPLY_STATUS && std::string(reply->str, reply->len) == "OK";
  freeReplyObject(reply);
  return ok ? chaosproxy::Status::Ok() : chaosproxy::Status(chaosproxy::StatusCode::kUnavailable, "redis put rejected");
}

chaosproxy::Status RedisPhotoCache::Invalidate(const PhotoId& id) {
  if (!available_ || context_ == nullptr) return {chaosproxy::StatusCode::kUnavailable, "redis unavailable"};
  redisReply* reply = static_cast<redisReply*>(redisCommand(static_cast<redisContext*>(context_), "DEL phototask:%s", id.c_str()));
  if (reply == nullptr) return {chaosproxy::StatusCode::kUnavailable, "redis invalidate failed"};
  freeReplyObject(reply);
  return chaosproxy::Status::Ok();
}

}  // namespace phototask
