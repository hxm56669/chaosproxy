#include "phototask/dependency_guard.h"
#include "phototask/photo_service.h"

#include <gtest/gtest.h>

#include <thread>

namespace {

phototask::PhotoAsset Asset(const std::string& id) {
  return phototask::PhotoAsset{id, "originals/a.ppm", std::string(64, 'b'), 10, 2, 2, "{\"v\":1}"};
}

TEST(C1RedisTest, PingPutGetAndInvalidateUseRealRedis) {
  phototask::RedisPhotoCache cache;
  ASSERT_TRUE(cache.Ping().ok());
  const auto asset = Asset("cache-c1");
  ASSERT_TRUE(cache.Put(asset).ok());
  const auto hit = cache.Get(asset.id);
  ASSERT_TRUE(hit.ok());
  ASSERT_TRUE(hit.value().has_value());
  EXPECT_EQ(hit.value()->sha256, asset.sha256);
  ASSERT_TRUE(cache.Invalidate(asset.id).ok());
  ASSERT_TRUE(cache.Get(asset.id).ok());
  EXPECT_FALSE(cache.Get(asset.id).value().has_value());
}

TEST(C2GuardTest, OpensAfterFailuresAndRecoversAfterCooldown) {
  phototask::DependencyGuard guard(1, 2, std::chrono::milliseconds(10));
  auto fail = [] { return chaosproxy::Status(chaosproxy::StatusCode::kUnavailable, "down"); };
  EXPECT_FALSE(guard.Run(fail).ok());
  EXPECT_FALSE(guard.Run(fail).ok());
  EXPECT_TRUE(guard.breaker_open());
  EXPECT_EQ(guard.Run([] { return chaosproxy::Status::Ok(); }).code(), chaosproxy::StatusCode::kUnavailable);
  std::this_thread::sleep_for(std::chrono::milliseconds(15));
  EXPECT_TRUE(guard.Run([] { return chaosproxy::Status::Ok(); }).ok());
}

TEST(C3InvalidationTest, FailedDeleteIsRetainedAndFlushedAfterRecovery) {
  phototask::TaskRepository repository;
  const auto asset = Asset("cache-c3");
  ASSERT_TRUE(repository.PutPhoto(asset).ok());
  phototask::RedisPhotoCache cache;
  phototask::PhotoService service(repository, cache);
  ASSERT_TRUE(service.Get(asset.id).ok());
  cache.SetAvailable(false);
  EXPECT_TRUE(service.Update(asset).ok());
  EXPECT_EQ(service.pending_invalidations(), 1U);
  cache.SetAvailable(true);
  EXPECT_TRUE(service.FlushInvalidations().ok());
  EXPECT_EQ(service.pending_invalidations(), 0U);
}

TEST(C4CacheTest, StrongReadBypassesCacheAndWinsStaleReadRace) {
  phototask::TaskRepository repository;
  auto old_asset = Asset("cache-c4");
  ASSERT_TRUE(repository.PutPhoto(old_asset).ok());
  phototask::RedisPhotoCache cache;
  phototask::PhotoService service(repository, cache);
  ASSERT_TRUE(service.Get(old_asset.id).ok());
  auto fresh_asset = old_asset;
  fresh_asset.metadata_json = "{\"v\":2}";
  ASSERT_TRUE(service.Update(fresh_asset).ok());
  const auto strong = service.Get(old_asset.id, true);
  ASSERT_TRUE(strong.ok());
  EXPECT_EQ(strong.value().metadata_json, fresh_asset.metadata_json);
}

}  // namespace
