#include "phototask/filesystem_store.h"
#include "phototask/image_processors.h"
#include "phototask/outbox_dispatcher.h"
#include "phototask/task_executor.h"
#include "phototask/task_service.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

namespace {

phototask::TaskSpec Spec(const std::string& key, const std::string& photo_id = "photo-pipeline") {
  return phototask::TaskSpec{key, photo_id, {"thumbnail"}, 2, 2};
}

TEST(B2TaskServiceTest, SameKeyIsIdempotentAndDifferentFingerprintConflicts) {
  phototask::TaskRepository repository;
  phototask::TaskService service(repository);
  const auto first = service.Create(Spec("key-1"));
  ASSERT_TRUE(first.ok());
  const auto second = service.Create(Spec("key-1"));
  ASSERT_TRUE(second.ok());
  EXPECT_EQ(first.value().id, second.value().id);
  auto changed = Spec("key-1");
  changed.max_width = 1;
  EXPECT_EQ(service.Create(changed).status().code(), chaosproxy::StatusCode::kConflict);
}

TEST(B2HttpTest, CreateReturnsAcceptedAndConflict) {
  phototask::TaskRepository repository;
  phototask::TaskService service(repository);
  phototask::HttpHandlers handlers(service);
  EXPECT_EQ(handlers.PostTask(Spec("http-1")).status, 202);
  auto changed = Spec("http-1");
  changed.max_height = 1;
  EXPECT_EQ(handlers.PostTask(changed).status, 409);
}

TEST(B3OutboxTest, DispatchMovesPendingTaskToReady) {
  phototask::TaskRepository repository;
  phototask::TaskService service(repository);
  ASSERT_TRUE(service.Create(Spec("dispatch-1")).ok());
  EXPECT_EQ(repository.outbox_pending(), 1U);
  phototask::OutboxDispatcher dispatcher(repository);
  EXPECT_EQ(dispatcher.Dispatch(10), 1U);
  EXPECT_EQ(repository.outbox_pending(), 0U);
  const auto claimed = repository.ClaimReady("worker", phototask::Clock::now(), std::chrono::seconds(10));
  ASSERT_TRUE(claimed.ok());
  EXPECT_EQ(claimed.value().state, phototask::TaskState::kRunning);
}

TEST(B4ImageTest, ThumbnailProducesSmallerRealImageAndMetadata) {
  const std::string input = "P6\n4 2\n255\n" + std::string(24, '\x7f');
  const auto image = phototask::ImageProcessors::Thumbnail(input, 2, 2);
  ASSERT_TRUE(image.ok()) << image.status().message();
  EXPECT_EQ(image.value().width, 2U);
  EXPECT_EQ(image.value().height, 1U);
  const auto metadata = phototask::ImageProcessors::ExtractMetadata(image.value());
  ASSERT_TRUE(metadata.ok());
  EXPECT_NE(metadata.value().find("\"width\""), std::string::npos);
}

TEST(B4ExecutorTest, LocalExecutorPublishesImmutableAttempt) {
  const auto root = std::filesystem::temp_directory_path() / "phototask-b4-store";
  std::filesystem::remove_all(root);
  const auto input = root.parent_path() / "phototask-b4-input.ppm";
  { std::ofstream file(input, std::ios::binary); file << "P6\n4 2\n255\n" << std::string(24, '\x55'); }
  phototask::FileSystemStore files(root);
  const auto photo = files.Import(input);
  ASSERT_TRUE(photo.ok());
  phototask::TaskRepository repository;
  ASSERT_TRUE(repository.PutPhoto(photo.value()).ok());
  phototask::TaskService service(repository);
  const auto task = service.Create(Spec("execute-1", photo.value().id));
  ASSERT_TRUE(task.ok());
  phototask::OutboxDispatcher(repository).Dispatch(1);
  phototask::TaskExecutor executor(repository, files);
  ASSERT_TRUE(executor.ExecuteOne("worker-a", phototask::Clock::now()).ok());
  const auto done = repository.GetTask(task.value().id);
  ASSERT_TRUE(done.ok());
  EXPECT_EQ(done.value().state, phototask::TaskState::kSucceeded) << done.value().last_error;
  const auto output = files.Read(done.value().output_path);
  ASSERT_TRUE(output.ok());
  EXPECT_EQ(output.value().substr(0, 3), "P6\n");
  std::filesystem::remove(input);
  std::filesystem::remove_all(root);
}

TEST(B5LeaseTest, EpochPreventsOldOwnerCompletionAndExpiredLeaseRequeues) {
  phototask::TaskRepository repository;
  phototask::TaskService service(repository);
  const auto task = service.Create(Spec("lease-1"));
  ASSERT_TRUE(task.ok());
  phototask::OutboxDispatcher(repository).Dispatch(1);
  const auto t0 = phototask::Clock::now();
  const auto first = repository.ClaimReady("owner-a", t0, std::chrono::seconds(1));
  ASSERT_TRUE(first.ok());
  EXPECT_EQ(repository.Renew(task.value().id, "owner-b", first.value().epoch, t0,
                             std::chrono::seconds(1)).code(), chaosproxy::StatusCode::kStaleOwner);
  EXPECT_EQ(repository.RequeueExpired(t0 + std::chrono::seconds(2)), 1U);
  const auto second = repository.ClaimReady("owner-b", t0 + std::chrono::seconds(2), std::chrono::seconds(1));
  ASSERT_TRUE(second.ok());
  EXPECT_GT(second.value().epoch, first.value().epoch);
  EXPECT_EQ(repository.Complete(task.value().id, "owner-a", first.value().epoch, "old").code(),
            chaosproxy::StatusCode::kStaleOwner);
}

TEST(B6CommitUnknownTest, RetryFindsCommittedTaskByOriginalRequestKey) {
  phototask::TaskRepository repository;
  phototask::TaskService service(repository);
  service.SimulateCommitUnknownOnce();
  const auto first = service.Create(Spec("unknown-1"));
  ASSERT_TRUE(first.ok()) << first.status().message();
  const auto second = service.Create(Spec("unknown-1"));
  ASSERT_TRUE(second.ok());
  EXPECT_EQ(first.value().id, second.value().id);
}

}  // namespace
