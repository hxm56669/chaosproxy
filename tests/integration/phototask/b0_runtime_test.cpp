#include "phototask/runtime_probe.h"

#include "phototask/v1/task_event.pb.h"

#include <gtest/gtest.h>

namespace phototask {
namespace {

TEST(B0RuntimeTest, DrogonMysqlTransactionCommitIsObservable) {
    auto probe = RunMysqlTransactionProbe(MysqlOptions{});
    ASSERT_TRUE(probe.ok()) << probe.status().message();
    EXPECT_TRUE(probe.value().connected);
    EXPECT_TRUE(probe.value().commit_callback_received);
    EXPECT_TRUE(probe.value().commit_succeeded);
    EXPECT_EQ(probe.value().committed_rows, 1U);
}

TEST(B0RuntimeTest, ProtobufTaskEventRoundTrips) {
  phototask::v1::TaskEvent event;
  event.set_task_id("task-1");
  event.set_request_key("request-1");
  event.set_fingerprint("fp-1");
  event.set_state("PENDING_DISPATCH");
  event.set_sequence(7);

  std::string encoded;
  ASSERT_TRUE(event.SerializeToString(&encoded));
  phototask::v1::TaskEvent parsed;
  ASSERT_TRUE(parsed.ParseFromString(encoded));
  EXPECT_EQ(parsed.task_id(), "task-1");
  EXPECT_EQ(parsed.state(), "PENDING_DISPATCH");
  EXPECT_EQ(parsed.sequence(), 7);
}

}  // namespace
}  // namespace phototask
