#include "phototask/kafka_consumer.h"
#include "phototask/kafka_protocol.h"
#include "phototask/kafka_publisher.h"

#include <gtest/gtest.h>

namespace {

phototask::KafkaRecord Record(const std::string& id, std::int64_t offset) {
  return phototask::KafkaRecord{id, "task-1", 0, offset, "payload"};
}

TEST(F1RouteTest, RejectsUnmappedOrMalformedAdvertisedEndpoint) {
  EXPECT_FALSE(phototask::ValidateKafkaRoute({"", "kafka-b1:9092", "kafka-b1:19092"}).ok());
  EXPECT_FALSE(phototask::ValidateKafkaRoute({"b1", "kafka-b1:9092", "kafka-b1"}).ok());
  EXPECT_TRUE(phototask::ValidateKafkaRoute({"b1", "kafka-b1:9092", "kafka-b1:19092"}).ok());
}

TEST(F2PublisherTest, EnqueueIsNotReportedAsDelivery) {
  const auto publisher = phototask::KafkaPublisher::Create({"b1", "127.0.0.1:1", "127.0.0.1:19092"}, "photo.events");
  ASSERT_TRUE(publisher.ok()) << publisher.status().message();
  const auto result = publisher.value()->Publish("event-1", "payload");
  if (result.ok()) {
    EXPECT_EQ(result.value().state, phototask::PublishState::kEnqueued);
    EXPECT_EQ(publisher.value()->Poll(0), phototask::PublishState::kEnqueued);
  } else {
    EXPECT_EQ(result.status().code(), chaosproxy::StatusCode::kUnavailable);
  }
}

TEST(F3AdmissionTest, InboxDeduplicatesAndOnlyCommitsContiguousOffsets) {
  phototask::InboxLedger ledger;
  ASSERT_TRUE(ledger.Admit(Record("e0", 0)).ok());
  ASSERT_TRUE(ledger.Admit(Record("e2", 2)).ok());
  ASSERT_TRUE(ledger.Admit(Record("e0", 0)).ok());
  std::int64_t offset = -1;
  ASSERT_TRUE(ledger.CommitContiguous(0, offset).ok());
  EXPECT_EQ(offset, 0);
  ASSERT_TRUE(ledger.Admit(Record("e1", 1)).ok());
  ASSERT_TRUE(ledger.CommitContiguous(0, offset).ok());
  EXPECT_EQ(offset, 2);
  EXPECT_EQ(ledger.size(), 3U);
}

TEST(F4ConsumerTest, StaleAssignmentCannotAdmitAndBadRecordsAreQuarantined) {
  phototask::KafkaConsumerCoordinator consumer(2);
  consumer.Assign(7);
  EXPECT_EQ(consumer.OnRecord(6, Record("old", 0)).code(), chaosproxy::StatusCode::kStaleOwner);
  EXPECT_EQ(consumer.OnRecord(7, Record("", 1)).code(), chaosproxy::StatusCode::kInvalidArgument);
  EXPECT_EQ(consumer.QuarantineSize(), 1U);
  ASSERT_TRUE(consumer.OnRecord(7, Record("good", 0)).ok());
  phototask::InboxLedger ledger;
  consumer.Revoke(8);
  std::int64_t offset = -1;
  EXPECT_EQ(consumer.AdmitReady(ledger, offset).code(), chaosproxy::StatusCode::kStaleOwner);
}

TEST(F5RecoveryTest, ReplayedEventIsOneLogicalInboxEntry) {
  phototask::InboxLedger ledger;
  const auto record = Record("same-event", 0);
  ASSERT_TRUE(ledger.Admit(record).ok());
  ASSERT_TRUE(ledger.Admit(record).ok());
  EXPECT_EQ(ledger.size(), 1U);
  EXPECT_TRUE(ledger.Contains("same-event"));
}

TEST(F6ConfigTest, ConsumerQueueIsBoundedDuringBacklog) {
  phototask::KafkaConsumerCoordinator consumer(1);
  consumer.Assign(1);
  ASSERT_TRUE(consumer.OnRecord(1, Record("e0", 0)).ok());
  EXPECT_EQ(consumer.OnRecord(1, Record("e1", 1)).code(), chaosproxy::StatusCode::kResourceExhausted);
}

}  // namespace
