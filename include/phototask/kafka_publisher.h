#pragma once

#include "chaosproxy/common/status.h"
#include "phototask/kafka_protocol.h"

#include <librdkafka/rdkafkacpp.h>

#include <memory>
#include <string>

namespace phototask {

enum class PublishState { kEnqueued, kDelivered, kFailed };
struct PublishResult { PublishState state; std::string event_id; };

class KafkaPublisher {
 public:
  static chaosproxy::StatusOr<std::unique_ptr<KafkaPublisher>> Create(const KafkaRoute& route,
                                                                       std::string topic);
  ~KafkaPublisher();
  chaosproxy::StatusOr<PublishResult> Publish(const std::string& event_id,
                                              const std::string& payload);
  PublishState Poll(int timeout_ms);
  std::size_t queued() const noexcept;

 private:
  KafkaPublisher(std::unique_ptr<RdKafka::Producer> producer, std::string topic)
      : producer_(std::move(producer)), topic_(std::move(topic)) {}
  std::unique_ptr<RdKafka::Producer> producer_;
  std::string topic_;
  PublishState last_state_ = PublishState::kEnqueued;
};

}  // namespace phototask
