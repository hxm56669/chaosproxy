#include "phototask/kafka_publisher.h"

#include <librdkafka/rdkafkacpp.h>

namespace phototask {

chaosproxy::StatusOr<std::unique_ptr<KafkaPublisher>> KafkaPublisher::Create(const KafkaRoute& route,
                                                                              std::string topic) {
  const auto valid = ValidateKafkaRoute(route);
  if (!valid.ok() || topic.empty()) return valid.ok() ? chaosproxy::Status(chaosproxy::StatusCode::kInvalidArgument, "Kafka topic is required") : valid;
  std::string error;
  std::unique_ptr<RdKafka::Conf> config(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (!config || config->set("bootstrap.servers", route.bootstrap_servers, error) != RdKafka::Conf::CONF_OK)
    return chaosproxy::Status(chaosproxy::StatusCode::kInvalidArgument, error.empty() ? "Kafka configuration failed" : error);
  std::unique_ptr<RdKafka::Producer> producer(RdKafka::Producer::create(config.get(), error));
  if (!producer) return chaosproxy::Status(chaosproxy::StatusCode::kUnavailable, error);
  return std::unique_ptr<KafkaPublisher>(new KafkaPublisher(std::move(producer), std::move(topic)));
}

KafkaPublisher::~KafkaPublisher() { if (producer_) producer_->flush(100); }

chaosproxy::StatusOr<PublishResult> KafkaPublisher::Publish(const std::string& event_id,
                                                             const std::string& payload) {
  if (event_id.empty() || payload.empty()) return chaosproxy::Status(chaosproxy::StatusCode::kInvalidArgument, "event id and payload are required");
  const auto result = producer_->produce(topic_, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
                                         const_cast<char*>(payload.data()), payload.size(), event_id.data(),
                                         event_id.size(), 0, nullptr);
  if (result != RdKafka::ERR_NO_ERROR) {
    last_state_ = PublishState::kFailed;
    return chaosproxy::Status(chaosproxy::StatusCode::kUnavailable, RdKafka::err2str(result));
  }
  last_state_ = PublishState::kEnqueued;
  return PublishResult{PublishState::kEnqueued, event_id};
}

PublishState KafkaPublisher::Poll(int timeout_ms) {
  producer_->poll(timeout_ms);
  if (producer_->outq_len() == 0) last_state_ = PublishState::kDelivered;
  return last_state_;
}

std::size_t KafkaPublisher::queued() const noexcept { return producer_->outq_len(); }

}  // namespace phototask
