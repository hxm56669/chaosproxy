#include "phototask/kafka_protocol.h"

#include <cctype>

namespace phototask {

chaosproxy::Status ValidateKafkaRoute(const KafkaRoute& route) {
  if (route.logical_name.empty() || route.bootstrap_servers.empty() || route.advertised_endpoint.empty())
    return {chaosproxy::StatusCode::kInvalidArgument, "Kafka route fields are required"};
  if (route.advertised_endpoint.find(':') == std::string::npos)
    return {chaosproxy::StatusCode::kInvalidArgument, "Kafka advertised endpoint must contain host:port"};
  return chaosproxy::Status::Ok();
}

}  // namespace phototask
