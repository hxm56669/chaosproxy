#pragma once

#include "chaosproxy/common/status_or.h"

#include <cstdint>
#include <string>

namespace phototask {

struct KafkaRoute {
  std::string logical_name;
  std::string bootstrap_servers;
  std::string advertised_endpoint;
};

chaosproxy::Status ValidateKafkaRoute(const KafkaRoute& route);

}  // namespace phototask
