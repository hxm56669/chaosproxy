#include "phototask/runtime_status.h"

#include <sstream>

namespace phototask {

std::string RuntimeStatus::Metrics() const {
  std::ostringstream output;
  output << "phototask_requests_total " << requests_.load() << '\n';
  output << "phototask_ready " << (ready_.load() ? 1 : 0) << '\n';
  return output.str();
}

}  // namespace phototask
