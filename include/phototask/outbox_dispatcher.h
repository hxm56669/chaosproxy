#pragma once

#include "phototask/ports.h"

namespace phototask {

class OutboxDispatcher {
 public:
  explicit OutboxDispatcher(TaskRepository& repository) : repository_(repository) {}
  std::size_t Dispatch(std::size_t limit);

 private:
  TaskRepository& repository_;
};

}  // namespace phototask
