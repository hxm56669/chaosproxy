#pragma once

#include "phototask/filesystem_store.h"
#include "phototask/image_processors.h"
#include "phototask/ports.h"

namespace phototask {

class TaskExecutor {
 public:
  TaskExecutor(TaskRepository& repository, FileSystemStore& files)
      : repository_(repository), files_(files) {}
  chaosproxy::Status ExecuteOne(const std::string& owner, Clock::time_point now,
                                std::chrono::seconds lease = std::chrono::seconds(30));

 private:
  TaskRepository& repository_;
  FileSystemStore& files_;
};

}  // namespace phototask
