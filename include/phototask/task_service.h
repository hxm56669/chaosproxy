#pragma once

#include "phototask/ports.h"

namespace phototask {

struct HttpResponse { int status = 500; std::string body; };

class TaskService {
 public:
  explicit TaskService(TaskRepository& repository) : repository_(repository) {}
  chaosproxy::StatusOr<TaskRecord> Create(TaskSpec spec);
  chaosproxy::StatusOr<TaskRecord> Lookup(const TaskId& id) const { return repository_.GetTask(id); }
  void SimulateCommitUnknownOnce() noexcept { commit_unknown_once_ = true; }

 private:
  TaskRepository& repository_;
  bool commit_unknown_once_ = false;
};

class HttpHandlers {
 public:
  explicit HttpHandlers(TaskService& service) : service_(service) {}
  HttpResponse PostTask(TaskSpec spec);
  HttpResponse GetTask(const TaskId& id) const;

 private:
  TaskService& service_;
};

}  // namespace phototask
