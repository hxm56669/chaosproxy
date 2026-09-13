#pragma once

#include "chaosproxy/common/status_or.h"
#include "phototask/model/model.h"

#include <filesystem>
#include <string>

namespace phototask {

class FileSystemStore {
 public:
  explicit FileSystemStore(std::filesystem::path root);

  chaosproxy::StatusOr<PhotoAsset> Import(const std::filesystem::path& source);
  chaosproxy::StatusOr<std::string> Read(const std::string& relative_path) const;
  const std::filesystem::path& root() const noexcept { return root_; }

 private:
  std::filesystem::path root_;
};

}  // namespace phototask
