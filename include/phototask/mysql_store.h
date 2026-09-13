#pragma once

#include "chaosproxy/common/status.h"
#include "phototask/model/model.h"
#include "phototask/runtime_probe.h"

#include <memory>
#include <string>

namespace drogon::orm { class DbClient; }

namespace phototask {

class MysqlStore {
 public:
  explicit MysqlStore(MysqlOptions options = {});

  chaosproxy::Status InitializeSchema(const std::string& sql_path);
  chaosproxy::Status InsertImported(const PhotoAsset& asset);
  chaosproxy::StatusOr<PhotoAsset> FindPhotoBySha256(const std::string& sha256) const;

 private:
  MysqlOptions options_;
  std::shared_ptr<drogon::orm::DbClient> client_;
};

}  // namespace phototask
