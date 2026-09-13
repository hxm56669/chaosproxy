#include "phototask/mysql_store.h"

#include <drogon/orm/DbClient.h>

#include <fstream>
#include <sstream>

namespace phototask {
namespace {

std::string Quote(const std::string& value) {
  std::string escaped;
  escaped.reserve(value.size() + 2);
  escaped.push_back('\'');
  for (char c : value) {
    if (c == '\'') escaped += "''";
    else escaped.push_back(c);
  }
  escaped.push_back('\'');
  return escaped;
}

std::string ConnectionString(const MysqlOptions& options) {
  return "host=" + options.host + " port=" + std::to_string(options.port) +
         " dbname=" + options.database + " user=" + options.user + " password=" + options.password;
}

}  // namespace

MysqlStore::MysqlStore(MysqlOptions options) : options_(std::move(options)) {
  client_ = drogon::orm::DbClient::newMysqlClient(ConnectionString(options_), 1);
}

chaosproxy::Status MysqlStore::InitializeSchema(const std::string& sql_path) {
  std::ifstream input(sql_path);
  if (!input) return {chaosproxy::StatusCode::kNotFound, "schema file not found"};
  std::stringstream buffer;
  buffer << input.rdbuf();
  std::string statement;
  std::stringstream statements(buffer.str());
  while (std::getline(statements, statement, ';')) {
    if (statement.find_first_not_of(" \t\r\n") == std::string::npos) continue;
    try {
      client_->execSqlSync(statement);
    } catch (const std::exception& error) {
      return {chaosproxy::StatusCode::kUnavailable, error.what()};
    }
  }
  return chaosproxy::Status::Ok();
}

chaosproxy::Status MysqlStore::InsertImported(const PhotoAsset& asset) {
  const std::string sql = "INSERT INTO photos (id, original_path, sha256, bytes, width, height, metadata_json) VALUES (" +
      Quote(asset.id) + "," + Quote(asset.original_path) + "," + Quote(asset.sha256) + "," +
      std::to_string(asset.bytes) + "," + std::to_string(asset.width) + "," + std::to_string(asset.height) + "," +
      Quote(asset.metadata_json) + ") ON DUPLICATE KEY UPDATE id=id";
  try {
    client_->execSqlSync(sql);
  } catch (const std::exception& error) {
    return {chaosproxy::StatusCode::kUnavailable, error.what()};
  }
  return chaosproxy::Status::Ok();
}

chaosproxy::StatusOr<PhotoAsset> MysqlStore::FindPhotoBySha256(const std::string& sha256) const {
  try {
    const auto rows = client_->execSqlSync("SELECT id, original_path, sha256, bytes, width, height, metadata_json FROM photos WHERE sha256=" + Quote(sha256));
    if (rows.empty()) return chaosproxy::Status(chaosproxy::StatusCode::kNotFound, "photo not found");
    PhotoAsset asset;
    asset.id = rows[0]["id"].as<std::string>();
    asset.original_path = rows[0]["original_path"].as<std::string>();
    asset.sha256 = rows[0]["sha256"].as<std::string>();
    asset.bytes = rows[0]["bytes"].as<std::uint64_t>();
    asset.width = rows[0]["width"].as<std::uint32_t>();
    asset.height = rows[0]["height"].as<std::uint32_t>();
    asset.metadata_json = rows[0]["metadata_json"].as<std::string>();
    return asset;
  } catch (const std::exception& error) {
    return chaosproxy::Status(chaosproxy::StatusCode::kUnavailable, error.what());
  }
}

}  // namespace phototask
