#include "phototask/filesystem_store.h"

#include <openssl/sha.h>

#include <array>
#include <fstream>
#include <iomanip>
#include <sstream>

namespace phototask {
namespace {

std::string Hash(const std::string& bytes) {
  std::array<unsigned char, SHA256_DIGEST_LENGTH> digest{};
  SHA256(reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(), digest.data());
  std::ostringstream out;
  out << std::hex << std::setfill('0');
  for (unsigned char byte : digest) out << std::setw(2) << static_cast<unsigned int>(byte);
  return out.str();
}

bool Supported(std::string extension) {
  for (char& c : extension) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return extension == ".jpg" || extension == ".jpeg" || extension == ".png" || extension == ".ppm";
}

}  // namespace

FileSystemStore::FileSystemStore(std::filesystem::path root) : root_(std::move(root)) {}

chaosproxy::StatusOr<PhotoAsset> FileSystemStore::Import(const std::filesystem::path& source) {
  std::error_code error;
  if (!std::filesystem::is_regular_file(source, error)) {
    return chaosproxy::Status(chaosproxy::StatusCode::kNotFound, "photo input is not a regular file");
  }
  if (!Supported(source.extension().string())) {
    return chaosproxy::Status(chaosproxy::StatusCode::kInvalidArgument,
                              "only jpg, jpeg, png and ppm inputs are supported");
  }
  const auto size = std::filesystem::file_size(source, error);
  if (error || size == 0 || size > 64U * 1024U * 1024U) {
    return chaosproxy::Status(chaosproxy::StatusCode::kInvalidArgument,
                              "photo input is empty or exceeds the 64 MiB limit");
  }
  std::ifstream input(source, std::ios::binary);
  std::string bytes(static_cast<std::size_t>(size), '\0');
  input.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  if (!input) return chaosproxy::Status(chaosproxy::StatusCode::kIoError, "cannot read photo input");

  const std::string sha = Hash(bytes);
  const std::string extension = source.extension().string();
  const std::filesystem::path relative = std::filesystem::path("originals") / sha.substr(0, 2) /
                                         (sha + extension);
  const std::filesystem::path destination = root_ / relative;
  std::filesystem::create_directories(destination.parent_path(), error);
  if (error) return chaosproxy::Status(chaosproxy::StatusCode::kIoError, "cannot create photo directory");
  if (!std::filesystem::exists(destination)) {
    std::ofstream output(destination, std::ios::binary | std::ios::trunc);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    if (!output) return chaosproxy::Status(chaosproxy::StatusCode::kIoError, "cannot persist imported photo");
  }
  PhotoAsset asset;
  asset.id = "photo-" + sha.substr(0, 24);
  asset.original_path = relative.generic_string();
  asset.sha256 = sha;
  asset.bytes = size;
  return asset;
}

chaosproxy::StatusOr<std::string> FileSystemStore::Read(const std::string& relative_path) const {
  const auto path = (root_ / relative_path).lexically_normal();
  if (path.string().rfind(root_.lexically_normal().string(), 0) != 0) {
    return chaosproxy::Status(chaosproxy::StatusCode::kInvalidArgument, "path escapes photo root");
  }
  std::ifstream input(path, std::ios::binary);
  if (!input) return chaosproxy::Status(chaosproxy::StatusCode::kNotFound, "photo output not found");
  return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

}  // namespace phototask
