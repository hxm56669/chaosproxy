#pragma once

#include "chaosproxy/common/status.h"

#include <cstddef>
#include <filesystem>
#include <string>

namespace chaosproxy {

struct LoggingOptions {
    std::string logger_name = "chaosproxy";
    std::filesystem::path file_path;
    std::size_t queue_size = 8192;
    bool enable_console = true;
};

Status InitLogging(const LoggingOptions& options);

}  // namespace chaosproxy
