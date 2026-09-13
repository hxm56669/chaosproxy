#pragma once

#include "chaosproxy/common/status_or.h"

#include <cstddef>
#include <cstdint>
#include <string>

namespace phototask {

struct MysqlOptions {
    std::string host = "127.0.0.1";
    std::uint16_t port = 3306;
    std::string database = "phototask_test";
    std::string user = "phototask";
    std::string password;
};

struct MysqlProbeResult {
    bool connected = false;
    bool commit_callback_received = false;
    bool commit_succeeded = false;
    std::size_t committed_rows = 0;
};

chaosproxy::StatusOr<MysqlProbeResult> RunMysqlTransactionProbe(
    const MysqlOptions& options);

}  // namespace phototask
