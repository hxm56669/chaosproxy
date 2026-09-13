#include "phototask/runtime_probe.h"

#include <drogon/orm/DbClient.h>

#include <string>

namespace phototask {

chaosproxy::StatusOr<MysqlProbeResult> RunMysqlTransactionProbe(
    const MysqlOptions& options) {
    MysqlProbeResult result;
    const std::string connection =
        "host=" + options.host + " port=" + std::to_string(options.port) +
        " dbname=" + options.database + " user=" + options.user +
        " password=" + options.password;
    try {
        auto client = drogon::orm::DbClient::newMysqlClient(connection, 1);
        const auto row = client->execSqlSync("SELECT 1 AS connected");
        result.connected = !row.empty() && row[0]["connected"].as<int>() == 1;
        auto transaction = client->newTransaction([&result](bool committed) {
            result.commit_callback_received = true;
            result.commit_succeeded = committed;
        });
        transaction->execSqlSync(
            "CREATE TABLE IF NOT EXISTS b0_transaction_probe "
            "(id INT PRIMARY KEY, marker VARCHAR(32) NOT NULL)");
        transaction->execSqlSync(
            "INSERT INTO b0_transaction_probe(id, marker) VALUES(1, 'committed') "
            "ON DUPLICATE KEY UPDATE marker=VALUES(marker)");
        transaction.reset();
        const auto committed = client->execSqlSync(
            "SELECT COUNT(*) AS rows_count FROM b0_transaction_probe WHERE id=1");
        result.committed_rows = committed[0]["rows_count"].as<std::size_t>();
        return result;
    } catch (const std::exception& error) {
        return chaosproxy::Status(chaosproxy::StatusCode::kUnavailable,
                                  std::string("MariaDB probe failed: ") + error.what());
    }
}

}  // namespace phototask
