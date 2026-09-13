#include "chaosproxy/common/logging.h"
#include "chaosproxy/common/status.h"
#include "chaosproxy/proxy/config.h"
#include "chaosproxy/proxy/proxy_server.h"

#include <iostream>
#include <string>

#include <CLI/CLI.hpp>

namespace {

int ReportStatus(const chaosproxy::Status& status) {
    std::cerr << status.message() << '\n';
    return chaosproxy::ExitCodeForStatus(status);
}

}  // namespace

int main(int argc, char** argv) {
    CLI::App app{"ChaosProxy TCP fault-injection proxy"};
    app.set_version_flag("--version", "chaosproxy 0.1.0");

    std::string config_path;
    bool check_config = false;
    app.add_option("-c,--config", config_path,
                   "JSON proxy configuration path");
    app.add_flag("--check-config", check_config,
                 "load and validate configuration without binding listeners");

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& error) {
        return app.exit(error);
    }

    if (config_path.empty()) {
        return ReportStatus(chaosproxy::Status(
            chaosproxy::StatusCode::kInvalidArgument,
            "proxy configuration is required; use --config PATH"));
    }

    chaosproxy::LoggingOptions logging_options;
    const chaosproxy::Status logging_status =
        chaosproxy::InitLogging(logging_options);
    if (!logging_status.ok()) {
        return ReportStatus(logging_status);
    }

    auto config = chaosproxy::LoadProxyConfig(config_path);
    if (!config.ok()) return ReportStatus(config.status());
    if (check_config) {
        std::cout << "config ok\n";
        return 0;
    }
    chaosproxy::ProxyServer server;
    const chaosproxy::Status start_status = server.Start(config.value());
    if (!start_status.ok()) return ReportStatus(start_status);
    server.Run();
    return 0;
}
