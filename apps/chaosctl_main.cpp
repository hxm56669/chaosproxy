#include "chaosproxy/common/status.h"

#include <iostream>
#include <algorithm>
#include <cstdint>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <CLI/CLI.hpp>

namespace {

int ReportStatus(const chaosproxy::Status& status) {
    std::cerr << status.message() << '\n';
    return chaosproxy::ExitCodeForStatus(status);
}

}  // namespace

int main(int argc, char** argv) {
    CLI::App app{"ChaosProxy Unix-domain control client"};
    app.set_version_flag("--version", "chaosctl 0.1.0");

    std::string socket_path;
    std::string operation = "get_metrics";
    std::uint64_t request_id = 1;
    std::uint64_t expected_version = 0;
    app.add_option("-s,--socket", socket_path, "control UDS path");
    app.add_option("-o,--operation", operation, "control operation");
    app.add_option("--request-id", request_id, "request id");
    app.add_option("--expected-version", expected_version, "expected policy version");

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& error) {
        return app.exit(error);
    }

    if (socket_path.empty()) {
        return ReportStatus(chaosproxy::Status(
            chaosproxy::StatusCode::kInvalidArgument,
            "control socket is required; use --socket PATH"));
    }

    const int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return ReportStatus(chaosproxy::Status(
            chaosproxy::StatusCode::kUnavailable, "cannot create control socket"));
    }
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (socket_path.size() >= sizeof(address.sun_path)) {
        ::close(fd);
        return ReportStatus(chaosproxy::Status(
            chaosproxy::StatusCode::kInvalidArgument, "control socket path is too long"));
    }
    std::copy(socket_path.c_str(), socket_path.c_str() + socket_path.size() + 1,
              address.sun_path);
    if (::connect(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0) {
        ::close(fd);
        return ReportStatus(chaosproxy::Status(
            chaosproxy::StatusCode::kUnavailable, "cannot connect to control UDS"));
    }
    const std::string request = "{\"operation\":\"" + operation +
                                "\",\"request_id\":" + std::to_string(request_id) +
                                ",\"expected_policy_version\":" +
                                std::to_string(expected_version) + "}\n";
    std::size_t sent = 0;
    while (sent < request.size()) {
        const ssize_t count = ::send(fd, request.data() + sent, request.size() - sent,
                                     MSG_NOSIGNAL);
        if (count <= 0) {
            ::close(fd);
            return ReportStatus(chaosproxy::Status(
                chaosproxy::StatusCode::kUnavailable, "failed to send control request"));
        }
        sent += static_cast<std::size_t>(count);
    }
    char response[4096]{};
    const ssize_t count = ::recv(fd, response, sizeof(response) - 1, 0);
    ::close(fd);
    if (count <= 0) {
        return ReportStatus(chaosproxy::Status(
            chaosproxy::StatusCode::kUnavailable, "failed to receive control reply"));
    }
    std::cout.write(response, count);
    return 0;
}
