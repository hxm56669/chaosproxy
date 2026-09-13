#pragma once

#include "chaosproxy/control/control_protocol.h"

#include <deque>
#include <filesystem>
#include <functional>
#include <unordered_map>
#include <vector>

namespace chaosproxy {

class ControlServer {
public:
    using CommandHandler = std::function<ControlReply(const ControlCommand&)>;

    explicit ControlServer(CommandHandler handler = {});
    ~ControlServer();

    Status Start(const std::filesystem::path& uds_path);
    std::vector<int> AcceptPending();
    void OnReadable(int client_fd);
    void OnWritable(int client_fd);
    void QueueReply(std::uint64_t request_id, ControlReply reply);
    void StopAccepting();

    int listen_fd() const noexcept { return listen_fd_; }

private:
    struct ClientState {
        ControlProtocol protocol;
        std::deque<std::string> replies;
    };

    CommandHandler handler_;
    int listen_fd_ = -1;
    std::filesystem::path uds_path_;
    std::unordered_map<int, ClientState> clients_;
};

}  // namespace chaosproxy
