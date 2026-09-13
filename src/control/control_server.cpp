#include "chaosproxy/control/control_server.h"

#include "chaosproxy/common/status.h"

#include <array>
#include <cerrno>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <utility>

namespace chaosproxy {

ControlServer::ControlServer(CommandHandler handler) : handler_(std::move(handler)) {}

ControlServer::~ControlServer() { StopAccepting(); }

Status ControlServer::Start(const std::filesystem::path& uds_path) {
    const std::string path = uds_path.string();
    if (path.empty() || path.size() >= sizeof(sockaddr_un::sun_path)) {
        return Status(StatusCode::kInvalidArgument, "control UDS path is invalid");
    }
    StopAccepting();
    listen_fd_ = ::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd_ < 0) return StatusFromErrno(errno, "socket(AF_UNIX)");
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::copy(path.c_str(), path.c_str() + path.size() + 1, address.sun_path);
    (void)::unlink(path.c_str());
    if (::bind(listen_fd_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0 ||
        ::listen(listen_fd_, 16) < 0) {
        const int error = errno;
        StopAccepting();
        return StatusFromErrno(error, "bind/listen(control UDS)");
    }
    uds_path_ = uds_path;
    return Status::Ok();
}

std::vector<int> ControlServer::AcceptPending() {
    std::vector<int> accepted;
    for (;;) {
        const int fd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd >= 0) {
            clients_.emplace(fd, ClientState{});
            accepted.push_back(fd);
            continue;
        }
        if (errno == EINTR) continue;
        break;
    }
    return accepted;
}

void ControlServer::OnReadable(int client_fd) {
    auto client = clients_.find(client_fd);
    if (client == clients_.end()) client = clients_.emplace(client_fd, ClientState{}).first;
    std::array<std::byte, 4096> buffer{};
    for (;;) {
        const ssize_t count = ::recv(client_fd, buffer.data(), buffer.size(), MSG_DONTWAIT);
        if (count > 0) {
            const Status fed = client->second.protocol.Feed(
                std::span<const std::byte>(buffer.data(), static_cast<std::size_t>(count)));
            if (!fed.ok()) return;
            for (;;) {
                auto command = client->second.protocol.NextCommand();
                if (!command.ok() || !command.value().has_value()) break;
                ControlReply reply = handler_ ? handler_(*command.value())
                                              : ControlReply{command.value()->request_id, true,
                                                             "OK", "received", 0, 0, {}};
                client->second.replies.push_back(
                    client->second.protocol.EncodeReply(reply));
            }
            continue;
        }
        if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) return;
        (void)::close(client_fd);
        clients_.erase(client);
        return;
    }
}

void ControlServer::OnWritable(int client_fd) {
    const auto client = clients_.find(client_fd);
    if (client == clients_.end()) return;
    while (!client->second.replies.empty()) {
        std::string& reply = client->second.replies.front();
        const ssize_t count = ::send(client_fd, reply.data(), reply.size(), MSG_DONTWAIT);
        if (count > 0) {
            reply.erase(0, static_cast<std::size_t>(count));
            if (reply.empty()) client->second.replies.pop_front();
            continue;
        }
        if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) return;
        (void)::close(client_fd);
        clients_.erase(client);
        return;
    }
}

void ControlServer::QueueReply(std::uint64_t request_id, ControlReply reply) {
    reply.request_id = request_id;
    for (auto& [fd, client] : clients_) {
        static_cast<void>(fd);
        if (client.replies.size() < 64) {
            client.replies.push_back(client.protocol.EncodeReply(reply));
        }
    }
}

void ControlServer::StopAccepting() {
    for (const auto& [fd, state] : clients_) {
        static_cast<void>(state);
        (void)::close(fd);
    }
    clients_.clear();
    if (listen_fd_ >= 0) {
        (void)::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (!uds_path_.empty()) {
        (void)::unlink(uds_path_.c_str());
        uds_path_.clear();
    }
}

}  // namespace chaosproxy
