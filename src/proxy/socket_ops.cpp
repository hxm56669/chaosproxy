#include "chaosproxy/proxy/socket_ops.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace chaosproxy {
namespace {

constexpr int kMaxEintrRetries = 4;

bool IsWouldBlock(int error) noexcept {
    return error == EAGAIN || error == EWOULDBLOCK;
}

}  // namespace

StatusOr<SocketAddress> SocketAddress::Parse(std::string_view host,
                                             std::uint16_t port) {
    if (host.empty()) {
        return Status(StatusCode::kInvalidArgument, "host must not be empty");
    }

    const std::string host_string(host);
    const std::string port_string = std::to_string(port);
    addrinfo hints{};
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;
    addrinfo* results = nullptr;
    const int error = ::getaddrinfo(host_string.c_str(), port_string.c_str(),
                                    &hints, &results);
    if (error != 0) {
        return Status(StatusCode::kInvalidArgument,
                      "getaddrinfo: " + std::string(gai_strerror(error)));
    }

    const addrinfo* result = results;
    SocketAddress address = SocketAddress::FromNative(result->ai_addr,
                                                      result->ai_addrlen);
    ::freeaddrinfo(results);
    return address;
}

SocketAddress SocketAddress::FromNative(const sockaddr* address,
                                        socklen_t length) noexcept {
    SocketAddress result;
    if (address == nullptr || length == 0 ||
        length > sizeof(result.storage_)) {
        return result;
    }
    std::memcpy(&result.storage_, address, length);
    result.length_ = length;
    return result;
}

std::uint16_t SocketAddress::port() const noexcept {
    if (storage_.ss_family == AF_INET && length_ >= sizeof(sockaddr_in)) {
        const auto* address = reinterpret_cast<const sockaddr_in*>(&storage_);
        return ntohs(address->sin_port);
    }
    if (storage_.ss_family == AF_INET6 && length_ >= sizeof(sockaddr_in6)) {
        const auto* address = reinterpret_cast<const sockaddr_in6*>(&storage_);
        return ntohs(address->sin6_port);
    }
    return 0;
}

std::string SocketAddress::ToString() const {
    if (length_ == 0) {
        return "<invalid>";
    }
    char host[NI_MAXHOST]{};
    char service[NI_MAXSERV]{};
    const int error = ::getnameinfo(data(), length_, host, sizeof(host),
                                    service, sizeof(service),
                                    NI_NUMERICHOST | NI_NUMERICSERV);
    if (error != 0) {
        return "<invalid>";
    }
    return std::string(host) + ":" + service;
}

StatusOr<UniqueFd> CreateListener(const SocketAddress& address, int backlog) {
    if (address.size() == 0 || backlog <= 0) {
        return Status(StatusCode::kInvalidArgument,
                      "listener address and positive backlog are required");
    }

    UniqueFd listener(::socket(address.family(),
                               SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!listener) {
        const int error = errno;
        return StatusFromErrno(error, "socket");
    }

    int reuse = 1;
    if (::setsockopt(listener.get(), SOL_SOCKET, SO_REUSEADDR, &reuse,
                     sizeof(reuse)) < 0) {
        const int error = errno;
        return StatusFromErrno(error, "setsockopt(SO_REUSEADDR)");
    }
    if (::bind(listener.get(), address.data(), address.size()) < 0) {
        const int error = errno;
        return StatusFromErrno(error, "bind");
    }
    if (::listen(listener.get(), backlog) < 0) {
        const int error = errno;
        return StatusFromErrno(error, "listen");
    }
    return listener;
}

StatusOr<std::optional<AcceptResult>> TryAccept(int listener_fd) {
    if (listener_fd < 0) {
        return Status(StatusCode::kInvalidArgument,
                      "listener fd must be valid");
    }

    for (int attempt = 0; attempt < kMaxEintrRetries; ++attempt) {
        sockaddr_storage peer{};
        socklen_t peer_length = sizeof(peer);
        const int accepted = ::accept4(listener_fd,
                                        reinterpret_cast<sockaddr*>(&peer),
                                        &peer_length,
                                        SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (accepted >= 0) {
            AcceptResult result{UniqueFd(accepted),
                                SocketAddress::FromNative(
                                    reinterpret_cast<const sockaddr*>(&peer),
                                    peer_length)};
            return std::optional<AcceptResult>(std::move(result));
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (IsWouldBlock(error)) {
            return std::optional<AcceptResult>{};
        }
        return StatusFromErrno(error, "accept4");
    }
    return Status(StatusCode::kIoError, "accept4 interrupted repeatedly");
}

StatusOr<ConnectResult> StartConnect(const SocketAddress& address) {
    if (address.size() == 0) {
        return Status(StatusCode::kInvalidArgument,
                      "connect address must be valid");
    }

    UniqueFd fd(::socket(address.family(),
                         SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!fd) {
        const int error = errno;
        return StatusFromErrno(error, "socket");
    }

    if (::connect(fd.get(), address.data(), address.size()) == 0) {
        return ConnectResult{std::move(fd), ConnectProgress::kConnected};
    }
    const int error = errno;
    if (error == EINPROGRESS || error == EALREADY || error == EINTR) {
        return ConnectResult{std::move(fd), ConnectProgress::kInProgress};
    }
    return StatusFromErrno(error, "connect");
}

Status FinishConnect(int fd) {
    if (fd < 0) {
        return Status(StatusCode::kInvalidArgument,
                      "connect fd must be valid");
    }
    int socket_error = 0;
    socklen_t length = sizeof(socket_error);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &length) < 0) {
        const int error = errno;
        return StatusFromErrno(error, "getsockopt(SO_ERROR)");
    }
    if (socket_error != 0) {
        return StatusFromErrno(socket_error, "connect");
    }
    return Status::Ok();
}

IoResult TryRead(int fd, std::span<std::byte> dst) {
    if (dst.empty()) {
        return IoResult{IoCode::kProgress, 0, 0};
    }
    for (int attempt = 0; attempt < kMaxEintrRetries; ++attempt) {
        const ssize_t count = ::recv(fd, dst.data(), dst.size(), 0);
        if (count > 0) {
            return IoResult{IoCode::kProgress,
                            static_cast<std::size_t>(count), 0};
        }
        if (count == 0) {
            return IoResult{IoCode::kEof, 0, 0};
        }
        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (IsWouldBlock(error)) {
            return IoResult{IoCode::kWouldBlock, 0, error};
        }
        return IoResult{IoCode::kError, 0, error};
    }
    return IoResult{IoCode::kError, 0, EINTR};
}

IoResult TryWrite(int fd, std::span<const std::byte> src) {
    if (src.empty()) {
        return IoResult{IoCode::kProgress, 0, 0};
    }
    for (int attempt = 0; attempt < kMaxEintrRetries; ++attempt) {
        const ssize_t count = ::send(fd, src.data(), src.size(), MSG_NOSIGNAL);
        if (count > 0) {
            return IoResult{IoCode::kProgress,
                            static_cast<std::size_t>(count), 0};
        }
        if (count == 0) {
            return IoResult{IoCode::kError, 0, EIO};
        }
        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (IsWouldBlock(error)) {
            return IoResult{IoCode::kWouldBlock, 0, error};
        }
        return IoResult{IoCode::kError, 0, error};
    }
    return IoResult{IoCode::kError, 0, EINTR};
}

Status ShutdownWrite(int fd) {
    if (fd < 0) {
        return Status(StatusCode::kInvalidArgument,
                      "shutdown fd must be valid");
    }
    if (::shutdown(fd, SHUT_WR) < 0) {
        const int error = errno;
        return StatusFromErrno(error, "shutdown(SHUT_WR)");
    }
    return Status::Ok();
}

Status SetAbortiveClose(int fd) {
    if (fd < 0) {
        return Status(StatusCode::kInvalidArgument,
                      "abortive-close fd must be valid");
    }
    linger option{1, 0};
    if (::setsockopt(fd, SOL_SOCKET, SO_LINGER, &option, sizeof(option)) < 0) {
        const int error = errno;
        return StatusFromErrno(error, "setsockopt(SO_LINGER)");
    }
    return Status::Ok();
}

}  // namespace chaosproxy
