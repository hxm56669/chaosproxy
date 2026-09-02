#include "chaosproxy/socket_ops.h"

#include <cerrno>
#include <cstdint>
#include <limits>

namespace chaosproxy {
namespace {

bool IsWouldBlock(int error_number) noexcept {
    return error_number == EAGAIN || error_number == EWOULDBLOCK;
}

std::size_t ClampIoSize(std::size_t size) noexcept {
    const auto maximum = static_cast<std::size_t>(
        std::numeric_limits<ssize_t>::max());
    return size > maximum ? maximum : size;
}

}  // namespace

ReadResult TryRead(
    int fd,
    void* buffer,
    std::size_t capacity) noexcept {
    if (fd < 0 || (buffer == nullptr && capacity != 0)) {
        return {ReadStatus::kError, 0, EINVAL};
    }

    for (;;) {
        const ssize_t result = ::recv(
            fd,
            buffer,
            ClampIoSize(capacity),
            0);

        if (result > 0) {
            return {
                ReadStatus::kData,
                static_cast<std::size_t>(result),
                0
            };
        }

        if (result == 0) {
            return {ReadStatus::kEof, 0, 0};
        }

        const int error_number = errno;
        if (error_number == EINTR) {
            continue;
        }
        if (IsWouldBlock(error_number)) {
            return {ReadStatus::kWouldBlock, 0, 0};
        }
        return {ReadStatus::kError, 0, error_number};
    }
}

WriteResult TryWrite(
    int fd,
    const void* data,
    std::size_t size) noexcept {
    if (fd < 0 || (data == nullptr && size != 0)) {
        return {WriteStatus::kError, 0, EINVAL};
    }

    for (;;) {
        const ssize_t result = ::send(
            fd,
            data,
            ClampIoSize(size),
            MSG_NOSIGNAL);

        if (result >= 0) {
            return {
                WriteStatus::kWritten,
                static_cast<std::size_t>(result),
                0
            };
        }

        const int error_number = errno;
        if (error_number == EINTR) {
            continue;
        }
        if (IsWouldBlock(error_number)) {
            return {WriteStatus::kWouldBlock, 0, 0};
        }
        return {WriteStatus::kError, 0, error_number};
    }
}

AcceptResult TryAccept(int listen_fd) noexcept {
    if (listen_fd < 0) {
        return {AcceptStatus::kError, {}, EBADF};
    }

    for (;;) {
        const int client_fd = ::accept4(
            listen_fd,
            nullptr,
            nullptr,
            SOCK_NONBLOCK | SOCK_CLOEXEC);

        if (client_fd >= 0) {
            return {
                AcceptStatus::kAccepted,
                UniqueFd(client_fd),
                0
            };
        }

        const int error_number = errno;
        if (error_number == EINTR) {
            continue;
        }
        if (IsWouldBlock(error_number)) {
            return {AcceptStatus::kWouldBlock, {}, 0};
        }
        return {AcceptStatus::kError, {}, error_number};
    }
}

ConnectResult StartConnect(
    int fd,
    const sockaddr* address,
    socklen_t address_length) noexcept {
    if (fd < 0 || address == nullptr || address_length == 0) {
        return {ConnectStatus::kError, EINVAL};
    }

    for (;;) {
        if (::connect(fd, address, address_length) == 0) {
            return {ConnectStatus::kConnected, 0};
        }

        const int error_number = errno;
        if (error_number == EISCONN) {
            return {ConnectStatus::kConnected, 0};
        }
        if (error_number == EINPROGRESS ||
            error_number == EALREADY ||
            error_number == EINTR) {
            return {ConnectStatus::kInProgress, 0};
        }
        return {ConnectStatus::kError, error_number};
    }
}

ConnectResult CompleteConnect(int fd) noexcept {
    const int error_number = GetPendingSocketError(fd);
    if (error_number == 0) {
        return {ConnectStatus::kConnected, 0};
    }
    if (error_number == EINPROGRESS || error_number == EALREADY) {
        return {ConnectStatus::kInProgress, 0};
    }
    return {ConnectStatus::kError, error_number};
}

int GetPendingSocketError(int fd) noexcept {
    if (fd < 0) {
        return EBADF;
    }

    int socket_error = 0;
    socklen_t length = sizeof(socket_error);

    for (;;) {
        if (::getsockopt(
                fd,
                SOL_SOCKET,
                SO_ERROR,
                &socket_error,
                &length) == 0) {
            return socket_error;
        }

        const int error_number = errno;
        if (error_number != EINTR) {
            return error_number;
        }
    }
}

}  // namespace chaosproxy
