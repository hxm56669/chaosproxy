#pragma once

#include "chaosproxy/unique_fd.h"

#include <sys/socket.h>

#include <cstddef>

namespace chaosproxy {

enum class ReadStatus {
    kData,
    kEof,
    kWouldBlock,
    kError
};

struct ReadResult {
    ReadStatus status{ReadStatus::kError};
    std::size_t bytes_transferred{0};
    int error_number{0};
};

enum class WriteStatus {
    kWritten,
    kWouldBlock,
    kError
};

struct WriteResult {
    WriteStatus status{WriteStatus::kError};
    std::size_t bytes_transferred{0};
    int error_number{0};
};

enum class AcceptStatus {
    kAccepted,
    kWouldBlock,
    kError
};

struct AcceptResult {
    AcceptStatus status{AcceptStatus::kError};
    UniqueFd client_fd{};
    int error_number{0};
};

enum class ConnectStatus {
    kConnected,
    kInProgress,
    kError
};

struct ConnectResult {
    ConnectStatus status{ConnectStatus::kError};
    int error_number{0};
};

[[nodiscard]] ReadResult TryRead(
    int fd,
    void* buffer,
    std::size_t capacity) noexcept;

[[nodiscard]] WriteResult TryWrite(
    int fd,
    const void* data,
    std::size_t size) noexcept;

[[nodiscard]] AcceptResult TryAccept(int listen_fd) noexcept;

[[nodiscard]] ConnectResult StartConnect(
    int fd,
    const sockaddr* address,
    socklen_t address_length) noexcept;

[[nodiscard]] ConnectResult CompleteConnect(int fd) noexcept;

[[nodiscard]] int GetPendingSocketError(int fd) noexcept;

}  // namespace chaosproxy
