#pragma once

#include "chaosproxy/common/status_or.h"
#include "chaosproxy/proxy/types.h"

#include <cstddef>
#include <optional>
#include <span>

namespace chaosproxy {

struct AcceptResult {
    UniqueFd fd;
    SocketAddress peer;
};

StatusOr<UniqueFd> CreateListener(const SocketAddress&, int backlog);
StatusOr<std::optional<AcceptResult>> TryAccept(int listener_fd);
StatusOr<ConnectResult> StartConnect(const SocketAddress&);
Status FinishConnect(int fd);
IoResult TryRead(int fd, std::span<std::byte> dst);
IoResult TryWrite(int fd, std::span<const std::byte> src);
Status ShutdownWrite(int fd);
Status SetAbortiveClose(int fd);

}  // namespace chaosproxy
