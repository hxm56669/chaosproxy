#pragma once

#include "chaosproxy/common/status_or.h"
#include "chaosproxy/common/unique_fd.h"

#include <cstdint>
#include <netinet/in.h>
#include <string>
#include <string_view>
#include <sys/socket.h>

namespace chaosproxy {

struct ProxyId {
    std::uint32_t value = 0;
};

struct ConnectionToken {
    std::uint32_t slot = 0;
    std::uint32_t generation = 0;
};

enum class EndpointSide { kClient, kUpstream };
enum class DirectionId { kClientToUpstream, kUpstreamToClient };
enum class TransportState { kConnecting, kEstablished, kClosed };

enum class IoCode { kProgress, kWouldBlock, kEof, kError };

struct IoResult {
    IoCode code = IoCode::kError;
    std::size_t bytes = 0;
    int error = 0;
};

enum class ConnectProgress { kConnected, kInProgress };

struct ConnectResult {
    UniqueFd fd;
    ConnectProgress progress = ConnectProgress::kInProgress;
};

class SocketAddress {
public:
    static StatusOr<SocketAddress> Parse(std::string_view host,
                                         std::uint16_t port);
    static SocketAddress FromNative(const sockaddr* address,
                                    socklen_t length) noexcept;

    const sockaddr* data() const noexcept {
        return reinterpret_cast<const sockaddr*>(&storage_);
    }
    sockaddr* data() noexcept {
        return reinterpret_cast<sockaddr*>(&storage_);
    }
    socklen_t size() const noexcept { return length_; }
    int family() const noexcept { return storage_.ss_family; }
    std::uint16_t port() const noexcept;
    std::string ToString() const;

private:
    sockaddr_storage storage_{};
    socklen_t length_ = 0;
};

}  // namespace chaosproxy
