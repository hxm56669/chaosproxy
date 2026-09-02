#pragma once

#include "chaosproxy/connection.h"
#include "chaosproxy/event_loop.h"
#include "chaosproxy/unique_fd.h"

#include <sys/socket.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chaosproxy {

struct CreateConnectionResult {
    ConnectionToken token{};
    int error_number{0};

    [[nodiscard]] bool Ok() const noexcept {
        return token.IsValid() && error_number == 0;
    }
};

class ConnectionManager final {
public:
    ConnectionManager(
        EventLoop& loop,
        const sockaddr* upstream_address,
        socklen_t upstream_address_length,
        ConnectionLimits limits = {});

    ~ConnectionManager() noexcept;

    ConnectionManager(const ConnectionManager&) = delete;
    ConnectionManager& operator=(const ConnectionManager&) = delete;

    [[nodiscard]] CreateConnectionResult Create(
        UniqueFd client_fd);

    [[nodiscard]] bool Close(
        ConnectionToken token,
        CloseReason reason = CloseReason::kLocalStop) noexcept;

    void CloseAll(
        CloseReason reason = CloseReason::kLocalStop) noexcept;

    [[nodiscard]] std::size_t Size() const noexcept;

    [[nodiscard]] ConnectionPair* Find(
        ConnectionToken token) noexcept;

    [[nodiscard]] const ConnectionPair* Find(
        ConnectionToken token) const noexcept;

private:
    struct Slot {
        ConnectionGeneration generation{0};
        std::unique_ptr<ConnectionPair> connection;
        bool pending_destroy{false};
    };

    [[nodiscard]] ConnectionToken AllocateToken();
    void ReleaseSlot(ConnectionToken token) noexcept;
    void CollectClosed(ConnectionToken token) noexcept;

    [[nodiscard]] Slot* FindSlot(ConnectionToken token) noexcept;
    [[nodiscard]] const Slot* FindSlot(
        ConnectionToken token) const noexcept;

    void DispatchEvent(
        ConnectionToken connection_token,
        EndpointSide side,
        EventToken event_token,
        std::uint32_t events) noexcept;

    void OnClosed(
        ConnectionToken token,
        CloseReason reason) noexcept;

    EventLoop& loop_;
    sockaddr_storage upstream_address_{};
    socklen_t upstream_address_length_{0};
    ConnectionLimits limits_;

    std::vector<Slot> slots_;
    std::vector<ConnectionId> free_ids_;
    std::size_t active_count_{0};
};

}  // namespace chaosproxy
