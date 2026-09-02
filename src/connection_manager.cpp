#include "chaosproxy/connection_manager.h"

#include "chaosproxy/socket_ops.h"

#include <sys/socket.h>

#include <cerrno>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>

namespace chaosproxy {
namespace {

ConnectionGeneration NextGeneration(
    ConnectionGeneration current) noexcept {
    ++current;
    if (current == 0) {
        ++current;
    }
    return current;
}

}  // namespace

ConnectionManager::ConnectionManager(
    EventLoop& loop,
    const sockaddr* upstream_address,
    socklen_t upstream_address_length,
    ConnectionLimits limits)
    : loop_(loop),
      upstream_address_length_(upstream_address_length),
      limits_(limits) {
    if (upstream_address == nullptr ||
        upstream_address_length == 0 ||
        static_cast<std::size_t>(upstream_address_length) >
            sizeof(upstream_address_)) {
        throw std::invalid_argument("invalid upstream address");
    }

    std::memcpy(
        &upstream_address_,
        upstream_address,
        upstream_address_length);
}

ConnectionManager::~ConnectionManager() noexcept {
    CloseAll(CloseReason::kLocalStop);
}

CreateConnectionResult ConnectionManager::Create(
    UniqueFd client_fd) {
    if (!client_fd.IsValid()) {
        return {{}, EBADF};
    }

    const int family = upstream_address_.ss_family;
    UniqueFd upstream_fd(::socket(
        family,
        SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
        0));

    if (!upstream_fd.IsValid()) {
        return {{}, errno};
    }

    const ConnectResult connect_result = StartConnect(
        upstream_fd.Get(),
        reinterpret_cast<const sockaddr*>(&upstream_address_),
        upstream_address_length_);

    if (connect_result.status == ConnectStatus::kError) {
        return {{}, connect_result.error_number};
    }

    const EndpointState upstream_state =
        connect_result.status == ConnectStatus::kConnected
            ? EndpointState::kEstablished
            : EndpointState::kConnecting;

    const ConnectionToken token = AllocateToken();
    Slot& slot = slots_[static_cast<std::size_t>(token.id - 1)];

    try {
        slot.connection = std::make_unique<ConnectionPair>(
            loop_,
            token,
            std::move(client_fd),
            std::move(upstream_fd),
            upstream_state,
            limits_,
            [this](
                ConnectionToken connection_token,
                EndpointSide side,
                EventToken event_token,
                std::uint32_t events) {
                DispatchEvent(
                    connection_token,
                    side,
                    event_token,
                    events);
            },
            [this](ConnectionToken closed_token, CloseReason reason) {
                OnClosed(closed_token, reason);
            });

        ++active_count_;
        slot.connection->Start();
    } catch (...) {
        if (slot.connection) {
            slot.connection.reset();
            --active_count_;
        }
        slot.pending_destroy = false;
        free_ids_.push_back(token.id);
        throw;
    }

    return {token, 0};
}

bool ConnectionManager::Close(
    ConnectionToken token,
    CloseReason reason) noexcept {
    ConnectionPair* connection = Find(token);
    if (connection == nullptr) {
        return false;
    }

    connection->Close(reason);
    CollectClosed(token);
    return true;
}

void ConnectionManager::CloseAll(CloseReason reason) noexcept {
    for (Slot& slot : slots_) {
        if (slot.connection) {
            slot.connection->Close(reason);
        }
    }

    for (std::size_t index = 0; index < slots_.size(); ++index) {
        Slot& slot = slots_[index];
        if (slot.connection && slot.pending_destroy) {
            const ConnectionToken token{
                static_cast<ConnectionId>(index + 1),
                slot.generation
            };
            ReleaseSlot(token);
        }
    }
}

std::size_t ConnectionManager::Size() const noexcept {
    return active_count_;
}

ConnectionPair* ConnectionManager::Find(
    ConnectionToken token) noexcept {
    Slot* slot = FindSlot(token);
    return slot == nullptr ? nullptr : slot->connection.get();
}

const ConnectionPair* ConnectionManager::Find(
    ConnectionToken token) const noexcept {
    const Slot* slot = FindSlot(token);
    return slot == nullptr ? nullptr : slot->connection.get();
}

ConnectionToken ConnectionManager::AllocateToken() {
    if (!free_ids_.empty()) {
        const ConnectionId id = free_ids_.back();
        free_ids_.pop_back();

        Slot& slot = slots_[static_cast<std::size_t>(id - 1)];
        slot.generation = NextGeneration(slot.generation);
        slot.pending_destroy = false;
        return {id, slot.generation};
    }

    if (slots_.size() >=
        static_cast<std::size_t>(
            std::numeric_limits<ConnectionId>::max())) {
        throw std::overflow_error("connection id space exhausted");
    }

    Slot slot;
    slot.generation = 1;
    slots_.push_back(std::move(slot));

    return {
        static_cast<ConnectionId>(slots_.size()),
        1
    };
}

void ConnectionManager::ReleaseSlot(
    ConnectionToken token) noexcept {
    Slot* slot = FindSlot(token);
    if (slot == nullptr || !slot->connection) {
        return;
    }

    slot->connection.reset();
    slot->pending_destroy = false;
    free_ids_.push_back(token.id);

    if (active_count_ > 0) {
        --active_count_;
    }
}

void ConnectionManager::CollectClosed(
    ConnectionToken token) noexcept {
    Slot* slot = FindSlot(token);
    if (slot == nullptr || !slot->connection) {
        return;
    }

    if (!slot->pending_destroy ||
        !slot->connection->IsClosed()) {
        return;
    }

    ReleaseSlot(token);
}

ConnectionManager::Slot* ConnectionManager::FindSlot(
    ConnectionToken token) noexcept {
    if (!token.IsValid() || token.id > slots_.size()) {
        return nullptr;
    }

    Slot& slot = slots_[static_cast<std::size_t>(token.id - 1)];
    if (!slot.connection || slot.generation != token.generation) {
        return nullptr;
    }

    return &slot;
}

const ConnectionManager::Slot* ConnectionManager::FindSlot(
    ConnectionToken token) const noexcept {
    if (!token.IsValid() || token.id > slots_.size()) {
        return nullptr;
    }

    const Slot& slot = slots_[static_cast<std::size_t>(token.id - 1)];
    if (!slot.connection || slot.generation != token.generation) {
        return nullptr;
    }

    return &slot;
}

void ConnectionManager::DispatchEvent(
    ConnectionToken connection_token,
    EndpointSide side,
    EventToken event_token,
    std::uint32_t events) noexcept {
    ConnectionPair* connection = Find(connection_token);
    if (connection == nullptr) {
        return;
    }

    connection->OnEvent(side, event_token, events);
    CollectClosed(connection_token);
}

void ConnectionManager::OnClosed(
    ConnectionToken token,
    CloseReason /*reason*/) noexcept {
    Slot* slot = FindSlot(token);
    if (slot == nullptr) {
        return;
    }

    slot->pending_destroy = true;
}

}  // namespace chaosproxy
