#pragma once

#include "chaosproxy/buffer.h"
#include "chaosproxy/endpoint.h"
#include "chaosproxy/event_loop.h"
#include "chaosproxy/unique_fd.h"

#include <cstddef>
#include <cstdint>
#include <functional>

namespace chaosproxy {

using ConnectionId = std::uint64_t;
using ConnectionGeneration = std::uint64_t;

struct ConnectionToken {
    ConnectionId id{0};
    ConnectionGeneration generation{0};

    [[nodiscard]] bool IsValid() const noexcept {
        return id != 0 && generation != 0;
    }
};

inline bool operator==(
    const ConnectionToken& lhs,
    const ConnectionToken& rhs) noexcept {
    return lhs.id == rhs.id && lhs.generation == rhs.generation;
}

inline bool operator!=(
    const ConnectionToken& lhs,
    const ConnectionToken& rhs) noexcept {
    return !(lhs == rhs);
}

enum class CloseReason {
    kNone,
    kGracefulEof,
    kConnectFailed,
    kConnectionReset,
    kReadError,
    kWriteError,
    kSocketError,
    kShutdownError,
    kLocalStop,
    kInternalError
};

struct ConnectionLimits {
    std::size_t buffer_capacity{256 * 1024};
    std::size_t high_watermark{192 * 1024};
    std::size_t low_watermark{128 * 1024};
    std::size_t read_budget_per_event{64 * 1024};
    std::size_t write_budget_per_event{64 * 1024};
};

class ConnectionPair final {
public:
    using ClosedCallback =
        std::function<void(ConnectionToken, CloseReason)>;

    using EventDispatchCallback = std::function<void(
        ConnectionToken,
        EndpointSide,
        EventToken,
        std::uint32_t)>;

    ConnectionPair(
        EventLoop& loop,
        ConnectionToken token,
        UniqueFd client_fd,
        UniqueFd upstream_fd,
        EndpointState upstream_state,
        ConnectionLimits limits,
        EventDispatchCallback event_dispatch_callback,
        ClosedCallback closed_callback);

    ~ConnectionPair() noexcept;

    ConnectionPair(const ConnectionPair&) = delete;
    ConnectionPair& operator=(const ConnectionPair&) = delete;

    void Start();
    void Close(CloseReason reason) noexcept;

    [[nodiscard]] ConnectionToken Token() const noexcept;
    [[nodiscard]] ConnectionId Id() const noexcept;
    [[nodiscard]] ConnectionGeneration Generation() const noexcept;
    [[nodiscard]] bool IsStarted() const noexcept;
    [[nodiscard]] bool IsClosed() const noexcept;
    [[nodiscard]] CloseReason Reason() const noexcept;

    [[nodiscard]] std::size_t PendingBytes(
        EndpointSide source) const noexcept;

    [[nodiscard]] bool ReadPaused(
        EndpointSide source) const noexcept;

    [[nodiscard]] bool SourceEof(
        EndpointSide source) const noexcept;

    [[nodiscard]] bool WriteShutdownFor(
        EndpointSide source) const noexcept;

    void OnEvent(
        EndpointSide side,
        EventToken token,
        std::uint32_t events) noexcept;

private:
    struct Direction {
        Direction(
            EndpointSide source_side,
            EndpointSide destination_side,
            std::size_t capacity)
            : source(source_side),
              destination(destination_side),
              pending(capacity) {}

        EndpointSide source;
        EndpointSide destination;
        Buffer pending;
        bool source_eof{false};
        bool write_shutdown{false};
        bool read_paused{false};
    };

    void HandleEventImpl(
        EndpointSide side,
        EventToken token,
        std::uint32_t events);

    bool FinishUpstreamConnect();
    void HandleReadable(EndpointSide source_side);
    void HandleWritable(EndpointSide destination_side);

    void FlushDirection(
        Direction& direction,
        std::size_t& write_budget);

    void UpdateBackpressure(Direction& direction) noexcept;
    void TryFinishDirection(Direction& direction);
    void MaybeFinishConnection();

    [[nodiscard]] Endpoint& GetEndpoint(EndpointSide side) noexcept;
    [[nodiscard]] const Endpoint& GetEndpoint(
        EndpointSide side) const noexcept;

    [[nodiscard]] Direction& SourceDirection(
        EndpointSide source) noexcept;

    [[nodiscard]] const Direction& SourceDirection(
        EndpointSide source) const noexcept;

    [[nodiscard]] Direction& DestinationDirection(
        EndpointSide destination) noexcept;

    [[nodiscard]] const Direction& DestinationDirection(
        EndpointSide destination) const noexcept;

    [[nodiscard]] std::uint32_t DesiredInterests(
        EndpointSide side) const noexcept;

    void RefreshInterests();
    void RefreshEndpointInterests(Endpoint& endpoint);

    [[nodiscard]] bool ShutdownWrite(Direction& direction) noexcept;
    void Cleanup() noexcept;

    EventLoop& loop_;
    ConnectionToken token_;
    ConnectionLimits limits_;
    EventDispatchCallback event_dispatch_callback_;
    ClosedCallback closed_callback_;

    Endpoint client_;
    Endpoint upstream_;

    Direction client_to_upstream_;
    Direction upstream_to_client_;

    bool started_{false};
    bool closed_{false};
    CloseReason close_reason_{CloseReason::kNone};
};

}  // namespace chaosproxy
