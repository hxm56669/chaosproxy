#include "chaosproxy/connection.h"

#include "chaosproxy/socket_ops.h"

#include <sys/epoll.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <stdexcept>
#include <utility>

namespace chaosproxy {
namespace {

constexpr std::size_t kReadChunkSize = 16 * 1024;

void ValidateLimits(const ConnectionLimits& limits) {
    if (limits.buffer_capacity == 0 ||
        limits.low_watermark >= limits.high_watermark ||
        limits.high_watermark > limits.buffer_capacity ||
        limits.read_budget_per_event == 0 ||
        limits.write_budget_per_event == 0) {
        throw std::invalid_argument("invalid connection limits");
    }
}

CloseReason ReadErrorReason(int error_number) noexcept {
    return error_number == ECONNRESET
        ? CloseReason::kConnectionReset
        : CloseReason::kReadError;
}

CloseReason WriteErrorReason(int error_number) noexcept {
    return error_number == ECONNRESET || error_number == EPIPE
        ? CloseReason::kConnectionReset
        : CloseReason::kWriteError;
}

}  // namespace

ConnectionPair::ConnectionPair(
    EventLoop& loop,
    ConnectionToken token,
    UniqueFd client_fd,
    UniqueFd upstream_fd,
    EndpointState upstream_state,
    ConnectionLimits limits,
    EventDispatchCallback event_dispatch_callback,
    ClosedCallback closed_callback)
    : loop_(loop),
      token_(token),
      limits_(limits),
      event_dispatch_callback_(std::move(event_dispatch_callback)),
      closed_callback_(std::move(closed_callback)),
      client_(
          EndpointSide::kClient,
          std::move(client_fd),
          EndpointState::kEstablished),
      upstream_(
          EndpointSide::kUpstream,
          std::move(upstream_fd),
          upstream_state),
      client_to_upstream_(
          EndpointSide::kClient,
          EndpointSide::kUpstream,
          limits.buffer_capacity),
      upstream_to_client_(
          EndpointSide::kUpstream,
          EndpointSide::kClient,
          limits.buffer_capacity) {
    ValidateLimits(limits_);

    if (!token_.IsValid() ||
        !client_.HasFd() ||
        !upstream_.HasFd() ||
        !event_dispatch_callback_ ||
        !closed_callback_) {
        throw std::invalid_argument("invalid connection arguments");
    }

    if (upstream_state != EndpointState::kConnecting &&
        upstream_state != EndpointState::kEstablished) {
        throw std::invalid_argument("invalid upstream state");
    }
}

ConnectionPair::~ConnectionPair() noexcept {
    Cleanup();
}

void ConnectionPair::Start() {
    if (started_ || closed_) {
        return;
    }

    const ConnectionToken connection_token = token_;
    const EventDispatchCallback dispatch = event_dispatch_callback_;

    const auto add_endpoint = [&](Endpoint& endpoint) {
        const EndpointSide side = endpoint.Side();
        const std::uint32_t interests = DesiredInterests(side);

        const EventToken event_token = loop_.Add(
            endpoint.Fd(),
            interests,
            [connection_token, side, dispatch](
                EventToken observed_token,
                std::uint32_t events) {
                dispatch(
                    connection_token,
                    side,
                    observed_token,
                    events);
            });

        endpoint.SetToken(event_token);
        endpoint.SetInterests(interests);
    };

    try {
        add_endpoint(client_);
        add_endpoint(upstream_);
        started_ = true;
    } catch (...) {
        Cleanup();
        throw;
    }
}

void ConnectionPair::Close(CloseReason reason) noexcept {
    if (closed_) {
        return;
    }

    closed_ = true;
    close_reason_ = reason;
    Cleanup();

    try {
        closed_callback_(token_, reason);
    } catch (...) {
        // Closing a connection must not throw back into EventLoop.
    }
}

ConnectionToken ConnectionPair::Token() const noexcept {
    return token_;
}

ConnectionId ConnectionPair::Id() const noexcept {
    return token_.id;
}

ConnectionGeneration ConnectionPair::Generation() const noexcept {
    return token_.generation;
}

bool ConnectionPair::IsStarted() const noexcept {
    return started_;
}

bool ConnectionPair::IsClosed() const noexcept {
    return closed_;
}

CloseReason ConnectionPair::Reason() const noexcept {
    return close_reason_;
}

std::size_t ConnectionPair::PendingBytes(
    EndpointSide source) const noexcept {
    return SourceDirection(source).pending.Size();
}

bool ConnectionPair::ReadPaused(
    EndpointSide source) const noexcept {
    return SourceDirection(source).read_paused;
}

bool ConnectionPair::SourceEof(
    EndpointSide source) const noexcept {
    return SourceDirection(source).source_eof;
}

bool ConnectionPair::WriteShutdownFor(
    EndpointSide source) const noexcept {
    return SourceDirection(source).write_shutdown;
}

void ConnectionPair::OnEvent(
    EndpointSide side,
    EventToken token,
    std::uint32_t events) noexcept {
    if (closed_) {
        return;
    }

    try {
        HandleEventImpl(side, token, events);
    } catch (...) {
        Close(CloseReason::kInternalError);
    }
}

void ConnectionPair::HandleEventImpl(
    EndpointSide side,
    EventToken token,
    std::uint32_t events) {
    Endpoint& endpoint = GetEndpoint(side);

    if (token == 0 || token != endpoint.Token()) {
        return;
    }

    if (endpoint.State() == EndpointState::kConnecting) {
        if ((events & (EPOLLOUT | EPOLLERR | EPOLLHUP)) != 0U) {
            if (!FinishUpstreamConnect()) {
                return;
            }
        } else {
            return;
        }
    }

    if (closed_) {
        return;
    }

    if ((events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) != 0U) {
        HandleReadable(side);
    }

    if (closed_) {
        return;
    }

    if ((events & EPOLLERR) != 0U) {
        const int socket_error = GetPendingSocketError(endpoint.Fd());
        Close(
            socket_error == ECONNRESET
                ? CloseReason::kConnectionReset
                : CloseReason::kSocketError);
        return;
    }

    if ((events & EPOLLOUT) != 0U) {
        HandleWritable(side);
    }

    if (closed_) {
        return;
    }

    RefreshInterests();
}

bool ConnectionPair::FinishUpstreamConnect() {
    if (upstream_.State() != EndpointState::kConnecting) {
        return true;
    }

    const ConnectResult result = CompleteConnect(upstream_.Fd());

    if (result.status == ConnectStatus::kInProgress) {
        RefreshEndpointInterests(upstream_);
        return false;
    }

    if (result.status == ConnectStatus::kError) {
        Close(
            result.error_number == ECONNRESET
                ? CloseReason::kConnectionReset
                : CloseReason::kConnectFailed);
        return false;
    }

    upstream_.SetState(EndpointState::kEstablished);

    std::size_t write_budget = limits_.write_budget_per_event;
    FlushDirection(client_to_upstream_, write_budget);

    if (closed_) {
        return false;
    }

    TryFinishDirection(client_to_upstream_);
    if (closed_) {
        return false;
    }

    RefreshInterests();
    return true;
}

void ConnectionPair::HandleReadable(EndpointSide source_side) {
    Direction& direction = SourceDirection(source_side);

    if (direction.source_eof || direction.read_paused) {
        return;
    }

    Endpoint& source = GetEndpoint(source_side);
    if (source.State() != EndpointState::kEstablished) {
        return;
    }

    std::array<char, kReadChunkSize> buffer{};
    std::size_t read_budget = limits_.read_budget_per_event;
    std::size_t write_budget = limits_.write_budget_per_event;

    while (read_budget > 0 &&
           !direction.source_eof &&
           !direction.read_paused) {
        const std::size_t available = direction.pending.Available();
        if (available == 0) {
            direction.read_paused = true;
            break;
        }

        const std::size_t capacity = std::min(
            {buffer.size(), available, read_budget});

        const ReadResult result = TryRead(
            source.Fd(),
            buffer.data(),
            capacity);

        if (result.status == ReadStatus::kData) {
            if (!direction.pending.Append(
                    buffer.data(),
                    result.bytes_transferred)) {
                Close(CloseReason::kInternalError);
                return;
            }

            read_budget -= result.bytes_transferred;
            UpdateBackpressure(direction);
            FlushDirection(direction, write_budget);

            if (closed_) {
                return;
            }

            UpdateBackpressure(direction);
            continue;
        }

        if (result.status == ReadStatus::kEof) {
            direction.source_eof = true;
            direction.read_paused = true;
            TryFinishDirection(direction);
            break;
        }

        if (result.status == ReadStatus::kWouldBlock) {
            break;
        }

        Close(ReadErrorReason(result.error_number));
        return;
    }

    UpdateBackpressure(direction);
    TryFinishDirection(direction);
    MaybeFinishConnection();
}

void ConnectionPair::HandleWritable(
    EndpointSide destination_side) {
    Direction& direction = DestinationDirection(destination_side);

    std::size_t write_budget = limits_.write_budget_per_event;
    FlushDirection(direction, write_budget);

    if (closed_) {
        return;
    }

    UpdateBackpressure(direction);
    TryFinishDirection(direction);
    MaybeFinishConnection();
}

void ConnectionPair::FlushDirection(
    Direction& direction,
    std::size_t& write_budget) {
    Endpoint& destination = GetEndpoint(direction.destination);

    if (destination.State() != EndpointState::kEstablished ||
        direction.write_shutdown) {
        return;
    }

    while (!direction.pending.Empty() && write_budget > 0) {
        const std::size_t to_write = std::min(
            direction.pending.FrontSize(),
            write_budget);

        const WriteResult result = TryWrite(
            destination.Fd(),
            direction.pending.FrontData(),
            to_write);

        if (result.status == WriteStatus::kWritten) {
            direction.pending.Consume(result.bytes_transferred);
            write_budget -= result.bytes_transferred;
            continue;
        }

        if (result.status == WriteStatus::kWouldBlock) {
            break;
        }

        Close(WriteErrorReason(result.error_number));
        return;
    }
}

void ConnectionPair::UpdateBackpressure(
    Direction& direction) noexcept {
    if (direction.source_eof) {
        direction.read_paused = true;
        return;
    }

    if (!direction.read_paused &&
        direction.pending.Size() >= limits_.high_watermark) {
        direction.read_paused = true;
        return;
    }

    if (direction.read_paused &&
        direction.pending.Size() <= limits_.low_watermark) {
        direction.read_paused = false;
    }
}

void ConnectionPair::TryFinishDirection(Direction& direction) {
    if (!direction.source_eof ||
        !direction.pending.Empty() ||
        direction.write_shutdown) {
        return;
    }

    Endpoint& destination = GetEndpoint(direction.destination);

    if (destination.State() == EndpointState::kConnecting) {
        return;
    }

    if (destination.State() != EndpointState::kEstablished) {
        return;
    }

    if (!ShutdownWrite(direction)) {
        Close(CloseReason::kShutdownError);
        return;
    }

    direction.write_shutdown = true;
}

void ConnectionPair::MaybeFinishConnection() {
    const auto finished = [](const Direction& direction) {
        return direction.source_eof &&
               direction.pending.Empty() &&
               direction.write_shutdown;
    };

    if (finished(client_to_upstream_) &&
        finished(upstream_to_client_)) {
        Close(CloseReason::kGracefulEof);
    }
}

Endpoint& ConnectionPair::GetEndpoint(
    EndpointSide side) noexcept {
    return side == EndpointSide::kClient
        ? client_
        : upstream_;
}

const Endpoint& ConnectionPair::GetEndpoint(
    EndpointSide side) const noexcept {
    return side == EndpointSide::kClient
        ? client_
        : upstream_;
}

ConnectionPair::Direction& ConnectionPair::SourceDirection(
    EndpointSide source) noexcept {
    return source == EndpointSide::kClient
        ? client_to_upstream_
        : upstream_to_client_;
}

const ConnectionPair::Direction& ConnectionPair::SourceDirection(
    EndpointSide source) const noexcept {
    return source == EndpointSide::kClient
        ? client_to_upstream_
        : upstream_to_client_;
}

ConnectionPair::Direction& ConnectionPair::DestinationDirection(
    EndpointSide destination) noexcept {
    return destination == EndpointSide::kUpstream
        ? client_to_upstream_
        : upstream_to_client_;
}

const ConnectionPair::Direction& ConnectionPair::DestinationDirection(
    EndpointSide destination) const noexcept {
    return destination == EndpointSide::kUpstream
        ? client_to_upstream_
        : upstream_to_client_;
}

std::uint32_t ConnectionPair::DesiredInterests(
    EndpointSide side) const noexcept {
    const Endpoint& endpoint = GetEndpoint(side);

    if (endpoint.State() == EndpointState::kClosed) {
        return 0;
    }

    if (endpoint.State() == EndpointState::kConnecting) {
        return EPOLLOUT;
    }

    std::uint32_t interests = 0;

    const Direction& read_direction = SourceDirection(side);
    if (!read_direction.source_eof &&
        !read_direction.read_paused) {
        interests |= EPOLLIN | EPOLLRDHUP;
    }

    const Direction& write_direction = DestinationDirection(side);
    if (!write_direction.write_shutdown &&
        !write_direction.pending.Empty()) {
        interests |= EPOLLOUT;
    }

    return interests;
}

void ConnectionPair::RefreshInterests() {
    RefreshEndpointInterests(client_);
    if (!closed_) {
        RefreshEndpointInterests(upstream_);
    }
}

void ConnectionPair::RefreshEndpointInterests(Endpoint& endpoint) {
    if (closed_ || endpoint.Token() == 0) {
        return;
    }

    const std::uint32_t desired = DesiredInterests(endpoint.Side());
    if (desired == endpoint.Interests()) {
        return;
    }

    loop_.Modify(endpoint.Token(), desired);
    endpoint.SetInterests(desired);
}

bool ConnectionPair::ShutdownWrite(Direction& direction) noexcept {
    Endpoint& destination = GetEndpoint(direction.destination);

    for (;;) {
        if (::shutdown(destination.Fd(), SHUT_WR) == 0) {
            return true;
        }

        const int error_number = errno;
        if (error_number == EINTR) {
            continue;
        }

        return false;
    }
}

void ConnectionPair::Cleanup() noexcept {
    if (client_.Token() != 0) {
        (void)loop_.Remove(client_.Token());
        client_.SetToken(0);
    }

    if (upstream_.Token() != 0) {
        (void)loop_.Remove(upstream_.Token());
        upstream_.SetToken(0);
    }

    client_.SetInterests(0);
    upstream_.SetInterests(0);

    client_.CloseFd();
    upstream_.CloseFd();
    started_ = false;
}

}  // namespace chaosproxy
