#include "chaosproxy/proxy/connection_pair.h"

#include "chaosproxy/proxy/socket_ops.h"

#include <algorithm>
#include <utility>

namespace chaosproxy {

ConnectionPair::ConnectionPair(UniqueFd client, UniqueFd upstream)
    : client_{std::move(client)}, upstream_{std::move(upstream)} {}

Endpoint& ConnectionPair::endpoint(EndpointSide side) noexcept {
    return side == EndpointSide::kClient ? client_ : upstream_;
}

const Endpoint& ConnectionPair::endpoint(EndpointSide side) const noexcept {
    return side == EndpointSide::kClient ? client_ : upstream_;
}

ConnectionPair::DirectionState& ConnectionPair::direction(
    DirectionId id) noexcept {
    return id == DirectionId::kClientToUpstream ? client_to_upstream_
                                                 : upstream_to_client_;
}

const ConnectionPair::DirectionState& ConnectionPair::direction(
    DirectionId id) const noexcept {
    return id == DirectionId::kClientToUpstream ? client_to_upstream_
                                                 : upstream_to_client_;
}

EndpointSide ConnectionPair::SourceSide(DirectionId id) noexcept {
    return id == DirectionId::kClientToUpstream ? EndpointSide::kClient
                                                : EndpointSide::kUpstream;
}

EndpointSide ConnectionPair::DestinationSide(DirectionId id) noexcept {
    return id == DirectionId::kClientToUpstream ? EndpointSide::kUpstream
                                                : EndpointSide::kClient;
}

void ConnectionPair::OnReadable(EndpointSide side) {
    PumpBudget budget{64 * 1024, 64};
    Pump(side == EndpointSide::kClient ? DirectionId::kClientToUpstream
                                       : DirectionId::kUpstreamToClient,
         budget);
}

void ConnectionPair::OnWritable(EndpointSide side) {
    const DirectionId id = side == EndpointSide::kClient
                               ? DirectionId::kUpstreamToClient
                               : DirectionId::kClientToUpstream;
    PumpBudget budget{64 * 1024, 64};
    Pump(id, budget);
}

void ConnectionPair::OnEventError(EndpointSide, int) {
    Close(CloseReason::kPeerError);
}

void ConnectionPair::Pump(DirectionId id, PumpBudget& budget) {
    if (closed_) {
        return;
    }
    Endpoint& source = endpoint(SourceSide(id));
    Endpoint& destination = endpoint(DestinationSide(id));
    DirectionState& state = direction(id);

    while (budget.bytes_left > 0 && budget.calls_left > 0 && !closed_) {
        if (!state.queue.Empty()) {
            const std::span<const std::byte> front = state.queue.FrontBytes();
            const std::size_t available =
                std::min(front.size(), budget.bytes_left);
            const IoResult result = TryWrite(
                destination.fd.get(),
                front.first(available));
            --budget.calls_left;
            if (result.code == IoCode::kProgress) {
                state.queue.Consume(result.bytes);
                budget.bytes_left -= result.bytes;
                if (state.queue.Empty()) {
                    TryFinishDirection(id);
                }
                continue;
            }
            if (result.code == IoCode::kWouldBlock) {
                return;
            }
            Close(CloseReason::kPeerError);
            return;
        }

        const std::size_t capacity = std::min<std::size_t>(
            64 * 1024, budget.bytes_left);
        auto block = BufferBlock::Allocate(buffer_budget_, capacity);
        if (!block.ok()) {
            Close(CloseReason::kResourceExhausted);
            return;
        }
        const IoResult result =
            TryRead(source.fd.get(), block.value()->WritableBytes());
        --budget.calls_left;
        if (result.code == IoCode::kProgress) {
            std::shared_ptr<const BufferBlock> storage = std::move(block).value();
            Chunk chunk{std::move(storage), 0, result.bytes,
                        state.next_stream_offset, state.next_sequence++, {}};
            state.next_stream_offset += result.bytes;
            const Status status = state.queue.Push(std::move(chunk));
            if (!status.ok()) {
                Close(CloseReason::kResourceExhausted);
                return;
            }
            continue;
        }
        if (result.code == IoCode::kWouldBlock) {
            return;
        }
        if (result.code == IoCode::kEof) {
            OnSourceEof(id);
            return;
        }
        Close(CloseReason::kPeerError);
        return;
    }
}

void ConnectionPair::ResumeRead(DirectionId id) {
    PumpBudget budget{64 * 1024, 64};
    Pump(id, budget);
}

void ConnectionPair::OnSourceEof(DirectionId id) {
    DirectionState& state = direction(id);
    state.source_eof = true;
    TryFinishDirection(id);
}

void ConnectionPair::TryFinishDirection(DirectionId id) {
    if (closed_) {
        return;
    }
    const DirectionState& state = direction(id);
    Endpoint& destination = endpoint(DestinationSide(id));
    if (state.source_eof && state.queue.Empty() &&
        !destination.write_shutdown) {
        const Status status = ShutdownWrite(destination.fd.get());
        if (!status.ok()) {
            Close(CloseReason::kPeerError);
            return;
        }
        destination.write_shutdown = true;
    }
}

void ConnectionPair::RefreshInterests() {}

void ConnectionPair::Close(CloseReason reason) {
    if (closed_) {
        return;
    }
    closed_ = true;
    close_reason_ = reason;
    client_.state = TransportState::kClosed;
    upstream_.state = TransportState::kClosed;
    client_to_upstream_.queue.Clear();
    upstream_to_client_.queue.Clear();
    client_.fd.reset();
    upstream_.fd.reset();
}

bool ConnectionPair::closed() const noexcept {
    return closed_;
}

}  // namespace chaosproxy
