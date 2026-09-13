#pragma once

#include "chaosproxy/common/unique_fd.h"
#include "chaosproxy/proxy/buffer.h"
#include "chaosproxy/proxy/event_loop.h"
#include "chaosproxy/proxy/types.h"

#include <cstddef>
#include <cstdint>
#include <memory>

namespace chaosproxy {

struct PumpBudget {
    std::size_t bytes_left;
    std::uint32_t calls_left;
};

enum class CloseReason {
    kPeerError,
    kProtocolError,
    kResourceExhausted,
    kShutdown,
};

struct Endpoint {
    UniqueFd fd;
    TransportState state = TransportState::kEstablished;
    bool read_eof = false;
    bool write_shutdown = false;
    std::uint32_t subscribed_events = 0;
};

class ConnectionPair {
public:
    ConnectionPair(UniqueFd client, UniqueFd upstream);

    ConnectionPair(const ConnectionPair&) = delete;
    ConnectionPair& operator=(const ConnectionPair&) = delete;

    void OnReadable(EndpointSide side);
    void OnWritable(EndpointSide side);
    void OnEventError(EndpointSide side, int error);
    void Pump(DirectionId direction, PumpBudget& budget);
    void ResumeRead(DirectionId direction);
    void OnSourceEof(DirectionId direction);
    void TryFinishDirection(DirectionId direction);
    void RefreshInterests();
    void Close(CloseReason reason);

    bool closed() const noexcept;
    const Endpoint& endpoint(EndpointSide side) const noexcept;

private:
    struct DirectionState {
        ByteQueue queue;
        bool source_eof = false;
        std::uint64_t next_stream_offset = 0;
        std::uint64_t next_sequence = 0;
    };

    Endpoint& endpoint(EndpointSide side) noexcept;
    DirectionState& direction(DirectionId id) noexcept;
    const DirectionState& direction(DirectionId id) const noexcept;
    static EndpointSide SourceSide(DirectionId id) noexcept;
    static EndpointSide DestinationSide(DirectionId id) noexcept;

    Endpoint client_;
    Endpoint upstream_;
    DirectionState client_to_upstream_;
    DirectionState upstream_to_client_;
    bool closed_ = false;
    CloseReason close_reason_ = CloseReason::kShutdown;
    std::shared_ptr<BufferBudget> buffer_budget_ =
        std::make_shared<BufferBudget>(256 * 1024);
};

}  // namespace chaosproxy
