#pragma once

#include "chaosproxy/event_loop.h"
#include "chaosproxy/unique_fd.h"

#include <cstdint>

namespace chaosproxy {

enum class EndpointSide {
    kClient,
    kUpstream
};

enum class EndpointState {
    kConnecting,
    kEstablished,
    kClosed
};

class Endpoint final {
public:
    Endpoint(
        EndpointSide side,
        UniqueFd fd,
        EndpointState state) noexcept;

    [[nodiscard]] EndpointSide Side() const noexcept;
    [[nodiscard]] EndpointState State() const noexcept;
    void SetState(EndpointState state) noexcept;

    [[nodiscard]] int Fd() const noexcept;
    [[nodiscard]] bool HasFd() const noexcept;
    void CloseFd() noexcept;

    [[nodiscard]] EventToken Token() const noexcept;
    void SetToken(EventToken token) noexcept;

    [[nodiscard]] std::uint32_t Interests() const noexcept;
    void SetInterests(std::uint32_t interests) noexcept;

private:
    EndpointSide side_;
    UniqueFd fd_;
    EndpointState state_;
    EventToken token_{0};
    std::uint32_t interests_{0};
};

}  // namespace chaosproxy
