#include "chaosproxy/endpoint.h"

#include <utility>

namespace chaosproxy {

Endpoint::Endpoint(
    EndpointSide side,
    UniqueFd fd,
    EndpointState state) noexcept
    : side_(side),
      fd_(std::move(fd)),
      state_(state) {}

EndpointSide Endpoint::Side() const noexcept {
    return side_;
}

EndpointState Endpoint::State() const noexcept {
    return state_;
}

void Endpoint::SetState(EndpointState state) noexcept {
    state_ = state;
}

int Endpoint::Fd() const noexcept {
    return fd_.Get();
}

bool Endpoint::HasFd() const noexcept {
    return fd_.IsValid();
}

void Endpoint::CloseFd() noexcept {
    fd_.Reset();
    state_ = EndpointState::kClosed;
}

EventToken Endpoint::Token() const noexcept {
    return token_;
}

void Endpoint::SetToken(EventToken token) noexcept {
    token_ = token;
}

std::uint32_t Endpoint::Interests() const noexcept {
    return interests_;
}

void Endpoint::SetInterests(std::uint32_t interests) noexcept {
    interests_ = interests;
}

}  // namespace chaosproxy
