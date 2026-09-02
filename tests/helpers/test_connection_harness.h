#pragma once

#include "chaosproxy/connection.h"

#include <memory>
#include <utility>

namespace chaosproxy::test {

class DirectConnectionHarness final {
public:
    DirectConnectionHarness(
        EventLoop& loop,
        ConnectionToken token,
        UniqueFd client_fd,
        UniqueFd upstream_fd,
        EndpointState upstream_state,
        ConnectionLimits limits,
        ConnectionPair::ClosedCallback closed_callback) {
        connection_ = std::make_unique<ConnectionPair>(
            loop,
            token,
            std::move(client_fd),
            std::move(upstream_fd),
            upstream_state,
            limits,
            [this](
                ConnectionToken observed_connection,
                EndpointSide side,
                EventToken event_token,
                std::uint32_t events) {
                if (connection_ == nullptr ||
                    observed_connection != connection_->Token()) {
                    return;
                }
                connection_->OnEvent(side, event_token, events);
            },
            std::move(closed_callback));
    }

    DirectConnectionHarness(const DirectConnectionHarness&) = delete;
    DirectConnectionHarness& operator=(
        const DirectConnectionHarness&) = delete;

    void Start() {
        connection_->Start();
    }

    [[nodiscard]] ConnectionPair& Connection() noexcept {
        return *connection_;
    }

    [[nodiscard]] const ConnectionPair& Connection() const noexcept {
        return *connection_;
    }

private:
    std::unique_ptr<ConnectionPair> connection_;
};

}  // namespace chaosproxy::test
