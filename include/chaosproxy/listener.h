#pragma once

#include "chaosproxy/event_loop.h"
#include "chaosproxy/unique_fd.h"

#include <functional>

namespace chaosproxy {

class Listener final {
public:
    using AcceptedCallback = std::function<void(UniqueFd)>;
    using ErrorCallback = std::function<void(int)>;

    Listener(
        EventLoop& loop,
        UniqueFd listen_fd,
        AcceptedCallback accepted_callback,
        ErrorCallback error_callback);

    ~Listener() noexcept;

    Listener(const Listener&) = delete;
    Listener& operator=(const Listener&) = delete;

    [[nodiscard]] int Fd() const noexcept;
    [[nodiscard]] bool IsStarted() const noexcept;
    void Stop() noexcept;

private:
    void OnEvent(EventToken token, std::uint32_t events) noexcept;

    EventLoop& loop_;
    UniqueFd listen_fd_;
    AcceptedCallback accepted_callback_;
    ErrorCallback error_callback_;
    EventToken token_{0};
};

}  // namespace chaosproxy
