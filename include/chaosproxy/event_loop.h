#pragma once

#include "chaosproxy/unique_fd.h"

#include <sys/epoll.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <vector>

namespace chaosproxy {

using EventToken = std::uint64_t;
using EventCallback = std::function<void(EventToken, std::uint32_t)>;

class EventLoop final {
public:
    explicit EventLoop(std::size_t max_events = 128);

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    [[nodiscard]] EventToken Add(
        int fd,
        std::uint32_t interests,
        EventCallback callback);

    void Modify(EventToken token, std::uint32_t interests);
    [[nodiscard]] int Remove(EventToken token) noexcept;
    [[nodiscard]] bool Contains(EventToken token) const noexcept;

    int RunOnce(int timeout_ms);
    void Run();
    void Stop() noexcept;

private:
    struct Registration {
        int fd;
        std::uint32_t interests;
        EventCallback callback;
    };

    [[nodiscard]] EventToken NextToken();

    UniqueFd epoll_fd_;
    std::vector<epoll_event> ready_events_;
    std::unordered_map<EventToken, Registration> registrations_;
    EventToken next_token_{1};
    bool stop_requested_{false};
};

}  // namespace chaosproxy
