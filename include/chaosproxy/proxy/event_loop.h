#pragma once

#include "chaosproxy/common/status.h"
#include "chaosproxy/proxy/clock.h"
#include "chaosproxy/proxy/timer_queue.h"
#include "chaosproxy/proxy/types.h"

#include <cstdint>
#include <deque>
#include <functional>
#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace chaosproxy {

enum class WakeReason { kReadable, kWritable, kError, kTimer };

struct EventToken {
    std::uint64_t packed = 0;
};

struct ReadyItem {
    ConnectionToken connection;
    DirectionId direction = DirectionId::kClientToUpstream;
    WakeReason reason = WakeReason::kReadable;
};

class EventLoop {
public:
    using EventHandler = std::function<void(EventToken, std::uint32_t)>;

    explicit EventLoop(EventHandler handler = {});
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    Status AddFd(int fd, std::uint32_t events, EventToken token);
    Status ModifyFd(int fd, std::uint32_t events);
    Status RemoveFd(int fd);
    Status AttachTimerQueue(TimerQueue* timer_queue);
    void EnqueueReady(ConnectionToken, DirectionId, WakeReason);
    std::optional<ReadyItem> PopReady();
    std::size_t ReadyCount() const noexcept { return ready_queue_.size(); }
    void RunOnce(int timeout_ms);
    void Run();
    void RequestStop();

private:
    struct Registration {
        int fd = -1;
        EventToken token;
    };
    struct ReadyKey {
        ConnectionToken connection;
        DirectionId direction;

        bool operator==(const ReadyKey& other) const noexcept {
            return connection.slot == other.connection.slot &&
                   connection.generation == other.connection.generation &&
                   direction == other.direction;
        }
    };
    struct ReadyKeyHash {
        std::size_t operator()(const ReadyKey& key) const noexcept;
    };
    void DrainWakeFd();
    void ArmTimerFd(TimePoint now);

    int epoll_fd_ = -1;
    int wake_fd_ = -1;
    int timer_fd_ = -1;
    std::uint64_t next_registration_id_ = 1;
    bool stop_requested_ = false;
    EventHandler handler_;
    TimerQueue* timer_queue_ = nullptr;
    std::unordered_map<int, std::uint64_t> fd_to_registration_;
    std::unordered_map<std::uint64_t, Registration> registrations_;
    std::deque<ReadyItem> ready_queue_;
    std::unordered_set<ReadyKey, ReadyKeyHash> ready_keys_;
};

}  // namespace chaosproxy
