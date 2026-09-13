#include "chaosproxy/proxy/event_loop.h"

#include <cerrno>
#include <cstdint>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <algorithm>
#include <climits>
#include <chrono>
#include <utility>

namespace chaosproxy {

EventLoop::EventLoop(EventHandler handler) : handler_(std::move(handler)) {
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        return;
    }
    wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd_ < 0) {
        const int error = errno;
        (void)error;
        ::close(epoll_fd_);
        epoll_fd_ = -1;
        return;
    }
    timer_fd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timer_fd_ < 0) {
        ::close(wake_fd_);
        wake_fd_ = -1;
        ::close(epoll_fd_);
        epoll_fd_ = -1;
        return;
    }
    epoll_event event{};
    event.events = EPOLLIN;
    event.data.u64 = 0;
    (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &event);
    event.events = EPOLLIN;
    event.data.u64 = UINT64_MAX;
    (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, timer_fd_, &event);
}

EventLoop::~EventLoop() {
    if (wake_fd_ >= 0) {
        (void)::close(wake_fd_);
    }
    if (timer_fd_ >= 0) {
        (void)::close(timer_fd_);
    }
    if (epoll_fd_ >= 0) {
        (void)::close(epoll_fd_);
    }
}

Status EventLoop::AttachTimerQueue(TimerQueue* timer_queue) {
    if (epoll_fd_ < 0 || timer_fd_ < 0 || timer_queue == nullptr) {
        return Status(StatusCode::kInvalidArgument,
                      "event loop and timer queue must be valid");
    }
    timer_queue_ = timer_queue;
    return Status::Ok();
}

Status EventLoop::AddFd(int fd, std::uint32_t events, EventToken token) {
    if (fd < 0 || epoll_fd_ < 0) {
        return Status(StatusCode::kInvalidArgument,
                      "event loop and fd must be valid");
    }
    if (fd_to_registration_.contains(fd)) {
        return Status(StatusCode::kConflict, "fd is already registered");
    }
    if (next_registration_id_ == 0) {
        return Status(StatusCode::kResourceExhausted,
                      "event registration id exhausted");
    }

    const std::uint64_t registration_id = next_registration_id_++;
    epoll_event event{};
    event.events = events;
    event.data.u64 = registration_id;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) < 0) {
        const int error = errno;
        return StatusFromErrno(error, "epoll_ctl(ADD)");
    }
    fd_to_registration_.emplace(fd, registration_id);
    registrations_.emplace(registration_id, Registration{fd, token});
    return Status::Ok();
}

Status EventLoop::ModifyFd(int fd, std::uint32_t events) {
    if (fd < 0 || epoll_fd_ < 0) {
        return Status(StatusCode::kInvalidArgument,
                      "event loop and fd must be valid");
    }
    if (!fd_to_registration_.contains(fd)) {
        return Status(StatusCode::kNotFound, "fd is not registered");
    }
    epoll_event event{};
    event.events = events;
    event.data.u64 = fd_to_registration_.at(fd);
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &event) < 0) {
        const int error = errno;
        return StatusFromErrno(error, "epoll_ctl(MOD)");
    }
    return Status::Ok();
}

Status EventLoop::RemoveFd(int fd) {
    if (fd < 0 || epoll_fd_ < 0) {
        return Status(StatusCode::kInvalidArgument,
                      "event loop and fd must be valid");
    }
    const auto registration = fd_to_registration_.find(fd);
    if (registration == fd_to_registration_.end()) {
        return Status(StatusCode::kNotFound, "fd is not registered");
    }
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) < 0) {
        const int error = errno;
        return StatusFromErrno(error, "epoll_ctl(DEL)");
    }
    registrations_.erase(registration->second);
    fd_to_registration_.erase(registration);
    return Status::Ok();
}

void EventLoop::EnqueueReady(ConnectionToken connection, DirectionId direction,
                             WakeReason reason) {
    const ReadyKey key{connection, direction};
    if (ready_keys_.insert(key).second) {
        ready_queue_.push_back(ReadyItem{connection, direction, reason});
    }
}

std::size_t EventLoop::ReadyKeyHash::operator()(
    const ReadyKey& key) const noexcept {
    const std::size_t slot = static_cast<std::size_t>(key.connection.slot);
    const std::size_t generation =
        static_cast<std::size_t>(key.connection.generation);
    const std::size_t direction =
        static_cast<std::size_t>(key.direction == DirectionId::kClientToUpstream);
    return slot ^ (generation * static_cast<std::size_t>(0x9e3779b9U)) ^
           (direction << 1U);
}

std::optional<ReadyItem> EventLoop::PopReady() {
    if (ready_queue_.empty()) {
        return std::nullopt;
    }
    ReadyItem item = ready_queue_.front();
    ready_queue_.pop_front();
    ready_keys_.erase(ReadyKey{item.connection, item.direction});
    return item;
}

void EventLoop::DrainWakeFd() {
    if (wake_fd_ < 0) {
        return;
    }
    std::uint64_t value = 0;
    while (::read(wake_fd_, &value, sizeof(value)) == sizeof(value)) {
    }
}

void EventLoop::ArmTimerFd(TimePoint now) {
    if (timer_fd_ < 0 || timer_queue_ == nullptr) {
        return;
    }
    itimerspec spec{};
    const auto deadline = timer_queue_->NextDeadline();
    if (deadline.has_value()) {
        const auto remaining = std::max(Duration::zero(), *deadline - now);
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            remaining);
        const auto safe_ns = std::max<std::int64_t>(1, ns.count());
        spec.it_value.tv_sec = static_cast<time_t>(safe_ns / 1000000000LL);
        spec.it_value.tv_nsec = static_cast<long>(safe_ns % 1000000000LL);
    }
    (void)::timerfd_settime(timer_fd_, 0, &spec, nullptr);
}

void EventLoop::RunOnce(int timeout_ms) {
    if (epoll_fd_ < 0) {
        return;
    }
    ArmTimerFd(Clock::now());
    epoll_event events[64]{};
    int effective_timeout = timeout_ms;
    if (timer_queue_ != nullptr) {
        const auto deadline = timer_queue_->NextDeadline();
        if (deadline.has_value()) {
            const auto remaining_duration = *deadline - Clock::now();
            auto remaining =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    remaining_duration)
                    .count();
            if (remaining_duration > std::chrono::milliseconds(remaining)) {
                ++remaining;
            }
            const int timer_timeout = static_cast<int>(std::clamp<std::int64_t>(
                remaining, 0, static_cast<std::int64_t>(INT_MAX)));
            if (effective_timeout < 0 || timer_timeout < effective_timeout) {
                effective_timeout = timer_timeout;
            }
        }
    }
    const int count = ::epoll_wait(epoll_fd_, events, 64, effective_timeout);
    if (count < 0) {
        if (errno == EINTR) {
            return;
        }
        return;
    }
    for (int index = 0; index < count; ++index) {
        const std::uint64_t registration_id = events[index].data.u64;
        if (registration_id == 0) {
            DrainWakeFd();
            continue;
        }
        if (registration_id == UINT64_MAX) {
            std::uint64_t expirations = 0;
            (void)::read(timer_fd_, &expirations, sizeof(expirations));
            if (timer_queue_ != nullptr) {
                (void)timer_queue_->RunExpired(Clock::now(), 64);
            }
            continue;
        }
        const auto registration = registrations_.find(registration_id);
        if (registration != registrations_.end() && handler_) {
            handler_(registration->second.token, events[index].events);
        }
    }
    if (timer_queue_ != nullptr) {
        (void)timer_queue_->RunExpired(Clock::now(), 64);
    }
}

void EventLoop::Run() {
    while (!stop_requested_) {
        RunOnce(-1);
    }
}

void EventLoop::RequestStop() {
    stop_requested_ = true;
    if (wake_fd_ >= 0) {
        const std::uint64_t value = 1;
        (void)::write(wake_fd_, &value, sizeof(value));
    }
}

}  // namespace chaosproxy
