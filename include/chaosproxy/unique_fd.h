#pragma once

#include <unistd.h>

#include <utility>

namespace chaosproxy {

class UniqueFd final {
public:
    UniqueFd() noexcept = default;

    explicit UniqueFd(int fd) noexcept
        : fd_(fd) {}

    ~UniqueFd() noexcept {
        Reset();
    }

    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;

    UniqueFd(UniqueFd&& other) noexcept
        : fd_(other.Release()) {}

    UniqueFd& operator=(UniqueFd&& other) noexcept {
        if (this != &other) {
            Reset(other.Release());
        }
        return *this;
    }

    [[nodiscard]] int Get() const noexcept {
        return fd_;
    }

    [[nodiscard]] bool IsValid() const noexcept {
        return fd_ >= 0;
    }

    [[nodiscard]] int Release() noexcept {
        return std::exchange(fd_, -1);
    }

    void Reset(int fd = -1) noexcept {
        if (fd_ == fd) {
            return;
        }
        const int old_fd = std::exchange(fd_, fd);
        if (old_fd >= 0) {
            (void)::close(old_fd);
        }
    }

private:
    int fd_{-1};
};

}  // namespace chaosproxy
