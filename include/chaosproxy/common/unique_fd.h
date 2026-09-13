#pragma once

#include <unistd.h>

namespace chaosproxy {

class UniqueFd {
public:
    UniqueFd() noexcept = default;
    explicit UniqueFd(int fd) noexcept : fd_(fd) {}

    ~UniqueFd() {
        reset();
    }

    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;

    UniqueFd(UniqueFd&& other) noexcept : fd_(other.release()) {}

    UniqueFd& operator=(UniqueFd&& other) noexcept {
        if (this != &other) {
            reset(other.release());
        }
        return *this;
    }

    int get() const noexcept { return fd_; }

    int release() noexcept {
        const int released = fd_;
        fd_ = -1;
        return released;
    }

    void reset(int replacement = -1) noexcept {
        if (fd_ != replacement) {
            if (fd_ >= 0) {
                // Do not retry close after EINTR: the numeric descriptor may
                // already have been reused by the kernel.
                (void)::close(fd_);
            }
            fd_ = replacement;
        }
    }

    explicit operator bool() const noexcept { return fd_ >= 0; }

private:
    int fd_ = -1;
};

}  // namespace chaosproxy
