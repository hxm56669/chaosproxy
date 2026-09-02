#pragma once

#include "chaosproxy/event_loop.h"
#include "chaosproxy/socket_ops.h"
#include "chaosproxy/unique_fd.h"

#include <sys/socket.h>

#include <array>
#include <cstddef>
#include <functional>
#include <string>
#include <utility>

namespace chaosproxy::test {

inline std::pair<UniqueFd, UniqueFd> MakeSocketPair() {
    int raw_fds[2]{};
    if (::socketpair(
            AF_UNIX,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            raw_fds) < 0) {
        return {};
    }

    return {
        UniqueFd(raw_fds[0]),
        UniqueFd(raw_fds[1])
    };
}

inline bool PumpUntil(
    EventLoop& loop,
    const std::function<bool()>& condition,
    int max_rounds = 200,
    int timeout_ms = 10) {
    for (int round = 0; round < max_rounds; ++round) {
        if (condition()) {
            return true;
        }
        (void)loop.RunOnce(timeout_ms);
    }
    return condition();
}

inline std::string DrainAvailable(int fd) {
    std::string output;
    std::array<char, 4096> buffer{};

    for (;;) {
        const ReadResult result = TryRead(
            fd,
            buffer.data(),
            buffer.size());

        if (result.status == ReadStatus::kData) {
            output.append(buffer.data(), result.bytes_transferred);
            continue;
        }

        break;
    }

    return output;
}

inline bool WriteAllWithLoop(
    EventLoop& loop,
    int fd,
    const std::string& data,
    int max_rounds = 500) {
    std::size_t offset = 0;

    for (int round = 0;
         round < max_rounds && offset < data.size();
         ++round) {
        const WriteResult result = TryWrite(
            fd,
            data.data() + offset,
            data.size() - offset);

        if (result.status == WriteStatus::kWritten) {
            offset += result.bytes_transferred;
        } else if (result.status != WriteStatus::kWouldBlock) {
            return false;
        }

        (void)loop.RunOnce(1);
    }

    return offset == data.size();
}

}  // namespace chaosproxy::test
