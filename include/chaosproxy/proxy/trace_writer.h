#pragma once

#include "chaosproxy/proxy/clock.h"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>

namespace chaosproxy {

struct TraceEvent {
    std::string run_id;
    std::string type;
    std::uint64_t policy_version = 0;
    std::uint64_t connection_id = 0;
    std::uint64_t byte_offset = 0;
    std::string detail;
};

struct TraceStats {
    std::size_t queued = 0;
    std::uint64_t written = 0;
    std::uint64_t dropped = 0;
    bool incomplete = false;
};

class TraceWriter {
public:
    explicit TraceWriter(std::filesystem::path path, std::size_t capacity = 1024);
    ~TraceWriter();

    TraceWriter(const TraceWriter&) = delete;
    TraceWriter& operator=(const TraceWriter&) = delete;

    bool TryAppend(const TraceEvent& event);
    void FlushUntil(TimePoint deadline);
    TraceStats Snapshot() const;

private:
    void Run();
    static std::string Encode(const TraceEvent& event);

    const std::filesystem::path path_;
    const std::size_t capacity_;
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::condition_variable drained_;
    std::deque<TraceEvent> queue_;
    std::thread worker_;
    bool stopping_ = false;
    std::atomic<std::uint64_t> written_{0};
    std::atomic<std::uint64_t> dropped_{0};
};

}  // namespace chaosproxy
