#include "chaosproxy/proxy/trace_writer.h"

#include <fstream>

namespace chaosproxy {

TraceWriter::TraceWriter(std::filesystem::path path, std::size_t capacity)
    : path_(std::move(path)), capacity_(capacity), worker_(&TraceWriter::Run, this) {}

TraceWriter::~TraceWriter() {
    {
        std::lock_guard lock(mutex_);
        stopping_ = true;
    }
    condition_.notify_one();
    if (worker_.joinable()) worker_.join();
}

bool TraceWriter::TryAppend(const TraceEvent& event) {
    std::lock_guard lock(mutex_);
    if (queue_.size() >= capacity_ || stopping_) {
        dropped_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    queue_.push_back(event);
    condition_.notify_one();
    return true;
}

void TraceWriter::FlushUntil(TimePoint deadline) {
    std::unique_lock lock(mutex_);
    drained_.wait_until(lock, deadline, [this] { return queue_.empty(); });
}

TraceStats TraceWriter::Snapshot() const {
    std::lock_guard lock(mutex_);
    return TraceStats{queue_.size(), written_.load(std::memory_order_relaxed),
                      dropped_.load(std::memory_order_relaxed),
                      dropped_.load(std::memory_order_relaxed) != 0};
}

std::string TraceWriter::Encode(const TraceEvent& event) {
    return event.run_id + "\t" + event.type + "\t" +
           std::to_string(event.policy_version) + "\t" +
           std::to_string(event.connection_id) + "\t" +
           std::to_string(event.byte_offset) + "\t" + event.detail + "\n";
}

void TraceWriter::Run() {
    std::ofstream output(path_, std::ios::app);
    for (;;) {
        TraceEvent event;
        {
            std::unique_lock lock(mutex_);
            condition_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
            if (queue_.empty() && stopping_) return;
            event = std::move(queue_.front());
            queue_.pop_front();
        }
        if (output) {
            output << Encode(event);
            output.flush();
            written_.fetch_add(1, std::memory_order_relaxed);
        } else {
            dropped_.fetch_add(1, std::memory_order_relaxed);
        }
        drained_.notify_all();
    }
}

}  // namespace chaosproxy
