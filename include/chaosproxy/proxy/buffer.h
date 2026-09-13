#pragma once

#include "chaosproxy/common/status_or.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <span>
#include <vector>

namespace chaosproxy {

struct PolicySnapshot;

class BufferBudget {
public:
    explicit BufferBudget(std::size_t limit) : limit_(limit) {}

    bool TryReserve(std::size_t capacity) noexcept;
    void Release(std::size_t capacity) noexcept;
    std::size_t Used() const noexcept;
    std::size_t Limit() const noexcept { return limit_; }

private:
    const std::size_t limit_;
    std::atomic<std::size_t> used_{0};
};

struct BufferBlock {
    static StatusOr<std::shared_ptr<BufferBlock>> Allocate(
        std::shared_ptr<BufferBudget> budget, std::size_t capacity);

    ~BufferBlock();

    std::span<std::byte> WritableBytes() noexcept {
        return std::span<std::byte>(bytes.data(), bytes.size());
    }

    std::span<const std::byte> Bytes() const noexcept {
        return std::span<const std::byte>(bytes.data(), bytes.size());
    }

    std::vector<std::byte> bytes;
    std::shared_ptr<BufferBudget> budget;

private:
    BufferBlock(std::shared_ptr<BufferBudget> owner, std::size_t capacity);
};

struct Chunk {
    std::shared_ptr<const BufferBlock> storage;
    std::size_t offset = 0;
    std::size_t length = 0;
    std::uint64_t stream_offset = 0;
    std::uint64_t sequence = 0;
    std::shared_ptr<const PolicySnapshot> policy;
};

class ByteQueue {
public:
    Status Push(Chunk chunk);
    std::span<const std::byte> FrontBytes() const;
    void Consume(std::size_t bytes);
    std::size_t QueuedBytes() const noexcept { return queued_bytes_; }
    bool Empty() const noexcept { return chunks_.empty(); }
    void Clear() noexcept;

private:
    std::deque<Chunk> chunks_;
    std::size_t queued_bytes_ = 0;
};

}  // namespace chaosproxy
