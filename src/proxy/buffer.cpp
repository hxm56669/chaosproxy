#include "chaosproxy/proxy/buffer.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace chaosproxy {

bool BufferBudget::TryReserve(std::size_t capacity) noexcept {
    std::size_t current = used_.load(std::memory_order_relaxed);
    while (current <= limit_) {
        if (capacity > limit_ - current) {
            return false;
        }
        if (used_.compare_exchange_weak(current, current + capacity,
                                         std::memory_order_acq_rel,
                                         std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

void BufferBudget::Release(std::size_t capacity) noexcept {
    const std::size_t previous =
        used_.fetch_sub(capacity, std::memory_order_acq_rel);
    if (previous < capacity) {
        used_.store(0, std::memory_order_release);
    }
}

std::size_t BufferBudget::Used() const noexcept {
    return used_.load(std::memory_order_acquire);
}

BufferBlock::BufferBlock(std::shared_ptr<BufferBudget> owner,
                         std::size_t capacity)
    : bytes(capacity), budget(std::move(owner)) {}

BufferBlock::~BufferBlock() {
    if (budget) {
        budget->Release(bytes.size());
    }
}

StatusOr<std::shared_ptr<BufferBlock>> BufferBlock::Allocate(
    std::shared_ptr<BufferBudget> budget, std::size_t capacity) {
    if (!budget || capacity == 0) {
        return Status(StatusCode::kInvalidArgument,
                      "buffer budget and positive capacity are required");
    }
    if (!budget->TryReserve(capacity)) {
        return Status(StatusCode::kResourceExhausted,
                      "buffer allocation budget exhausted");
    }
    const std::shared_ptr<BufferBudget> owner = budget;
    try {
        return std::shared_ptr<BufferBlock>(new BufferBlock(owner, capacity));
    } catch (...) {
        owner->Release(capacity);
        return Status(StatusCode::kResourceExhausted,
                      "buffer allocation failed");
    }
}

Status ByteQueue::Push(Chunk chunk) {
    if (!chunk.storage || chunk.length == 0 ||
        chunk.offset > chunk.storage->bytes.size() ||
        chunk.length > chunk.storage->bytes.size() - chunk.offset) {
        return Status(StatusCode::kInvalidArgument,
                      "chunk range is outside its buffer block");
    }
    if (queued_bytes_ > std::numeric_limits<std::size_t>::max() -
                            chunk.length) {
        return Status(StatusCode::kResourceExhausted,
                      "byte queue size overflow");
    }
    queued_bytes_ += chunk.length;
    chunks_.push_back(std::move(chunk));
    return Status::Ok();
}

std::span<const std::byte> ByteQueue::FrontBytes() const {
    if (chunks_.empty()) {
        return {};
    }
    const Chunk& chunk = chunks_.front();
    return std::span<const std::byte>(
        chunk.storage->bytes.data() + chunk.offset, chunk.length);
}

void ByteQueue::Consume(std::size_t bytes) {
    if (bytes > queued_bytes_) {
        bytes = queued_bytes_;
    }
    while (bytes > 0 && !chunks_.empty()) {
        Chunk& chunk = chunks_.front();
        const std::size_t consumed = std::min(bytes, chunk.length);
        chunk.offset += consumed;
        chunk.length -= consumed;
        bytes -= consumed;
        queued_bytes_ -= consumed;
        if (chunk.length == 0) {
            chunks_.pop_front();
        }
    }
}

void ByteQueue::Clear() noexcept {
    chunks_.clear();
    queued_bytes_ = 0;
}

}  // namespace chaosproxy
