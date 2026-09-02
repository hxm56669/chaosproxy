#include "chaosproxy/buffer.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace chaosproxy {

Buffer::Buffer(std::size_t capacity)
    : storage_(capacity) {
    if (capacity == 0) {
        throw std::invalid_argument("buffer capacity must be positive");
    }
}

std::size_t Buffer::Size() const noexcept {
    return size_;
}

std::size_t Buffer::Capacity() const noexcept {
    return storage_.size();
}

std::size_t Buffer::Available() const noexcept {
    return Capacity() - size_;
}

bool Buffer::Empty() const noexcept {
    return size_ == 0;
}

const char* Buffer::FrontData() const noexcept {
    if (Empty()) {
        return nullptr;
    }
    return storage_.data() + head_;
}

std::size_t Buffer::FrontSize() const noexcept {
    if (Empty()) {
        return 0;
    }

    return std::min(size_, Capacity() - head_);
}

bool Buffer::Append(
    const void* data,
    std::size_t size) noexcept {
    if (size == 0) {
        return true;
    }

    if (data == nullptr || size > Available()) {
        return false;
    }

    const auto* bytes = static_cast<const char*>(data);
    const std::size_t tail = (head_ + size_) % Capacity();
    const std::size_t first = std::min(size, Capacity() - tail);

    std::memcpy(storage_.data() + tail, bytes, first);

    const std::size_t second = size - first;
    if (second > 0) {
        std::memcpy(storage_.data(), bytes + first, second);
    }

    size_ += size;
    return true;
}

void Buffer::Consume(std::size_t size) noexcept {
    const std::size_t consumed = std::min(size, size_);
    head_ = (head_ + consumed) % Capacity();
    size_ -= consumed;

    if (size_ == 0) {
        head_ = 0;
    }
}

void Buffer::Clear() noexcept {
    head_ = 0;
    size_ = 0;
}

}  // namespace chaosproxy
