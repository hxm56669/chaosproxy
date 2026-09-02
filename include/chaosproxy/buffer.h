#pragma once

#include <cstddef>
#include <vector>

namespace chaosproxy {

class Buffer final {
public:
    explicit Buffer(std::size_t capacity);

    [[nodiscard]] std::size_t Size() const noexcept;
    [[nodiscard]] std::size_t Capacity() const noexcept;
    [[nodiscard]] std::size_t Available() const noexcept;
    [[nodiscard]] bool Empty() const noexcept;

    [[nodiscard]] const char* FrontData() const noexcept;
    [[nodiscard]] std::size_t FrontSize() const noexcept;

    [[nodiscard]] bool Append(
        const void* data,
        std::size_t size) noexcept;

    void Consume(std::size_t size) noexcept;
    void Clear() noexcept;

private:
    std::vector<char> storage_;
    std::size_t head_{0};
    std::size_t size_{0};
};

}  // namespace chaosproxy
