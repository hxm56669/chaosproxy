#pragma once

#include "chaosproxy/common/status_or.h"
#include "chaosproxy/proxy/connection_pair.h"
#include "chaosproxy/proxy/types.h"

#include <cstdint>
#include <memory>
#include <vector>

namespace chaosproxy {

class ConnectionTable {
public:
    StatusOr<ConnectionToken> Insert(std::unique_ptr<ConnectionPair> pair);
    ConnectionPair* Find(ConnectionToken token) noexcept;
    void Retire(ConnectionToken token);
    void ReclaimRetired();

private:
    struct Slot {
        std::unique_ptr<ConnectionPair> pair;
        std::uint32_t generation = 0;
        bool retired = false;
    };

    std::vector<Slot> slots_;
    std::vector<std::size_t> retired_slots_;
};

}  // namespace chaosproxy
