#include "chaosproxy/proxy/connection_table.h"

#include "chaosproxy/proxy/connection_pair.h"

#include <limits>
#include <utility>

namespace chaosproxy {

StatusOr<ConnectionToken> ConnectionTable::Insert(
    std::unique_ptr<ConnectionPair> pair) {
    if (!pair) {
        return Status(StatusCode::kInvalidArgument,
                      "connection pair must not be null");
    }

    for (std::size_t slot_index = 0; slot_index < slots_.size(); ++slot_index) {
        Slot& slot = slots_[slot_index];
        if (!slot.pair && !slot.retired) {
            if (slot.generation == std::numeric_limits<std::uint32_t>::max()) {
                return Status(StatusCode::kResourceExhausted,
                              "connection generation exhausted");
            }
            ++slot.generation;
            slot.pair = std::move(pair);
            return ConnectionToken{static_cast<std::uint32_t>(slot_index),
                                   slot.generation};
        }
    }

    Slot slot;
    slot.generation = 1;
    slot.pair = std::move(pair);
    slots_.push_back(std::move(slot));
    return ConnectionToken{static_cast<std::uint32_t>(slots_.size() - 1), 1};
}

ConnectionPair* ConnectionTable::Find(ConnectionToken token) noexcept {
    if (token.slot >= slots_.size()) {
        return nullptr;
    }
    Slot& slot = slots_[token.slot];
    if (slot.retired || slot.generation != token.generation) {
        return nullptr;
    }
    return slot.pair.get();
}

void ConnectionTable::Retire(ConnectionToken token) {
    if (token.slot >= slots_.size()) {
        return;
    }
    Slot& slot = slots_[token.slot];
    if (slot.generation != token.generation || slot.retired || !slot.pair) {
        return;
    }
    slot.retired = true;
    retired_slots_.push_back(token.slot);
}

void ConnectionTable::ReclaimRetired() {
    for (const std::size_t slot_index : retired_slots_) {
        if (slot_index < slots_.size()) {
            Slot& slot = slots_[slot_index];
            slot.pair.reset();
            slot.retired = false;
        }
    }
    retired_slots_.clear();
}

}  // namespace chaosproxy
