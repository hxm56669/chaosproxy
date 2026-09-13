#pragma once

#include "chaosproxy/common/status.h"
#include "chaosproxy/proxy/buffer.h"
#include "chaosproxy/proxy/timer_queue.h"
#include "chaosproxy/proxy/toxics.h"
#include "chaosproxy/proxy/types.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>

namespace chaosproxy {

struct DirectionContext {
    ConnectionToken connection;
    DirectionId direction = DirectionId::kClientToUpstream;
    TimePoint now = Clock::now();
    ClockSource* clock = nullptr;
    TimerQueue* timers = nullptr;
    RandomSource* random = nullptr;
    std::function<Status(const Chunk&)> emit;
    std::function<void()> close;
    std::function<void()> reset;
    bool paused = false;
    std::shared_ptr<std::size_t> successful_bytes =
        std::make_shared<std::size_t>(0);
};

struct ContinuationToken {
    ConnectionToken connection;
    DirectionId direction = DirectionId::kClientToUpstream;
    std::uint64_t operation_id = 0;
    std::uint64_t policy_version = 0;
    std::size_t next_stage = 0;
};

class ToxicPipeline {
public:
    ~ToxicPipeline() { CancelAll(); }
    Status Accept(Chunk chunk, DirectionContext& context);
    Status Resume(ContinuationToken token);
    void CancelAll();
    std::size_t PendingCount() const noexcept { return pending_.size(); }

private:
    struct Operation {
        Chunk chunk;
        DirectionContext context;
        ContinuationToken token;
        std::optional<TimerId> timer;
    };

    TimePoint Now(const DirectionContext& context) const noexcept;
    Status Process(Operation operation);
    Status Emit(Operation& operation, Chunk chunk, const ToxicRule* limit_rule);
    Status Schedule(Operation operation, TimePoint deadline);
    void OnTimeout(std::uint64_t operation_id, bool reset);

    std::unordered_map<std::uint64_t, Operation> pending_;
    std::unordered_map<std::size_t, TokenBucket> buckets_;
    std::uint64_t next_operation_id_ = 1;
};

}  // namespace chaosproxy
