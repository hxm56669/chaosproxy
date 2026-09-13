#include "chaosproxy/proxy/toxic_pipeline.h"

#include <algorithm>
#include <utility>

namespace chaosproxy {

TimePoint ToxicPipeline::Now(const DirectionContext& context) const noexcept {
    return context.clock == nullptr ? Clock::now() : context.clock->Now();
}

Status ToxicPipeline::Accept(Chunk chunk, DirectionContext& context) {
    if (!context.emit || !chunk.storage || chunk.offset > chunk.storage->bytes.size() ||
        chunk.length > chunk.storage->bytes.size() - chunk.offset) {
        return Status(StatusCode::kInvalidArgument,
                      "toxic input requires a valid chunk and emit callback");
    }
    Operation operation{std::move(chunk), context, {}, std::nullopt};
    if (operation.chunk.policy) {
        operation.token.policy_version = operation.chunk.policy->version;
    }
    operation.token.connection = context.connection;
    operation.token.direction = context.direction;
    operation.token.operation_id = next_operation_id_++;
    return Process(std::move(operation));
}

Status ToxicPipeline::Resume(ContinuationToken token) {
    const auto it = pending_.find(token.operation_id);
    if (it == pending_.end() || it->second.token.connection.slot != token.connection.slot ||
        it->second.token.connection.generation != token.connection.generation ||
        it->second.token.direction != token.direction ||
        it->second.token.policy_version != token.policy_version) {
        return Status(StatusCode::kStaleOwner, "stale toxic continuation");
    }
    Operation operation = std::move(it->second);
    pending_.erase(it);
    if (operation.timer.has_value() && operation.context.timers != nullptr) {
        operation.context.timers->Cancel(*operation.timer);
    }
    operation.context.now = Now(operation.context);
    operation.token.next_stage = token.next_stage;
    return Process(std::move(operation));
}

Status ToxicPipeline::Emit(Operation& operation, Chunk chunk,
                           const ToxicRule* limit_rule) {
    if (limit_rule != nullptr) {
        const std::size_t remaining =
            *operation.context.successful_bytes >= limit_rule->limit_bytes
                ? 0
                : limit_rule->limit_bytes - *operation.context.successful_bytes;
        if (remaining == 0) {
            if (limit_rule->abort && operation.context.reset) {
                operation.context.reset();
            } else if (operation.context.close) {
                operation.context.close();
            }
            return Status::Ok();
        }
        chunk.length = std::min(chunk.length, remaining);
    }
    if (chunk.length == 0) {
        return Status::Ok();
    }
    Status status = operation.context.emit(chunk);
    if (status.ok()) {
        *operation.context.successful_bytes += chunk.length;
        if (limit_rule != nullptr &&
            *operation.context.successful_bytes >= limit_rule->limit_bytes) {
            if (limit_rule->abort && operation.context.reset) {
                operation.context.reset();
            } else if (operation.context.close) {
                operation.context.close();
            }
        }
    }
    return status;
}

Status ToxicPipeline::Schedule(Operation operation, TimePoint deadline) {
    if (operation.context.timers == nullptr || deadline == TimePoint::max()) {
        return Status(StatusCode::kUnavailable,
                      "toxic continuation requires a timer queue");
    }
    const std::uint64_t id = operation.token.operation_id;
    const ContinuationToken token = operation.token;
    TimerQueue* timers = operation.context.timers;
    pending_.emplace(id, std::move(operation));
    const TimerId timer = timers->Schedule(
        deadline, [this, token] { (void)Resume(token); });
    if (timer.value == 0) {
        pending_.erase(id);
        return Status(StatusCode::kResourceExhausted, "unable to schedule toxic continuation");
    }
    pending_.at(id).timer = timer;
    return Status::Ok();
}

void ToxicPipeline::OnTimeout(std::uint64_t operation_id, bool reset) {
    const auto it = pending_.find(operation_id);
    if (it == pending_.end()) {
        return;
    }
    Operation operation = std::move(it->second);
    pending_.erase(it);
    if (reset && operation.context.reset) {
        operation.context.reset();
    } else if (operation.context.close) {
        operation.context.close();
    }
}

Status ToxicPipeline::Process(Operation operation) {
    if (!operation.chunk.policy) {
        return Emit(operation, operation.chunk, nullptr);
    }
    const auto& stages = operation.chunk.policy->stages;
    std::optional<ToxicRule> limit_rule;
    for (std::size_t index = operation.token.next_stage; index < stages.size(); ++index) {
        const ToxicRule& rule = stages[index];
        operation.context.now = Now(operation.context);
        switch (rule.kind) {
            case ToxicKind::kLatency:
            case ToxicKind::kJitter: {
                Duration delay = rule.duration;
                if (rule.kind == ToxicKind::kJitter && operation.context.random != nullptr) {
                    delay += rule.duration * 0 +
                             std::chrono::nanoseconds(static_cast<std::int64_t>(
                                 operation.context.random->Uniform(
                                     static_cast<std::size_t>(
                                         std::max<std::int64_t>(0, rule.duration.count())))));
                }
                operation.token.next_stage = index + 1;
                return Schedule(std::move(operation), operation.context.now + delay);
            }
            case ToxicKind::kBandwidth: {
                if (rule.rate_bytes_per_second == 0 || rule.capacity == 0) {
                    return Status(StatusCode::kInvalidArgument,
                                  "bandwidth rate and capacity must be positive");
                }
                auto [bucket_it, inserted] = buckets_.try_emplace(
                    index, rule.rate_bytes_per_second, rule.capacity);
                if (inserted) {
                    bucket_it->second.Configure(rule.rate_bytes_per_second,
                                                rule.capacity);
                }
                TokenBucket& bucket = bucket_it->second;
                bucket.Refill(operation.context.now);
                const std::size_t available = bucket.Available(operation.context.now);
                if (available == 0) {
                    operation.token.next_stage = index;
                    return Schedule(std::move(operation),
                                    bucket.NextEligibleTime(1, operation.context.now));
                }
                const std::size_t amount = std::min(operation.chunk.length, available);
                Chunk piece = operation.chunk;
                piece.length = amount;
                Status status = Emit(operation, piece, limit_rule ? &*limit_rule : nullptr);
                if (!status.ok()) {
                    return status;
                }
                bucket.Consume(amount);
                operation.chunk.offset += amount;
                operation.chunk.length -= amount;
                if (operation.chunk.length != 0) {
                    operation.token.next_stage = index;
                    return Schedule(std::move(operation),
                                    bucket.NextEligibleTime(1, operation.context.now));
                }
                break;
            }
            case ToxicKind::kSlicer: {
                if (rule.slice_bytes == 0) {
                    return Status(StatusCode::kInvalidArgument,
                                  "slicer slice_bytes must be positive");
                }
                while (operation.chunk.length != 0) {
                    const std::size_t amount =
                        std::min(operation.chunk.length, rule.slice_bytes);
                    Chunk piece = operation.chunk;
                    piece.length = amount;
                    Status status = Emit(operation, piece, limit_rule ? &*limit_rule : nullptr);
                    if (!status.ok()) {
                        return status;
                    }
                    operation.chunk.offset += amount;
                    operation.chunk.length -= amount;
                }
                break;
            }
            case ToxicKind::kPause:
                operation.context.paused = true;
                operation.token.next_stage = index + 1;
                pending_.emplace(operation.token.operation_id, std::move(operation));
                return Status::Ok();
            case ToxicKind::kTimeout:
                operation.token.next_stage = index + 1;
                if (rule.duration.count() == 0) {
                    pending_.emplace(operation.token.operation_id, std::move(operation));
                    return Status::Ok();
                }
                if (operation.context.timers == nullptr) {
                    return Status(StatusCode::kUnavailable, "timeout requires a timer queue");
                }
                {
                    const std::uint64_t id = operation.token.operation_id;
                    pending_.emplace(id, std::move(operation));
                    const TimerId timer = operation.context.timers->Schedule(
                        Now(pending_.at(id).context) + rule.duration,
                        [this, id, reset = rule.abort] { OnTimeout(id, reset); });
                    if (timer.value == 0) {
                        pending_.erase(id);
                        return Status(StatusCode::kResourceExhausted,
                                      "unable to schedule timeout");
                    }
                    pending_.at(id).timer = timer;
                    return Status::Ok();
                }
            case ToxicKind::kClose:
                if (operation.context.close) operation.context.close();
                return Status::Ok();
            case ToxicKind::kReset:
                if (operation.context.reset) operation.context.reset();
                return Status::Ok();
            case ToxicKind::kLimitData:
                limit_rule = rule;
                break;
        }
    }
    return Emit(operation, operation.chunk, limit_rule ? &*limit_rule : nullptr);
}

void ToxicPipeline::CancelAll() {
    for (auto& [id, operation] : pending_) {
        static_cast<void>(id);
        if (operation.timer.has_value() && operation.context.timers != nullptr) {
            operation.context.timers->Cancel(*operation.timer);
        }
    }
    pending_.clear();
}

}  // namespace chaosproxy
