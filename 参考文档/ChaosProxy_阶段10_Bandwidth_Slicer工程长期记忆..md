# ChaosProxy 阶段10：Bandwidth / Slicer 工程长期记忆

> 文档定位：本文建立在阶段9 `Latency/Jitter + Scheduler + Sequence Gate + queued(pending+delayed+reorder)` 基线上，只记录阶段10新增/修改内容。继续使用阶段8后 `net / reactor / proxy / toxic` 四层目录，不回退旧扁平结构。

## 1. 当前项目位置

```text
项目：ChaosProxy
完成层级：L1 故障注入闭环（进行中）
前置工程：M3 + Stage7 TimerQueue + Stage8 ToxicPipeline + Stage9 Latency/Jitter
当前阶段：阶段10——Bandwidth 与 Slicer
本轮核心产出：Token Bucket + BandwidthToxic + SlicerToxic + Chunk零拷贝切片 + Sequence Gate流式提交
下一阶段：阶段11 Timeout / LimitData / Close / Reset
```

阶段10必须继续满足阶段9已经建立的约束：

```text
ConnectionToken{id,generation} 防 stale continuation
PipelineSnapshot 保证旧 Chunk 继续旧配置
queued = pending + delayed + reorder
Timer 必须注册到 Direction，Close 时主动 Cancel
异步等待不 sleep、不忙等
```

## 2. 本轮最关键的设计修订：Sequence Gate 从“批量提交”升级为“当前序列流式提交”

阶段9为了防止动态配置和异步 Latency 乱序，采用：

```text
sequence N 的全部 outputs
→ ExecutionState complete
→ 才允许进入 pending
```

这个语义对 Latency 1→1 没问题，但直接用于 Bandwidth 会产生严重错误：

```text
64 KiB 输入
→ Bandwidth 每秒 Emit 4 KiB
→ Sequence Gate 一直等待整个 sequence complete
→ 16 秒后一次性把 64 KiB 放进 pending
→ Socket 又瞬间发送
```

这样“Pipeline 内部看起来限速”，但真正 TCP 发送仍然形成最后一次突发，阶段10不能接受。

因此修改为：

```text
next_commit_sequence 指向的当前最老 sequence：
    已经产生的 outputs 可以立即按产生顺序流式提交到 pending
    sequence 尚未 complete 时，不推进 next_commit_sequence

更晚 sequence：
    即使已经产生 output，仍保存在 reorder
    不能越过前一个未完成 sequence
```

新不变量：

```text
同一 sequence 内：允许流式提交 output
跨 sequence：仍严格按 PipelineSequence 顺序
```

这同时兼容：

```text
1→0 Drop
1→1 Latency
1→N Slicer
1→N async Bandwidth
```

## 3. Bandwidth 工程设计

### 3.1 Token Bucket 参数

`BandwidthToxic` 第一版接口：

```cpp
BandwidthToxic(
    std::uint64_t rate_bytes_per_second,
    std::size_t burst_bytes,
    std::size_t quantum_bytes = 4 * 1024);
```

语义：

```text
rate_bytes_per_second：长期 token 产生速率
burst_bytes：桶容量，决定允许的瞬时 burst
quantum_bytes：未来 Timer 每次最多释放的粒度，用于限制 Timer 数量与发送颗粒度
```

`rate / burst / quantum` 必须全部大于 0。

### 3.2 为什么 Bucket 状态不放在 BandwidthToxic 对象本身

`Toxic` 对象属于 PipelineSnapshot，可能被多个连接/Direction 共享。若 `BandwidthToxic` 自己保存 `tokens`：

```text
连接A消耗token
→ 连接B也被影响
```

这是错误的。

因此每个 Direction 的 `DirectionRuntimeState` 新增：

```cpp
std::unordered_map<std::uint64_t, BandwidthBucketRuntimeState>
    bandwidth_buckets;
```

每个 `BandwidthToxic` 实例获得唯一 `runtime_key`：

```text
Direction + BandwidthToxic实例
→ 唯一Token Bucket状态
```

旧 PipelineSnapshot 与新 PipelineSnapshot 的 Bandwidth 实例不会互相破坏 token 状态。

### 3.3 为什么使用 virtual_time 预留未来 token

不能让每个输入 Chunk 独立到 Timer 到期时再竞争 token，否则：

```text
旧 Chunk 已经在等 token
新 Chunk 恰好在 Timer callback 前进入
→ 新 Chunk 可能先消费刚产生的 token
```

虽然 Sequence Gate 最终还能挡住字节顺序，但会造成旧 Chunk 被无意义拖后、reorder 增大。

当前实现采用同步“未来 token 预留”：

```text
Process(chunk A)
→ 按Token Bucket计算 A 每个 grant 的deadline
→ 立即把这些未来 token 时间段预留到 virtual_time

Process(chunk B)
→ 从 A 已预留完成后的 virtual_time 继续计算
```

因此同一 BandwidthToxic 内天然保持 FIFO token 分配。

### 3.4 Bandwidth 与阶段9内存预算

阶段10不新增第二套“bandwidth bytes”计数，而是复用阶段9已有异步预算 API：

```text
Bandwidth 等 token 的字节
→ TryReserveDelayedBytes

Timer 到期准备 Emit
→ ReleaseDelayedBytes
→ OnPipelineOutput
→ reorder / pending
```

因此所有权迁移仍然是：

```text
delayed(time-gated)
→ reorder
→ pending
→ send真正成功后释放queued
```

注意：字段名 `delayed_bytes` 最初来自 Latency；阶段10开始它实际上承担“所有由时间门控 Toxic 暂存的字节”这一更广义职责。为了减小 Stage10 diff，本阶段不做全局 rename。

### 3.5 Bandwidth 等待与 EPOLLOUT 完全分离

```text
token不足
→ Chunk仍在Bandwidth Toxic
→ TimerQueue
→ pending里没有这部分数据
→ 不会因为它订阅EPOLLOUT
```

只有已经经过 Bandwidth、进入 `pending` 后，如果：

```text
TryWrite → EAGAIN
```

才沿用阶段5：

```text
等待EPOLLOUT
```

因此不会出现“Socket一直可写，但tokens==0导致EPOLLOUT忙等”。

## 4. Slicer 工程设计

`SlicerToxic(slice_size)`：

```text
1 input Chunk
→ N output Chunks
```

必须满足：

```text
concat(outputs) == input
字节内容不变
字节顺序不变
总字节数不变
```

`slice_size == 0` 启动/构造失败。

Slicer 只改变 ChaosProxy Pipeline 的内部处理粒度，不代表真实 TCP segment 大小。

## 5. Chunk 为什么在阶段10升级为共享 backing + view

阶段8/9 `Chunk` 内部是单独 `std::vector<char>`。如果 Slicer 每切 4 KiB 都复制一份，或者 Bandwidth 每个 grant 都复制，会增加大量分配和内存预算偏差。

阶段10保留 `Chunk` 的移动接口，但内部改为：

```text
shared_ptr<vector<char>> backing
+ offset
+ size
```

新增：

```cpp
Chunk TakePrefix(std::size_t size);
```

切片只建立新的 view：

```text
原buffer: [ABCDEFGHIJ]
first:     [ABCD]
remain:        [EFGHIJ]
```

两个 Chunk 分别持有 backing，原对象销毁也不会悬空。

`Append()` 在当前 Chunk 已经是切片 view / backing 被共享时会 materialize 自己的可写副本，因此阶段8已有 `AppendMarkerToxic` 等测试语义仍保持正确。

## 6. 新增/修改文件

```text
修改：
include/chaosproxy/toxic/chunk.h
src/toxic/chunk.cpp
include/chaosproxy/toxic/direction_runtime.h
include/chaosproxy/toxic/toxic.h
src/toxic/toxic.cpp
src/proxy/connection.cpp
CMakeLists.txt

新增：
include/chaosproxy/toxic/bandwidth_toxic.h
include/chaosproxy/toxic/slicer_toxic.h
src/toxic/bandwidth_toxic.cpp
src/toxic/slicer_toxic.cpp

tests/unit/toxic/chunk_stage10_test.cpp
tests/unit/toxic/bandwidth_toxic_test.cpp
tests/unit/toxic/slicer_toxic_test.cpp
tests/integration/bandwidth_integration_test.cpp
```

## 7. 核心生产代码

### `include/chaosproxy/toxic/chunk.h`

```cpp
#pragma once

#include <cstddef>
#include <memory>
#include <vector>

namespace chaosproxy {

class Chunk final {
public:
    Chunk() = default;

    Chunk(const void* data, std::size_t size);
    explicit Chunk(std::vector<char> data) noexcept;

    Chunk(const Chunk&) = delete;
    Chunk& operator=(const Chunk&) = delete;
    Chunk(Chunk&&) noexcept = default;
    Chunk& operator=(Chunk&&) noexcept = default;

    [[nodiscard]] const char* Data() const noexcept;
    [[nodiscard]] char* Data() noexcept;
    [[nodiscard]] std::size_t Size() const noexcept;
    [[nodiscard]] bool Empty() const noexcept;

    void Append(const void* data, std::size_t size);

    // Returns a zero-copy prefix view and consumes the same prefix from *this.
    // Both chunks keep the shared backing storage alive independently.
    [[nodiscard]] Chunk TakePrefix(std::size_t size);

private:
    Chunk(
        std::shared_ptr<std::vector<char>> storage,
        std::size_t offset,
        std::size_t size) noexcept;

    void MaterializeForAppend(std::size_t appended_size);

    std::shared_ptr<std::vector<char>> storage_;
    std::size_t offset_{0};
    std::size_t size_{0};
};

}  // namespace chaosproxy
```

### `src/toxic/chunk.cpp`

```cpp
#include "chaosproxy/toxic/chunk.h"

#include <cstring>
#include <stdexcept>
#include <utility>

namespace chaosproxy {

Chunk::Chunk(const void* data, std::size_t size)
    : storage_(std::make_shared<std::vector<char>>(size)),
      size_(size) {
    if (size == 0) {
        storage_.reset();
        return;
    }

    if (data == nullptr) {
        throw std::invalid_argument("chunk data must not be null");
    }

    std::memcpy(storage_->data(), data, size);
}

Chunk::Chunk(std::vector<char> data) noexcept
    : storage_(data.empty()
          ? nullptr
          : std::make_shared<std::vector<char>>(std::move(data))),
      size_(storage_ ? storage_->size() : 0) {}

Chunk::Chunk(
    std::shared_ptr<std::vector<char>> storage,
    std::size_t offset,
    std::size_t size) noexcept
    : storage_(std::move(storage)),
      offset_(offset),
      size_(size) {}

const char* Chunk::Data() const noexcept {
    return Empty() ? nullptr : storage_->data() + offset_;
}

char* Chunk::Data() noexcept {
    return Empty() ? nullptr : storage_->data() + offset_;
}

std::size_t Chunk::Size() const noexcept {
    return size_;
}

bool Chunk::Empty() const noexcept {
    return size_ == 0;
}

void Chunk::Append(const void* data, std::size_t size) {
    if (size == 0) {
        return;
    }

    if (data == nullptr) {
        throw std::invalid_argument("chunk append data must not be null");
    }

    if (Empty()) {
        storage_ = std::make_shared<std::vector<char>>(size);
        std::memcpy(storage_->data(), data, size);
        offset_ = 0;
        size_ = size;
        return;
    }

    const bool owns_full_storage =
        storage_.use_count() == 1 &&
        offset_ == 0 &&
        size_ == storage_->size();

    if (owns_full_storage) {
        const auto* bytes = static_cast<const char*>(data);
        storage_->insert(storage_->end(), bytes, bytes + size);
        size_ += size;
        return;
    }

    MaterializeForAppend(size);
    std::memcpy(storage_->data() + size_, data, size);
    size_ += size;
}

Chunk Chunk::TakePrefix(std::size_t size) {
    if (size == 0 || size > size_) {
        throw std::invalid_argument("invalid chunk prefix size");
    }

    Chunk prefix(storage_, offset_, size);
    offset_ += size;
    size_ -= size;

    if (size_ == 0) {
        storage_.reset();
        offset_ = 0;
    }

    return prefix;
}

void Chunk::MaterializeForAppend(std::size_t appended_size) {
    auto materialized =
        std::make_shared<std::vector<char>>(size_ + appended_size);

    if (size_ > 0) {
        std::memcpy(materialized->data(), Data(), size_);
    }

    storage_ = std::move(materialized);
    offset_ = 0;
}

}  // namespace chaosproxy
```

### `include/chaosproxy/toxic/direction_runtime.h`

```cpp
#pragma once

#include "chaosproxy/reactor/clock.h"
#include "chaosproxy/toxic/random_source.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>

namespace chaosproxy {

struct BandwidthBucketRuntimeState final {
    bool initialized{false};
    long double tokens{0.0L};

    // This is a virtual reservation cursor. It may be in the future when
    // earlier chunks have already reserved future token production.
    TimePoint virtual_time{};
};

struct DirectionRuntimeState final {
    explicit DirectionRuntimeState(
        std::shared_ptr<RandomSource> random_source)
        : random(std::move(random_source)) {}

    std::shared_ptr<RandomSource> random;
    std::optional<TimePoint> latency_release_barrier;

    // One runtime bucket per BandwidthToxic instance. Different Pipeline
    // snapshots therefore cannot corrupt each other's token accounting.
    std::unordered_map<std::uint64_t, BandwidthBucketRuntimeState>
        bandwidth_buckets;
};

}  // namespace chaosproxy
```

### `DirectionContext` 增量

`include/chaosproxy/toxic/toxic.h` 在 `ConstrainLatencyDeadline()` 后增加：

```cpp
[[nodiscard]] BandwidthBucketRuntimeState& BandwidthBucket(
    std::uint64_t runtime_key) const;
```

`src/toxic/toxic.cpp` 增加：

```cpp
BandwidthBucketRuntimeState& DirectionContext::BandwidthBucket(
    std::uint64_t runtime_key) const {
    if (runtime_key == 0) {
        throw std::invalid_argument("bandwidth runtime key must be non-zero");
    }

    return runtime_->bandwidth_buckets[runtime_key];
}
```

### `include/chaosproxy/toxic/bandwidth_toxic.h`

```cpp
#pragma once

#include "chaosproxy/toxic/toxic.h"

#include <cstddef>
#include <cstdint>

namespace chaosproxy {

class BandwidthToxic final : public Toxic {
public:
    static constexpr std::size_t kDefaultQuantumBytes = 4 * 1024;

    BandwidthToxic(
        std::uint64_t rate_bytes_per_second,
        std::size_t burst_bytes,
        std::size_t quantum_bytes = kDefaultQuantumBytes);

    BandwidthToxic(const BandwidthToxic&) = delete;
    BandwidthToxic& operator=(const BandwidthToxic&) = delete;
    BandwidthToxic(BandwidthToxic&&) = delete;
    BandwidthToxic& operator=(BandwidthToxic&&) = delete;

    [[nodiscard]] std::uint64_t RateBytesPerSecond() const noexcept;
    [[nodiscard]] std::size_t BurstBytes() const noexcept;
    [[nodiscard]] std::size_t QuantumBytes() const noexcept;

    void Process(
        Chunk chunk,
        DirectionContext context,
        ToxicOutput output) override;

private:
    std::uint64_t rate_bytes_per_second_;
    std::size_t burst_bytes_;
    std::size_t quantum_bytes_;
    std::uint64_t runtime_key_;
};

}  // namespace chaosproxy
```

### `src/toxic/bandwidth_toxic.cpp`

```cpp
#include "chaosproxy/toxic/bandwidth_toxic.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <limits>
#include <memory>
#include <stdexcept>
#include <utility>

namespace chaosproxy {
namespace {

struct Grant final {
    std::size_t bytes{0};
    TimePoint deadline{};
};

struct PendingBandwidthRelease final {
    PendingBandwidthRelease(
        Chunk owned_chunk,
        ToxicOutput continuation,
        DirectionContext direction_context,
        std::size_t owned_bytes)
        : chunk(std::move(owned_chunk)),
          output(std::move(continuation)),
          context(std::move(direction_context)),
          bytes(owned_bytes) {}

    Chunk chunk;
    ToxicOutput output;
    DirectionContext context;
    std::size_t bytes{0};
    TimerId timer_id{0};
};

std::uint64_t NextRuntimeKey() noexcept {
    static std::atomic<std::uint64_t> next{1};

    for (;;) {
        const std::uint64_t candidate =
            next.fetch_add(1, std::memory_order_relaxed);
        if (candidate != 0) {
            return candidate;
        }
    }
}

long double Seconds(Duration duration) noexcept {
    return std::chrono::duration<long double>(duration).count();
}

void RefillTo(
    BandwidthBucketRuntimeState& state,
    TimePoint now,
    std::uint64_t rate,
    std::size_t burst) noexcept {
    if (!state.initialized) {
        state.initialized = true;
        state.tokens = static_cast<long double>(burst);
        state.virtual_time = now;
        return;
    }

    if (now <= state.virtual_time) {
        return;
    }

    const long double produced =
        Seconds(now - state.virtual_time) *
        static_cast<long double>(rate);

    state.tokens = std::min(
        static_cast<long double>(burst),
        state.tokens + produced);
    state.virtual_time = now;
}

Duration CeilDelay(long double seconds) {
    if (!(seconds >= 0.0L) || !std::isfinite(seconds)) {
        throw std::overflow_error("bandwidth wait duration overflow");
    }

    const auto exact = std::chrono::duration<long double>(seconds);
    Duration delay = std::chrono::duration_cast<Duration>(exact);

    if (std::chrono::duration<long double>(delay) < exact) {
        if (delay == Duration::max()) {
            throw std::overflow_error("bandwidth wait duration overflow");
        }
        delay += Duration{1};
    }

    if (delay <= Duration::zero()) {
        delay = Duration{1};
    }

    return delay;
}

Grant ReserveNextGrant(
    BandwidthBucketRuntimeState& state,
    TimePoint now,
    std::size_t remaining,
    std::uint64_t rate,
    std::size_t burst,
    std::size_t quantum) {
    RefillTo(state, now, rate, burst);

    const std::size_t max_grant =
        std::min({remaining, burst, quantum});

    const long double whole_tokens = std::floor(state.tokens);
    if (state.virtual_time <= now && whole_tokens >= 1.0L) {
        const auto available = static_cast<std::size_t>(std::min(
            whole_tokens,
            static_cast<long double>(max_grant)));

        state.tokens -= static_cast<long double>(available);
        if (state.tokens < 0.0L) {
            state.tokens = 0.0L;
        }

        return Grant{available, now};
    }

    const long double target = static_cast<long double>(max_grant);
    const long double missing = std::max(0.0L, target - state.tokens);
    const long double wait_seconds =
        missing / static_cast<long double>(rate);
    const Duration delay = CeilDelay(wait_seconds);

    if (state.virtual_time > TimePoint::max() - delay) {
        throw std::overflow_error("bandwidth deadline overflow");
    }

    state.virtual_time += delay;
    state.tokens = std::min(
        static_cast<long double>(burst),
        state.tokens +
            Seconds(delay) * static_cast<long double>(rate));

    // CeilDelay should make enough tokens available. One additional clock tick
    // handles a possible long-double rounding edge without creating a spin.
    if (state.tokens + 1e-12L < target) {
        if (state.virtual_time == TimePoint::max()) {
            throw std::overflow_error("bandwidth deadline overflow");
        }
        state.virtual_time += Duration{1};
        state.tokens = std::min(
            static_cast<long double>(burst),
            state.tokens +
                Seconds(Duration{1}) * static_cast<long double>(rate));
    }

    if (state.tokens + 1e-9L < target) {
        throw std::runtime_error("bandwidth token calculation failed");
    }

    state.tokens -= target;
    if (state.tokens < 0.0L) {
        state.tokens = 0.0L;
    }

    return Grant{max_grant, state.virtual_time};
}

bool ScheduleRelease(
    TimePoint deadline,
    Chunk chunk,
    DirectionContext context,
    ToxicOutput output) {
    const std::size_t bytes = chunk.Size();
    auto pending = std::make_shared<PendingBandwidthRelease>(
        std::move(chunk),
        std::move(output),
        context,
        bytes);

    const TimerId id = context.ScheduleAt(
        deadline,
        [pending]() mutable {
            const TimerId timer_id = pending->timer_id;
            pending->context.UnregisterTimer(timer_id);

            // Ownership moves from bandwidth-deferred storage into the next
            // pipeline stage before Sequence Gate/pending accounting takes it.
            pending->context.ReleaseDelayedBytes(pending->bytes);

            Chunk chunk = std::move(pending->chunk);
            ToxicOutput output = std::move(pending->output);
            output(std::move(chunk));
        });

    pending->timer_id = id;

    if (!context.RegisterTimer(id)) {
        (void)context.CancelTimer(id);
        context.ReleaseDelayedBytes(bytes);
        return false;
    }

    return true;
}

}  // namespace

BandwidthToxic::BandwidthToxic(
    std::uint64_t rate_bytes_per_second,
    std::size_t burst_bytes,
    std::size_t quantum_bytes)
    : rate_bytes_per_second_(rate_bytes_per_second),
      burst_bytes_(burst_bytes),
      quantum_bytes_(quantum_bytes),
      runtime_key_(NextRuntimeKey()) {
    if (rate_bytes_per_second_ == 0) {
        throw std::invalid_argument("bandwidth rate must be positive");
    }
    if (burst_bytes_ == 0) {
        throw std::invalid_argument("bandwidth burst must be positive");
    }
    if (quantum_bytes_ == 0) {
        throw std::invalid_argument("bandwidth quantum must be positive");
    }
}

std::uint64_t BandwidthToxic::RateBytesPerSecond() const noexcept {
    return rate_bytes_per_second_;
}

std::size_t BandwidthToxic::BurstBytes() const noexcept {
    return burst_bytes_;
}

std::size_t BandwidthToxic::QuantumBytes() const noexcept {
    return quantum_bytes_;
}

void BandwidthToxic::Process(
    Chunk chunk,
    DirectionContext context,
    ToxicOutput output) {
    if (!output) {
        throw std::invalid_argument("bandwidth output must not be empty");
    }

    if (chunk.Empty()) {
        output(std::move(chunk));
        return;
    }

    std::size_t unassigned_reserved_bytes = chunk.Size();
    if (!context.TryReserveDelayedBytes(unassigned_reserved_bytes)) {
        return;
    }

    BandwidthBucketRuntimeState& bucket =
        context.BandwidthBucket(runtime_key_);
    const TimePoint now = context.Now();

    try {
        while (!chunk.Empty()) {
            const Grant grant = ReserveNextGrant(
                bucket,
                now,
                chunk.Size(),
                rate_bytes_per_second_,
                burst_bytes_,
                quantum_bytes_);

            Chunk piece = chunk.TakePrefix(grant.bytes);

            if (grant.deadline <= now) {
                context.ReleaseDelayedBytes(grant.bytes);
                unassigned_reserved_bytes -= grant.bytes;
                output(std::move(piece));
                continue;
            }

            const bool registered = ScheduleRelease(
                grant.deadline,
                std::move(piece),
                context,
                output);

            unassigned_reserved_bytes -= grant.bytes;

            if (!registered) {
                context.ReleaseDelayedBytes(unassigned_reserved_bytes);
                return;
            }
        }
    } catch (...) {
        context.ReleaseDelayedBytes(unassigned_reserved_bytes);
        throw;
    }
}

}  // namespace chaosproxy
```

### `include/chaosproxy/toxic/slicer_toxic.h`

```cpp
#pragma once

#include "chaosproxy/toxic/toxic.h"

#include <cstddef>

namespace chaosproxy {

class SlicerToxic final : public Toxic {
public:
    explicit SlicerToxic(std::size_t slice_size);

    [[nodiscard]] std::size_t SliceSize() const noexcept;

    void Process(
        Chunk chunk,
        DirectionContext context,
        ToxicOutput output) override;

private:
    std::size_t slice_size_;
};

}  // namespace chaosproxy
```

### `src/toxic/slicer_toxic.cpp`

```cpp
#include "chaosproxy/toxic/slicer_toxic.h"

#include <stdexcept>
#include <utility>

namespace chaosproxy {

SlicerToxic::SlicerToxic(std::size_t slice_size)
    : slice_size_(slice_size) {
    if (slice_size_ == 0) {
        throw std::invalid_argument("slice size must be positive");
    }
}

std::size_t SlicerToxic::SliceSize() const noexcept {
    return slice_size_;
}

void SlicerToxic::Process(
    Chunk chunk,
    DirectionContext /*context*/,
    ToxicOutput output) {
    if (!output) {
        throw std::invalid_argument("slicer output must not be empty");
    }

    while (chunk.Size() > slice_size_) {
        output(chunk.TakePrefix(slice_size_));
    }

    if (!chunk.Empty()) {
        output(std::move(chunk));
    }
}

}  // namespace chaosproxy
```

## 8. ConnectionPair 的 Stage10 Sequence Gate 修改

### `OnPipelineOutput()`

原阶段9在：

```cpp
direction.reorder_bytes += chunk.Size();
result_it->second.outputs.push_back(std::move(chunk));
UpdateBackpressure(direction);
```

之间插入：

```cpp
TryDrainSequenceGate(direction);
if (closed_) {
    return;
}
```

即：当前 `next_commit_sequence` 一产生 output 就尝试往 `pending` 流式提交。

### `TryDrainSequenceGate()`

阶段9入口：

```cpp
if (result_it == direction.pipeline_results.end() ||
    !result_it->second.complete) {
    return;
}
```

改为：

```cpp
if (result_it == direction.pipeline_results.end()) {
    return;
}
```

先 drain 当前已经产生的 outputs；drain 完后增加：

```cpp
if (!result.complete) {
    return;
}
```

只有 sequence complete 后才：

```cpp
direction.pipeline_results.erase(result_it);
++direction.next_commit_sequence;
```

最终逻辑：

```text
当前最早sequence：output可流式进pending
当前最早sequence未complete：不推进commit sequence
后续sequence：继续被挡在reorder
```

## 9. CMake 增量

`chaosproxy_core` 增加：

```cmake
src/toxic/bandwidth_toxic.cpp
src/toxic/slicer_toxic.cpp
```

`chaosproxy_tests` 增加：

```cmake
tests/unit/toxic/chunk_stage10_test.cpp
tests/unit/toxic/bandwidth_toxic_test.cpp
tests/unit/toxic/slicer_toxic_test.cpp
tests/integration/bandwidth_integration_test.cpp
```

完整 patch 见同次交付的：

```text
chaosproxy_stage10_changes.patch
```

## 10. 测试目标

### Chunk / Slicer

```text
[x] TakePrefix后内容顺序正确
[x] prefix在原Chunk销毁后仍有效
[x] sliced view执行Append不会修改剩余view
[x] Slicer 10 bytes / 4 bytes → 4 + 4 + 2
[x] slice_size=0拒绝
```

### Bandwidth FakeScheduler

```text
[x] rate=4B/s burst=4B quantum=4B
    12B → t=0 释放4B，t=1s 释放4B，t=2s 释放4B

[x] 连续两个Process共享同一Direction bucket
    后输入不能抢占前输入预留的未来token

[x] token不足后只注册Timer
    不推进时间就不会重复执行/忙等

[x] delayed accounting随grant释放
```

### 集成测试

`BandwidthIntegrationTest.CurrentSequenceStreamsBeforePipelineCompletion` 专门回归 Stage10 Sequence Gate：

```text
输入8KiB
Bandwidth: 64KiB/s, burst=4KiB, quantum=4KiB

第一个4KiB应立即到达upstream
同时：
    DelayedBytes > 0
    PipelineInflight > 0

约后续Timer到期后第二个4KiB再到达
最终数据逐字节一致
```

如果仍使用阶段9“complete后整体提交”，这个测试的第一个 4 KiB 在 Pipeline 未完成时永远观察不到，因此会失败。

## 11. 本次实际验证

本次对 Stage10 新生产模块建立了与阶段9接口兼容的最小验证工程，并实际执行：

```text
[x] g++ -std=c++17 -Wall -Wextra -Wpedantic -Werror
    Chunk + SlicerToxic + BandwidthToxic 编译通过

[x] smoke：Chunk zero-copy prefix
[x] smoke：Slicer 1→N顺序
[x] smoke：Token Bucket t=0/1s/2s释放
[x] smoke：连续输入共享bucket且FIFO预留token

[x] AddressSanitizer + UndefinedBehaviorSanitizer smoke通过
    LeakSanitizer关闭
```

当前对话只提供了阶段0～9工程文档/补丁描述，没有实际完整 ChaosProxy Git 仓库，因此不能诚实写成：

```text
正式仓库CMake/Ninja/GTest/CTest已通过
```

正式仓库必须执行下一节质量门。

## 12. 正式仓库质量门

先应用 Stage10 patch / 按本文修改，然后：

```bash
cmake -S . -B build-debug \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DBUILD_TESTING=ON

cmake --build build-debug -j
ctest --test-dir build-debug --output-on-failure
```

只跑 Stage10：

```bash
./build-debug/chaosproxy_tests \
  --gtest_filter='ChunkStage10Test.*:SlicerToxicTest.*:BandwidthToxicTest.*:BandwidthIntegrationTest.*'
```

Sanitizer：

```bash
cmake -S . -B build-asan \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DBUILD_TESTING=ON \
    -DCMAKE_CXX_FLAGS='-fsanitize=address,undefined -fno-omit-frame-pointer'

cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure
```

## 13. 阶段10新增不变量

1. Bandwidth 限制的是单位时间字节数，不是一次 `send()` 大小。
2. `rate`、`burst`、`quantum` 全部必须大于 0。
3. Token Bucket runtime 属于 Direction，不属于共享 Toxic 对象。
4. 不同 BandwidthToxic snapshot 实例通过 `runtime_key` 隔离 bucket。
5. 后输入不得抢占前输入已经预留的未来 token。
6. token 不足只能等待 Timer，不允许 busy loop / sleep。
7. Bandwidth Timer 必须注册到 Direction，Close 时由既有 `CancelDirectionTimers()` 清理。
8. Bandwidth 暂存字节必须进入阶段9 hard-limit / high-low watermark 预算。
9. 当前最老 PipelineSequence 可以流式提交已产生 outputs。
10. 当前 sequence 未 complete 时不得推进 `next_commit_sequence`。
11. 后续 sequence 永远不能越过未完成的前序 sequence。
12. Slicer 必须保持内容、顺序、总字节数不变。
13. Slicer 只切 Pipeline Chunk，不宣称模拟 TCP 分段。
14. Chunk slice 必须拥有安全 backing 生命周期，不能保存悬空 `data()`。
15. Socket 写阻塞仍由 `EPOLLOUT` 恢复；Bandwidth 时间阻塞仍由 Timer 恢复，两者不能混淆。

## 14. 阶段10完成边界

完成：

```text
Token Bucket算法与burst
未来token deadline计算
无token Timer等待
BandwidthToxic异步1→N
SlicerToxic同步1→N
Chunk零拷贝切片
Stage9 Sequence Gate升级为当前序列流式提交
Stage9 queued/hard-limit/Timer cancel复用
Stage10单元/集成测试源码
新模块严格编译 + smoke + ASan/UBSan
```

未提前实现：

```text
Timeout
LimitData
Close Toxic
Reset Toxic
配置文件字段/动态控制面
指标与Benchmark最终报告
```

下一阶段：

```text
阶段11：Timeout、LimitData、Close 与 Reset
```
