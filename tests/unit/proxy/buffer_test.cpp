#include "chaosproxy/proxy/buffer.h"

#include <memory>

#include <gtest/gtest.h>

namespace chaosproxy {
namespace {

TEST(BufferTest, AllocationBudgetAndLogicalQueueBytesAreIndependent) {
    auto budget = std::make_shared<BufferBudget>(8);
    auto block_result = BufferBlock::Allocate(budget, 4);
    ASSERT_TRUE(block_result.ok());
    EXPECT_EQ(budget->Used(), 4U);

    EXPECT_FALSE(BufferBlock::Allocate(budget, 5).ok());
    std::shared_ptr<const BufferBlock> storage = std::move(block_result).value();
    Chunk chunk{storage, 0, 4, 10, 1, {}};
    ByteQueue queue;
    ASSERT_TRUE(queue.Push(std::move(chunk)).ok());
    EXPECT_EQ(queue.QueuedBytes(), 4U);
    EXPECT_EQ(queue.FrontBytes().size(), 4U);

    queue.Consume(2);
    EXPECT_EQ(queue.QueuedBytes(), 2U);
    EXPECT_EQ(queue.FrontBytes().size(), 2U);
    queue.Clear();
    EXPECT_TRUE(queue.Empty());
    EXPECT_EQ(budget->Used(), 4U);
    storage.reset();
    EXPECT_EQ(budget->Used(), 0U);
}

TEST(BufferTest, RejectsInvalidChunkRange) {
    auto budget = std::make_shared<BufferBudget>(16);
    auto block = BufferBlock::Allocate(budget, 8);
    ASSERT_TRUE(block.ok());
    ByteQueue queue;
    EXPECT_EQ(queue.Push(Chunk{std::move(block).value(), 7, 2, 0, 0, {}}).code(),
              StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace chaosproxy
