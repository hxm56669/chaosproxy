#include "chaosproxy/buffer.h"

#include <gtest/gtest.h>

#include <string>

namespace {

using chaosproxy::Buffer;

TEST(BufferTest, AppendsConsumesAndWrapsWithoutGrowing) {
    Buffer buffer(8);

    ASSERT_TRUE(buffer.Append("abcdef", 6));
    EXPECT_EQ(buffer.Size(), 6U);
    EXPECT_EQ(buffer.FrontSize(), 6U);

    buffer.Consume(5);
    EXPECT_EQ(buffer.Size(), 1U);

    ASSERT_TRUE(buffer.Append("12345", 5));
    EXPECT_EQ(buffer.Size(), 6U);
    EXPECT_EQ(buffer.Capacity(), 8U);

    std::string observed;
    while (!buffer.Empty()) {
        observed.append(buffer.FrontData(), buffer.FrontSize());
        buffer.Consume(buffer.FrontSize());
    }

    EXPECT_EQ(observed, "f12345");
}

TEST(BufferTest, RejectsAppendBeyondFixedCapacity) {
    Buffer buffer(4);

    EXPECT_TRUE(buffer.Append("abcd", 4));
    EXPECT_FALSE(buffer.Append("e", 1));
    EXPECT_EQ(buffer.Size(), 4U);
    EXPECT_EQ(buffer.Available(), 0U);
}

TEST(BufferTest, RejectsZeroCapacity) {
    EXPECT_THROW(Buffer(0), std::invalid_argument);
}

}  // namespace
