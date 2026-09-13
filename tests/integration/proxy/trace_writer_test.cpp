#include "chaosproxy/proxy/trace_writer.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace chaosproxy {
namespace {

TEST(TraceWriterTest, BoundedQueueReportsDroppedAndDrainsToFile) {
    const auto path = std::filesystem::temp_directory_path() / "chaosproxy-a8.trace";
    {
        TraceWriter exhausted(path, 0);
        EXPECT_FALSE(exhausted.TryAppend({"run", "drop", 1, 2, 3, "bounded"}));
        const TraceStats stats = exhausted.Snapshot();
        EXPECT_EQ(stats.dropped, 1U);
        EXPECT_TRUE(stats.incomplete);
    }
    {
        TraceWriter writer(path, 4);
        ASSERT_TRUE(writer.TryAppend({"run", "send", 2, 9, 12, "ok"}));
        writer.FlushUntil(Clock::now() + std::chrono::seconds(1));
        const TraceStats stats = writer.Snapshot();
        EXPECT_EQ(stats.written, 1U);
        EXPECT_EQ(stats.queued, 0U);
    }
    std::ifstream input(path);
    const std::string contents((std::istreambuf_iterator<char>(input)), {});
    std::filesystem::remove(path);
    EXPECT_NE(contents.find("run\tsend\t2\t9\t12\tok"), std::string::npos);
}

}  // namespace
}  // namespace chaosproxy
