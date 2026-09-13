#include "chaosproxy/common/completion.h"
#include "chaosproxy/common/status.h"

#include <cerrno>
#include <stdexcept>

#include <gtest/gtest.h>

namespace chaosproxy {
namespace {

TEST(StatusTest, OkAndErrorPreserveCodeAndMessage) {
    const Status ok = Status::Ok();
    EXPECT_TRUE(ok.ok());
    EXPECT_EQ(ok.code(), StatusCode::kOk);
    EXPECT_TRUE(ok.message().empty());

    const Status error(StatusCode::kConflict, "duplicate request");
    EXPECT_FALSE(error.ok());
    EXPECT_EQ(error.code(), StatusCode::kConflict);
    EXPECT_EQ(error.message(), "duplicate request");
}

TEST(StatusTest, ErrnoAndExitCodeMappingsAreStable) {
    const Status invalid = StatusFromErrno(EINVAL, "bind");
    EXPECT_EQ(invalid.code(), StatusCode::kInvalidArgument);
    EXPECT_NE(invalid.message().find("bind:"), std::string::npos);
    EXPECT_EQ(ExitCodeForStatus(invalid), 2);

    EXPECT_EQ(StatusFromErrno(ETIMEDOUT, "connect").code(),
              StatusCode::kDeadlineExceeded);
    EXPECT_EQ(ExitCodeForStatus(Status::Ok()), 0);
    EXPECT_EQ(ExitCodeForStatus(
                  Status(StatusCode::kCommitUnknown, "commit result unknown")),
              9);
}

struct MoveOnly {
    explicit MoveOnly(int initial_value) : value(initial_value) {}
    MoveOnly(const MoveOnly&) = delete;
    MoveOnly& operator=(const MoveOnly&) = delete;
    MoveOnly(MoveOnly&&) = default;
    int value;
};

TEST(StatusOrTest, SupportsMoveOnlyValue) {
    StatusOr<MoveOnly> result(MoveOnly(42));
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.value().value, 42);

    StatusOr<int> error(Status(StatusCode::kUnavailable, "redis offline"));
    EXPECT_FALSE(error.ok());
    EXPECT_EQ(error.status().code(), StatusCode::kUnavailable);
    EXPECT_THROW((void)error.value(), std::logic_error);
}

TEST(StatusOrTest, NormalizesAnOkError) {
    StatusOr<int> result(Status::Ok());
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInternal);
}

TEST(CompletionTest, CarriesStatusOrExactlyThroughCallback) {
    int callback_count = 0;
    Completion<int> completion = [&callback_count](StatusOr<int> result) {
        ++callback_count;
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), 7);
    };
    completion(StatusOr<int>(7));
    EXPECT_EQ(callback_count, 1);
}

}  // namespace
}  // namespace chaosproxy
