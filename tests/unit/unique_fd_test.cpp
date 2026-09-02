#include "chaosproxy/unique_fd.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cerrno>
#include <utility>

namespace {

using chaosproxy::UniqueFd;

TEST(UniqueFdTest, MoveTransfersOwnershipAndClosesExactlyOnce) {
    int raw_fds[2]{};
    ASSERT_EQ(::pipe2(raw_fds, O_CLOEXEC), 0);

    const int owned_fd = raw_fds[0];
    UniqueFd first(owned_fd);
    UniqueFd second(std::move(first));

    EXPECT_FALSE(first.IsValid());
    EXPECT_EQ(second.Get(), owned_fd);

    second.Reset();
    errno = 0;
    EXPECT_EQ(::fcntl(owned_fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);

    (void)::close(raw_fds[1]);
}

TEST(UniqueFdTest, ReleaseReturnsFdWithoutClosingIt) {
    int raw_fds[2]{};
    ASSERT_EQ(::pipe2(raw_fds, O_CLOEXEC), 0);

    UniqueFd fd(raw_fds[0]);
    const int released = fd.Release();

    EXPECT_FALSE(fd.IsValid());
    EXPECT_NE(::fcntl(released, F_GETFD), -1);

    (void)::close(released);
    (void)::close(raw_fds[1]);
}

}  // namespace
