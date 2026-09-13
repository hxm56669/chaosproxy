#pragma once

#include <string>
#include <string_view>

namespace chaosproxy {

enum class StatusCode {
    kOk,
    kInvalidArgument,
    kNotFound,
    kConflict,
    kResourceExhausted,
    kUnavailable,
    kDeadlineExceeded,
    kIoError,
    kCommitUnknown,
    kStaleOwner,
    kInconsistent,
    kInternal,
};

class Status {
public:
    static Status Ok();

    Status(StatusCode code, std::string message);

    bool ok() const noexcept;
    StatusCode code() const noexcept;
    const std::string& message() const noexcept;

private:
    StatusCode code_;
    std::string message_;
};

Status StatusFromErrno(int error, std::string_view operation);
int ExitCodeForStatus(const Status& status) noexcept;

}  // namespace chaosproxy
