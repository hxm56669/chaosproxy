#include "chaosproxy/common/status.h"

#include <cerrno>
#include <cstring>
#include <string>
#include <utility>

namespace chaosproxy {
namespace {

StatusCode CodeFromErrno(int error) noexcept {
    switch (error) {
        case EINVAL:
            return StatusCode::kInvalidArgument;
        case ENOENT:
            return StatusCode::kNotFound;
        case EEXIST:
            return StatusCode::kConflict;
        case EAGAIN:
#if EWOULDBLOCK != EAGAIN
        case EWOULDBLOCK:
#endif
        case ENOBUFS:
        case ENOMEM:
            return StatusCode::kResourceExhausted;
        case ETIMEDOUT:
            return StatusCode::kDeadlineExceeded;
        case ECONNREFUSED:
        case ECONNRESET:
        case ECONNABORTED:
        case ENETDOWN:
        case ENETUNREACH:
        case EHOSTUNREACH:
            return StatusCode::kUnavailable;
        default:
            return StatusCode::kIoError;
    }
}

}  // namespace

Status Status::Ok() {
    return Status(StatusCode::kOk, {});
}

Status::Status(StatusCode code, std::string message)
    : code_(code), message_(std::move(message)) {}

bool Status::ok() const noexcept {
    return code_ == StatusCode::kOk;
}

StatusCode Status::code() const noexcept {
    return code_;
}

const std::string& Status::message() const noexcept {
    return message_;
}

Status StatusFromErrno(int error, std::string_view operation) {
    const StatusCode code = CodeFromErrno(error);
    std::string message(operation);
    message.append(": ");
    message.append(std::strerror(error));
    return Status(code, std::move(message));
}

int ExitCodeForStatus(const Status& status) noexcept {
    switch (status.code()) {
        case StatusCode::kOk:
            return 0;
        case StatusCode::kInvalidArgument:
            return 2;
        case StatusCode::kNotFound:
            return 3;
        case StatusCode::kConflict:
            return 4;
        case StatusCode::kResourceExhausted:
            return 5;
        case StatusCode::kUnavailable:
            return 6;
        case StatusCode::kDeadlineExceeded:
            return 7;
        case StatusCode::kIoError:
            return 8;
        case StatusCode::kCommitUnknown:
            return 9;
        case StatusCode::kStaleOwner:
            return 10;
        case StatusCode::kInconsistent:
            return 11;
        case StatusCode::kInternal:
            return 12;
    }
    return 12;
}

}  // namespace chaosproxy
