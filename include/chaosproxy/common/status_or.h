#pragma once

#include "chaosproxy/common/status.h"

#include <optional>
#include <stdexcept>
#include <utility>

namespace chaosproxy {

template <class T>
class StatusOr {
public:
    StatusOr(T value)
        : status_(Status::Ok()), value_(std::move(value)) {}

    StatusOr(Status error)
        : status_(error.ok()
                      ? Status(StatusCode::kInternal,
                               "StatusOr cannot contain an OK error")
                      : std::move(error)) {}

    bool ok() const noexcept {
        return status_.ok() && value_.has_value();
    }

    const Status& status() const noexcept {
        return status_;
    }

    T& value() & {
        EnsureValue();
        return *value_;
    }

    const T& value() const & {
        EnsureValue();
        return *value_;
    }

    T&& value() && {
        EnsureValue();
        return std::move(*value_);
    }

private:
    void EnsureValue() const {
        if (!ok()) {
            throw std::logic_error(status_.message().empty()
                                       ? "StatusOr has no value"
                                       : status_.message());
        }
    }

    Status status_;
    std::optional<T> value_;
};

}  // namespace chaosproxy
