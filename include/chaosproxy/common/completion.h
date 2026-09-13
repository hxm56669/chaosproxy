#pragma once

#include "chaosproxy/common/status_or.h"

#include <functional>

namespace chaosproxy {

struct Unit {};

template <class T>
using Completion = std::function<void(StatusOr<T>)>;

}  // namespace chaosproxy
