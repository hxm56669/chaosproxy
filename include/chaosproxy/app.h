#pragma once

#include <iosfwd>

namespace chaosproxy {

int Run(
    int argc,
    char* argv[],
    std::ostream& output,
    std::ostream& error
);

}  // namespace chaosproxy

