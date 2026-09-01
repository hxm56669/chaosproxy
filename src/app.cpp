#include "chaosproxy/app.h"

#include <ostream>
#include <string_view>

namespace chaosproxy {
namespace {

void PrintUsage(std::ostream& output) {
    output
        << "ChaosProxy 0.1.0\n"
        << "Usage: chaosproxy [--help]\n"
        << "\n"
        << "M0 engineering scaffold.\n"
        << "TCP proxy networking is not implemented yet.\n";
}

}  // namespace

int Run(
    int argc,
    char* argv[],
    std::ostream& output,
    std::ostream& error
) {
    if (argc == 1) {
        output << "ChaosProxy M0 scaffold\n";
        return 0;
    }

    const std::string_view argument{argv[1]};

    if (argc == 2 && argument == "--help") {
        PrintUsage(output);
        return 0;
    }

    error << "Unknown or invalid arguments\n";
    return 2;
}

}  // namespace chaosproxy

