#include "chaosproxy/app.h"

#include <gtest/gtest.h>

#include <sstream>
#include <string>

namespace {

TEST(AppTest, RunsWithoutArguments) {
    char program_name[] = "chaosproxy";
    char* argv[] = {program_name};

    std::ostringstream output;
    std::ostringstream error;

    const int result = chaosproxy::Run(1, argv, output, error);

    EXPECT_EQ(result, 0);
    EXPECT_NE(output.str().find("ChaosProxy"), std::string::npos);
    EXPECT_TRUE(error.str().empty());
}

TEST(AppTest, PrintsHelp) {
    char program_name[] = "chaosproxy";
    char help_argument[] = "--help";
    char* argv[] = {program_name, help_argument};

    std::ostringstream output;
    std::ostringstream error;

    const int result = chaosproxy::Run(2, argv, output, error);

    EXPECT_EQ(result, 0);
    EXPECT_NE(output.str().find("Usage:"), std::string::npos);
    EXPECT_TRUE(error.str().empty());
}

TEST(AppTest, RejectsUnknownArgument) {
    char program_name[] = "chaosproxy";
    char unknown_argument[] = "--unknown";
    char* argv[] = {program_name, unknown_argument};

    std::ostringstream output;
    std::ostringstream error;

    const int result = chaosproxy::Run(2, argv, output, error);

    EXPECT_NE(result, 0);
    EXPECT_TRUE(output.str().empty());
    EXPECT_NE(error.str().find("Unknown"), std::string::npos);
}

}  // namespace

