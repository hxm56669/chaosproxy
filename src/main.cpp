#include "chaosproxy/app.h"

#include <iostream>

int main(int argc, char* argv[]) {
    return chaosproxy::Run(
        argc,
        argv,
        std::cout,
        std::cerr
    );
}

