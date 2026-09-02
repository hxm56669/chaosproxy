cmake_minimum_required(VERSION 3.16)

project(
    ChaosProxy
    VERSION 0.1.0
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "ChaosProxy M2/M3 requires Linux (epoll/accept4)")
endif()

add_library(chaosproxy_core
    src/app.cpp
    src/socket_ops.cpp
    src/event_loop.cpp
    src/listener.cpp
    src/timer_queue.cpp
    src/buffer.cpp
    src/endpoint.cpp
    src/connection.cpp
    src/connection_manager.cpp
)

target_include_directories(chaosproxy_core
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

target_compile_definitions(chaosproxy_core PRIVATE _GNU_SOURCE)
target_compile_options(
    chaosproxy_core
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

add_executable(chaosproxy
    src/main.cpp
)

target_link_libraries(chaosproxy
    PRIVATE
        chaosproxy_core
)

include(CTest)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)

    add_executable(chaosproxy_tests
        tests/unit/app_test.cpp
        tests/unit/unique_fd_test.cpp
        tests/unit/socket_ops_test.cpp
        tests/unit/event_loop_test.cpp
        tests/unit/timer_queue_test.cpp
        tests/unit/event_loop_timer_test.cpp
        tests/unit/buffer_test.cpp
        tests/unit/connection_test.cpp
        tests/integration/listener_event_loop_test.cpp
        tests/integration/multi_connection_proxy_test.cpp
        tests/integration/backpressure_test.cpp
        tests/integration/half_close_test.cpp
        tests/integration/connection_error_test.cpp
        tests/integration/connection_generation_test.cpp
    )

    target_include_directories(chaosproxy_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/tests
    )

    target_compile_definitions(chaosproxy_tests PRIVATE _GNU_SOURCE)
    target_compile_options(
        chaosproxy_tests
        PRIVATE
            -Wall
            -Wextra
            -Wpedantic
    )

    target_link_libraries(chaosproxy_tests
        PRIVATE
            chaosproxy_core
            GTest::gtest_main
    )

    include(GoogleTest)
    gtest_discover_tests(chaosproxy_tests)
endif()
