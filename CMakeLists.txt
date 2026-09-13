cmake_minimum_required(VERSION 3.25)

project(chaosproxy_phototask VERSION 0.1.0 LANGUAGES C CXX)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "This blueprint targets Linux")
endif()

set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(CP_BUILD_PHOTO "Build PhotoTask programs" OFF)
option(CP_BUILD_KAFKA "Build Kafka transport" OFF)
option(CP_WARNINGS_AS_ERRORS "Treat project warnings as errors" OFF)
set(CP_SANITIZER "none" CACHE STRING "none/address-undefined/thread")

if(CP_BUILD_KAFKA AND NOT CP_BUILD_PHOTO)
  message(FATAL_ERROR "CP_BUILD_KAFKA requires CP_BUILD_PHOTO")
endif()

include(cmake/project_options.cmake)
include(CTest)
find_package(Threads REQUIRED)
find_package(CLI11 CONFIG REQUIRED)
find_package(jsoncpp CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

add_subdirectory(src)
add_subdirectory(apps)
if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
