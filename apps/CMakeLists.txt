add_executable(chaosproxy chaosproxy_main.cpp)
cp_project_target(chaosproxy)
target_link_libraries(chaosproxy PRIVATE chaosproxy_core cp_common CLI11::CLI11)

add_executable(chaosctl chaosctl_main.cpp)
cp_project_target(chaosctl)
target_link_libraries(chaosctl PRIVATE chaosproxy_core cp_common CLI11::CLI11)

if(CP_BUILD_PHOTO)
  add_executable(photo_runtime photo_runtime_main.cpp)
  cp_project_target(photo_runtime)
  target_link_libraries(photo_runtime PRIVATE phototask_runtime Drogon::Drogon CLI11::CLI11)
endif()
