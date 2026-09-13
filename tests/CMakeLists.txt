find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

add_executable(common_tests unit/common/status_test.cpp)
cp_project_target(common_tests)
target_link_libraries(common_tests PRIVATE cp_common GTest::gtest_main)
gtest_discover_tests(common_tests DISCOVERY_MODE PRE_TEST)

add_executable(proxy_tests
  unit/proxy/socket_ops_test.cpp
  unit/proxy/buffer_test.cpp
  unit/proxy/event_loop_test.cpp
  unit/proxy/timer_queue_test.cpp
  unit/proxy/toxic_pipeline_test.cpp
  unit/proxy/config_control_test.cpp
  integration/proxy/trace_writer_test.cpp
  unit/proxy/connection_table_test.cpp
  integration/proxy/connection_pair_test.cpp
  integration/proxy/half_close_test.cpp
)
cp_project_target(proxy_tests)
target_link_libraries(proxy_tests PRIVATE chaosproxy_core GTest::gtest_main)
gtest_discover_tests(proxy_tests DISCOVERY_MODE PRE_TEST)
