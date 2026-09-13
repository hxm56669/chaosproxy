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

if(CP_BUILD_PHOTO)
  add_executable(phototask_tests
    integration/phototask/b0_runtime_test.cpp
    integration/phototask/b1_model_filesystem_test.cpp
    integration/phototask/b2_b6_pipeline_test.cpp)
  target_sources(phototask_tests PRIVATE integration/phototask/c1_c4_cache_test.cpp)
  target_sources(phototask_tests PRIVATE integration/phototask/d4_status_test.cpp)
  cp_project_target(phototask_tests)
  target_compile_definitions(phototask_tests PRIVATE
    PHOTOTASK_SOURCE_DIR="${PROJECT_SOURCE_DIR}")
  target_link_libraries(phototask_tests PRIVATE phototask_runtime GTest::gtest_main)
  gtest_discover_tests(phototask_tests DISCOVERY_MODE PRE_TEST)
endif()

if(CP_BUILD_KAFKA)
  add_executable(kafka_tests integration/phototask/f1_f6_kafka_test.cpp)
  cp_project_target(kafka_tests)
  target_link_libraries(kafka_tests PRIVATE phototask_kafka GTest::gtest_main)
  gtest_discover_tests(kafka_tests DISCOVERY_MODE PRE_TEST)
endif()
