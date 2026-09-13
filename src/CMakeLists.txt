add_library(cp_common STATIC
  common/status.cpp
  common/logging.cpp
)
cp_project_target(cp_common)
target_include_directories(cp_common PUBLIC "${PROJECT_SOURCE_DIR}/include")
target_link_libraries(cp_common PUBLIC spdlog::spdlog Threads::Threads
  PRIVATE supc++)

add_library(chaosproxy_core STATIC
  proxy/socket_ops.cpp
  proxy/buffer.cpp
  proxy/event_loop.cpp
  proxy/timer_queue.cpp
  proxy/toxics.cpp
  proxy/toxic_pipeline.cpp
  proxy/config.cpp
  proxy/proxy_server.cpp
  proxy/trace_writer.cpp
  control/control_protocol.cpp
  control/control_server.cpp
  proxy/connection_table.cpp
  proxy/connection_pair.cpp
)
cp_project_target(chaosproxy_core)
target_include_directories(chaosproxy_core PUBLIC "${PROJECT_SOURCE_DIR}/include")
target_link_libraries(chaosproxy_core PUBLIC cp_common JsonCpp::JsonCpp)
