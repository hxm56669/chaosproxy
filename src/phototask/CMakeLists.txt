set(PHOTOTASK_PROTO "${PROJECT_SOURCE_DIR}/proto/phototask/v1/task_event.proto")
set(PHOTOTASK_PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/phototask/v1/task_event.pb.cc")
set(PHOTOTASK_PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/phototask/v1/task_event.pb.h")
add_custom_command(
  OUTPUT ${PHOTOTASK_PROTO_SRCS} ${PHOTOTASK_PROTO_HDRS}
  COMMAND protobuf::protoc
  ARGS --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
       --proto_path=${PROJECT_SOURCE_DIR}/proto
       phototask/v1/task_event.proto
  DEPENDS ${PHOTOTASK_PROTO}
  VERBATIM)

add_library(phototask_runtime STATIC
  runtime_probe.cpp
  model/model.cpp
  filesystem_store.cpp
  mysql_store.cpp
  task_pipeline.cpp
  redis_cache.cpp
  dependency_guard.cpp
  photo_service.cpp
  runtime_status.cpp
  ${PHOTOTASK_PROTO_SRCS}
  ${PHOTOTASK_PROTO_HDRS})
cp_project_target(phototask_runtime)
target_include_directories(phototask_runtime PUBLIC
  "${PROJECT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(phototask_runtime PUBLIC chaosproxy_core Drogon::Drogon
  protobuf::libprotobuf OpenSSL::Crypto JsonCpp::JsonCpp hiredis::hiredis)
