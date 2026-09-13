add_library(phototask_kafka STATIC
  ../phototask/kafka_protocol.cpp
  ../phototask/kafka_publisher.cpp
  ../phototask/task_admission.cpp
  ../phototask/kafka_consumer.cpp)
cp_project_target(phototask_kafka)
target_include_directories(phototask_kafka PUBLIC "${PROJECT_SOURCE_DIR}/include")
target_link_libraries(phototask_kafka PUBLIC phototask_runtime RdKafka::rdkafka++)
