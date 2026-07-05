cmake_minimum_required(VERSION 3.28)
project(wge-kafka-detector VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ===== Options =====
option(WGE_DETECTOR_ENABLE_TESTS "Build unit tests" ON)
option(WGE_DETECTOR_ENABLE_INTEGRATION_TESTS "Build integration tests" OFF)
option(WGE_DETECTOR_LOG_LEVEL "Active log level (1=trace..7=off)" 3)

# ===== Find packages =====
find_package(wge REQUIRED)
find_package(RdKafka REQUIRED)
find_package(Protobuf REQUIRED)
find_package(simdjson REQUIRED)
# RE2 依赖已随 RegexMapper 移除
find_package(yaml-cpp REQUIRED)
find_package(spdlog REQUIRED)
find_package(prometheus-cpp QUIET)
find_package(GTest QUIET)

# ===== Protobuf generation =====
set(PROTO_DIR ${CMAKE_SOURCE_DIR}/proto)
set(PROTO_FILES
    ${PROTO_DIR}/http_access.proto
    ${PROTO_DIR}/wge_alert.proto
    ${PROTO_DIR}/akto_message.proto
    ${PROTO_DIR}/malicious_event.proto
)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})
target_include_directories(protobuf::libprotobuf INTERFACE ${CMAKE_BINARY_DIR})

# ===== Compiler options =====
add_compile_options(-Wall -Wextra -Wpedantic -Werror)
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-g -O0 -fsanitize=address,undefined)
    add_link_options(-fsanitize=address,undefined)
else()
    # Release: 安全加固编译选项
    add_compile_options(-fstack-protector-strong -D_FORTIFY_SOURCE=2 -fPIE)
    add_link_options(-Wl,-z,relro -Wl,-z,now -pie)
endif()

# ===== Core library =====
set(CORE_SOURCES
    # Config
    src/config/config.h
    src/config/config_loader.h src/config/config_loader.cc
    # Mapper
    src/mapper/mapper_config.h src/mapper/mapper_config.cc
    src/mapper/mapper.h src/mapper/mapper.cc
    src/mapper/json_mapper.h src/mapper/json_mapper.cc
    src/mapper/field_applier.h src/mapper/field_applier.cc
    # Kafka
    src/kafka/consumer.h src/kafka/consumer.cc
    src/kafka/producer.h src/kafka/producer.cc
    src/kafka/dlq.h src/kafka/dlq.cc
    # Detector
    src/detector/result.h
    src/detector/worker_pool.h src/detector/worker_pool.cc
    src/detector/http_extractor_adapter.h src/detector/http_extractor_adapter.cc
    src/detector/alert_builder.h src/detector/alert_builder.cc
    src/detector/detector_service.h src/detector/detector_service.cc
    # WAL
    src/wal/wal_writer.h src/wal/wal_writer.cc
    src/wal/wal_relay.h src/wal/wal_relay.cc
    # Metrics
    src/metrics/metrics.h src/metrics/metrics.cc
    # Akto Adapter (WGE 接入 Akto 的协议转换层)
    adapters/akto/akto_adapter.h adapters/akto/akto_adapter.cc
    adapters/akto/akto_preprocessor.h adapters/akto/akto_preprocessor.cc
    adapters/akto/akto_metrics.h
    adapters/akto/akto_dlq.h adapters/akto/akto_dlq.cc
    # Proto
    ${PROTO_SRCS} ${PROTO_HDRS}
)

add_library(wge_kafka_detector_core STATIC ${CORE_SOURCES})
target_include_directories(wge_kafka_detector_core PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/adapters
    ${CMAKE_BINARY_DIR}
)
target_link_libraries(wge_kafka_detector_core PUBLIC
    wge::wge
    RdKafka::rdkafka++
    protobuf::libprotobuf
    simdjson::simdjson
    # RE2 已随 RegexMapper 移除
    yaml-cpp
    spdlog::spdlog
    # WGE SDK 依赖库
    /home/z/my-project/antlr4-local/lib/libantlr4-runtime.a
    /home/z/my-project/sys-libs/usr/lib/x86_64-linux-gnu/libhs.a
    /home/z/my-project/libinjection-local/lib/libinjection.a
    /home/z/my-project/sys-libs/usr/lib/x86_64-linux-gnu/libpcre2-8.a
    /home/z/my-project/sys-libs/usr/lib/x86_64-linux-gnu/libxml2.a
    pthread
    dl
    m
)
if(prometheus-cpp_FOUND)
    target_link_libraries(wge_kafka_detector_core PRIVATE prometheus-cpp::core prometheus-cpp::pull)
    target_compile_definitions(wge_kafka_detector_core PRIVATE WGE_DETECTOR_HAS_PROMETHEUS)
endif()

add_compile_definitions(WGE_LOG_ACTIVE_LEVEL=${WGE_DETECTOR_LOG_LEVEL})

# ===== Main executable =====
add_executable(wge-detector src/main.cc)
target_link_libraries(wge-detector PRIVATE wge_kafka_detector_core)
install(TARGETS wge-detector RUNTIME DESTINATION bin)

# ===== Tests =====
if(WGE_DETECTOR_ENABLE_TESTS AND GTest_FOUND)
    enable_testing()
    set(TEST_SOURCES
        test/unit/test_json_mapper.cc
        test/unit/test_field_applier.cc
        test/unit/test_extractor_adapter.cc
        test/unit/test_alert_builder.cc
        test/unit/test_offset_manager.cc
        test/unit/test_config_loader.cc
        test/unit/test_worker_pool.cc
        # Akto Adapter 集成测试
        adapters/akto/test_akto_adapter.cc
    )
    add_executable(wge_detector_test ${TEST_SOURCES})
    target_link_libraries(wge_detector_test PRIVATE
        wge_kafka_detector_core
        GTest::gtest_main
    )
    add_test(NAME wge_detector_test COMMAND wge_detector_test)
endif()
