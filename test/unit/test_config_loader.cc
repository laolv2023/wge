/**
 * @file test_config_loader.cc
 * @brief ConfigLoader 单元测试 — 验证 YAML 配置加载、验证和热重载
 */

#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <string>

#include "config/config.h"
#include "config/config_loader.h"

using namespace wge::kafka::config;

// ============================================================================
// 测试夹具: 管理临时 YAML 文件
// ============================================================================

class ConfigLoaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = "/tmp/wge_config_test_" +
                    std::to_string(
                        std::chrono::steady_clock::now().time_since_epoch().count());
        std::system(("mkdir -p " + test_dir_).c_str());
    }

    void TearDown() override {
        std::system(("rm -rf " + test_dir_).c_str());
    }

    /// 在测试目录中写入 YAML 文件，返回完整路径
    std::string writeYaml(const std::string& filename,
                          const std::string& content) {
        std::string path = test_dir_ + "/" + filename;
        std::ofstream ofs(path);
        ofs << content;
        ofs.close();
        return path;
    }

    std::string test_dir_;
};

// ============================================================================
// 1. LoadMinimalConfig — 最小配置加载成功
// ============================================================================

TEST_F(ConfigLoaderTest, LoadMinimalConfig) {
    const char* yaml = R"(
kafka:
  consumer:
    bootstrap_servers: "localhost:9092"
    topic: "http-access"
  producer:
    bootstrap_servers: "localhost:9092"
    topic: "wge-alert"
wge:
  rule_files:
    - "/etc/wge/rules/basic.conf"
mapping:
  log_mapping_path: "config/log_mapping.yaml"
)";

    std::string path = writeYaml("minimal.yaml", yaml);

    auto result = ConfigLoader::loadFromFile(path);
    ASSERT_TRUE(result.has_value()) << "Error: " << result.error();

    const auto& cfg = *result;

    // Kafka consumer
    EXPECT_EQ(cfg.kafka.consumer.bootstrap_servers, "localhost:9092");
    EXPECT_EQ(cfg.kafka.consumer.topic, "http-access");
    EXPECT_EQ(cfg.kafka.consumer.group_id, "wge-kafka-detector");  // 默认值

    // Kafka producer
    EXPECT_EQ(cfg.kafka.producer.bootstrap_servers, "localhost:9092");
    EXPECT_EQ(cfg.kafka.producer.topic, "wge-alert");

    // WGE
    ASSERT_EQ(cfg.wge.rule_files.size(), 1u);
    EXPECT_EQ(cfg.wge.rule_files[0], "/etc/wge/rules/basic.conf");

    // Mapping
    EXPECT_EQ(cfg.mapping.log_mapping_path, "config/log_mapping.yaml");

    // 默认值验证
    EXPECT_EQ(cfg.detector.worker_threads, 0);          // 默认 0 (自动)
    EXPECT_EQ(cfg.detector.poll_interval_ms, 100);
    EXPECT_EQ(cfg.observability.log_level, "info");      // 默认值
    EXPECT_EQ(cfg.app_version, "0.1.0");
}

// ============================================================================
// 2. LoadFullConfig — 完整配置加载，所有字段正确
// ============================================================================

TEST_F(ConfigLoaderTest, LoadFullConfig) {
    const char* yaml = R"(
kafka:
  consumer:
    bootstrap_servers: "kafka1:9092,kafka2:9092,kafka3:9092"
    group_id: "wge-detector-prod"
    topic: "http-access-prod"
    auto_offset_reset: "earliest"
    enable_auto_commit: false
    session_timeout_ms: 45000
    max_poll_records: 1000
    heartbeat_interval_ms: 15000
    max_poll_interval_ms: 600000
    fetch_wait_max_ms: 1000
    fetch_min_bytes: 1024
    fetch_max_bytes: 104857600
  producer:
    bootstrap_servers: "kafka1:9092,kafka2:9092"
    topic: "wge-alert-prod"
    dlq_topic: "wge-dlq-prod"
    acks: -1
    compression_type: "zstd"
    linger_ms: 10.0
    batch_size: 2097152
    max_request_size: 2097152
    retries: 5
    retry_backoff_ms: 200
wge:
  rule_files:
    - "/etc/wge/rules/crs.conf"
    - "/etc/wge/rules/custom.conf"
  engine_config_path: "/etc/wge/engine.json"
  rule_update_interval_sec: 120
  engine_init_timeout_ms: 60000
  strict_mode: true
mapping:
  log_mapping_path: "/etc/wge-detector/log_mapping.yaml"
detector:
  poll_interval_ms: 50
  worker_threads: 16
  batch_size: 128
  max_pending_tasks: 4096
  task_timeout_ms: 10000
  health_check_port: 8888
  graceful_shutdown_timeout_ms: 45000
  enable_dlq: true
  wal_dir: "/data/wal"
  wal_segment_max_size: 536870912
observability:
  prometheus_enabled: true
  prometheus_port: 9095
  prometheus_path: "/metrics"
  otel_enabled: true
  otel_endpoint: "http://otel-collector:4317"
  log_level: "debug"
  log_format: "text"
  log_file_path: "/var/log/wge-detector/app.log"
app_version: "1.2.3"
instance_name: "wge-detector-us-east-1"
)";

    std::string path = writeYaml("full.yaml", yaml);

    auto result = ConfigLoader::loadFromFile(path);
    ASSERT_TRUE(result.has_value()) << "Error: " << result.error();

    const auto& c = *result;

    // Consumer
    EXPECT_EQ(c.kafka.consumer.bootstrap_servers, "kafka1:9092,kafka2:9092,kafka3:9092");
    EXPECT_EQ(c.kafka.consumer.group_id, "wge-detector-prod");
    EXPECT_EQ(c.kafka.consumer.topic, "http-access-prod");
    EXPECT_EQ(c.kafka.consumer.auto_offset_reset, "earliest");
    EXPECT_FALSE(c.kafka.consumer.enable_auto_commit);
    EXPECT_EQ(c.kafka.consumer.session_timeout_ms, 45000);
    EXPECT_EQ(c.kafka.consumer.max_poll_records, 1000);
    EXPECT_EQ(c.kafka.consumer.heartbeat_interval_ms, 15000);
    EXPECT_EQ(c.kafka.consumer.max_poll_interval_ms, 600000);
    EXPECT_EQ(c.kafka.consumer.fetch_wait_max_ms, 1000);
    EXPECT_EQ(c.kafka.consumer.fetch_min_bytes, 1024);
    EXPECT_EQ(c.kafka.consumer.fetch_max_bytes, 104857600);

    // Producer
    EXPECT_EQ(c.kafka.producer.bootstrap_servers, "kafka1:9092,kafka2:9092");
    EXPECT_EQ(c.kafka.producer.topic, "wge-alert-prod");
    EXPECT_EQ(c.kafka.producer.dlq_topic, "wge-dlq-prod");
    EXPECT_EQ(c.kafka.producer.acks, -1);
    EXPECT_EQ(c.kafka.producer.compression_type, "zstd");
    EXPECT_DOUBLE_EQ(c.kafka.producer.linger_ms, 10.0);
    EXPECT_EQ(c.kafka.producer.batch_size, 2097152);
    EXPECT_EQ(c.kafka.producer.max_request_size, 2097152);
    EXPECT_EQ(c.kafka.producer.retries, 5);
    EXPECT_EQ(c.kafka.producer.retry_backoff_ms, 200);

    // WGE
    ASSERT_EQ(c.wge.rule_files.size(), 2u);
    EXPECT_EQ(c.wge.rule_files[0], "/etc/wge/rules/crs.conf");
    EXPECT_EQ(c.wge.rule_files[1], "/etc/wge/rules/custom.conf");
    EXPECT_EQ(c.wge.engine_config_path, "/etc/wge/engine.json");
    EXPECT_EQ(c.wge.rule_update_interval_sec, 120);
    EXPECT_EQ(c.wge.engine_init_timeout_ms, 60000);
    EXPECT_TRUE(c.wge.strict_mode);

    // Mapping
    EXPECT_EQ(c.mapping.log_mapping_path, "/etc/wge-detector/log_mapping.yaml");

    // Detector
    EXPECT_EQ(c.detector.poll_interval_ms, 50);
    EXPECT_EQ(c.detector.worker_threads, 16);
    EXPECT_EQ(c.detector.batch_size, 128);
    EXPECT_EQ(c.detector.max_pending_tasks, 4096);
    EXPECT_EQ(c.detector.task_timeout_ms, 10000);
    EXPECT_EQ(c.detector.health_check_port, 8888);
    EXPECT_EQ(c.detector.graceful_shutdown_timeout_ms, 45000);
    EXPECT_TRUE(c.detector.enable_dlq);
    EXPECT_EQ(c.detector.wal_dir, "/data/wal");
    EXPECT_EQ(c.detector.wal_segment_max_size, 536870912);

    // Observability
    EXPECT_TRUE(c.observability.prometheus_enabled);
    EXPECT_EQ(c.observability.prometheus_port, 9095);
    EXPECT_EQ(c.observability.prometheus_path, "/metrics");
    EXPECT_TRUE(c.observability.otel_enabled);
    EXPECT_EQ(c.observability.otel_endpoint, "http://otel-collector:4317");
    EXPECT_EQ(c.observability.log_level, "debug");
    EXPECT_EQ(c.observability.log_format, "text");
    EXPECT_EQ(c.observability.log_file_path, "/var/log/wge-detector/app.log");

    // 顶层
    EXPECT_EQ(c.app_version, "1.2.3");
    EXPECT_EQ(c.instance_name, "wge-detector-us-east-1");
}

// ============================================================================
// 3. LoadMissingFile — 文件不存在返回错误
// ============================================================================

TEST_F(ConfigLoaderTest, LoadMissingFile) {
    auto result = ConfigLoader::loadFromFile("/nonexistent/path/config.yaml");
    EXPECT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("Failed to open"), std::string::npos);
}

// ============================================================================
// 4. LoadInvalidYaml — 非法 YAML 返回错误
// ============================================================================

TEST_F(ConfigLoaderTest, LoadInvalidYaml) {
    const char* bad_yaml = R"(
kafka:
  consumer:
    bootstrap_servers: "localhost:9092"
    topic: "test"
  producer: [this is invalid syntax ::: {{{ 
    bootstrap_servers: "localhost:9092"
)";

    std::string path = writeYaml("invalid.yaml", bad_yaml);

    auto result = ConfigLoader::loadFromFile(path);
    EXPECT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("YAML parse error"), std::string::npos);
}

// ============================================================================
// 5. LoadMissingRequiredField — 缺少必填字段返回错误
// ============================================================================

TEST_F(ConfigLoaderTest, LoadMissingRequiredField) {
    // 缺少 kafka.consumer.topic
    {
        const char* yaml = R"(
kafka:
  consumer:
    bootstrap_servers: "localhost:9092"
  producer:
    bootstrap_servers: "localhost:9092"
    topic: "wge-alert"
wge:
  rule_files:
    - "/etc/wge/rules/basic.conf"
mapping:
  log_mapping_path: "config/log_mapping.yaml"
)";
        std::string path = writeYaml("no_consumer_topic.yaml", yaml);
        auto result = ConfigLoader::loadFromFile(path);
        EXPECT_FALSE(result.has_value());
        EXPECT_NE(result.error().find("kafka.consumer.topic"), std::string::npos);
    }

    // 缺少 kafka.producer.topic
    {
        const char* yaml = R"(
kafka:
  consumer:
    bootstrap_servers: "localhost:9092"
    topic: "http-access"
  producer:
    bootstrap_servers: "localhost:9092"
wge:
  rule_files:
    - "/etc/wge/rules/basic.conf"
mapping:
  log_mapping_path: "config/log_mapping.yaml"
)";
        std::string path = writeYaml("no_producer_topic.yaml", yaml);
        auto result = ConfigLoader::loadFromFile(path);
        EXPECT_FALSE(result.has_value());
        EXPECT_NE(result.error().find("kafka.producer.topic"), std::string::npos);
    }

    // 缺少 wge.rule_files
    {
        const char* yaml = R"(
kafka:
  consumer:
    bootstrap_servers: "localhost:9092"
    topic: "http-access"
  producer:
    bootstrap_servers: "localhost:9092"
    topic: "wge-alert"
mapping:
  log_mapping_path: "config/log_mapping.yaml"
)";
        std::string path = writeYaml("no_rule_files.yaml", yaml);
        auto result = ConfigLoader::loadFromFile(path);
        EXPECT_FALSE(result.has_value());
        EXPECT_NE(result.error().find("wge.rule_files"), std::string::npos);
    }

    // 缺少 mapping.log_mapping_path
    {
        const char* yaml = R"(
kafka:
  consumer:
    bootstrap_servers: "localhost:9092"
    topic: "http-access"
  producer:
    bootstrap_servers: "localhost:9092"
    topic: "wge-alert"
wge:
  rule_files:
    - "/etc/wge/rules/basic.conf"
)";
        std::string path = writeYaml("no_mapping.yaml", yaml);
        auto result = ConfigLoader::loadFromFile(path);
        EXPECT_FALSE(result.has_value());
        EXPECT_NE(result.error().find("mapping.log_mapping_path"), std::string::npos);
    }
}

// ============================================================================
// 6. EnvironmentVariableSubstitution — ${VAR_NAME} 替换正确
// ============================================================================

TEST_F(ConfigLoaderTest, EnvironmentVariableSubstitution) {
    // 设置环境变量
    setenv("TEST_KAFKA_HOST", "kafka-prod:9092", 1);
    setenv("TEST_TOPIC_IN", "http-access-env", 1);
    setenv("TEST_TOPIC_OUT", "wge-alert-env", 1);
    setenv("TEST_RULE_PATH", "/env/rules/basic.conf", 1);
    setenv("TEST_GROUP_ID", "group-from-env", 1);

    const char* yaml = R"(
kafka:
  consumer:
    bootstrap_servers: "${TEST_KAFKA_HOST}"
    group_id: "${TEST_GROUP_ID}"
    topic: "${TEST_TOPIC_IN}"
  producer:
    bootstrap_servers: "${TEST_KAFKA_HOST}"
    topic: "${TEST_TOPIC_OUT}"
wge:
  rule_files:
    - "${TEST_RULE_PATH}"
mapping:
  log_mapping_path: "config/log_mapping.yaml"
)";

    std::string path = writeYaml("envsubst.yaml", yaml);

    auto result = ConfigLoader::loadFromFile(path);
    ASSERT_TRUE(result.has_value()) << "Error: " << result.error();

    const auto& c = *result;

    EXPECT_EQ(c.kafka.consumer.bootstrap_servers, "kafka-prod:9092");
    EXPECT_EQ(c.kafka.consumer.group_id, "group-from-env");
    EXPECT_EQ(c.kafka.consumer.topic, "http-access-env");
    EXPECT_EQ(c.kafka.producer.bootstrap_servers, "kafka-prod:9092");
    EXPECT_EQ(c.kafka.producer.topic, "wge-alert-env");
    ASSERT_EQ(c.wge.rule_files.size(), 1u);
    EXPECT_EQ(c.wge.rule_files[0], "/env/rules/basic.conf");

    // 清理
    unsetenv("TEST_KAFKA_HOST");
    unsetenv("TEST_TOPIC_IN");
    unsetenv("TEST_TOPIC_OUT");
    unsetenv("TEST_RULE_PATH");
    unsetenv("TEST_GROUP_ID");
}

// ============================================================================
// 7. EnvironmentVariableWithDefault — ${VAR_NAME:-default} 默认值语法
// ============================================================================

TEST_F(ConfigLoaderTest, EnvironmentVariableWithDefault) {
    // 确保环境变量未设置
    unsetenv("NONEXISTENT_VAR_FOR_TEST");

    const char* yaml = R"(
kafka:
  consumer:
    bootstrap_servers: "localhost:9092"
    group_id: "${NONEXISTENT_VAR_FOR_TEST:-fallback-group}"
    topic: "${NONEXISTENT_VAR_FOR_TEST:-fallback-topic}"
  producer:
    bootstrap_servers: "localhost:9092"
    topic: "${NONEXISTENT_VAR_FOR_TEST:-fallback-alert-topic}"
wge:
  rule_files:
    - "/etc/wge/rules/basic.conf"
mapping:
  log_mapping_path: "config/log_mapping.yaml"
)";

    std::string path = writeYaml("envdefault.yaml", yaml);

    auto result = ConfigLoader::loadFromFile(path);
    ASSERT_TRUE(result.has_value()) << "Error: " << result.error();

    const auto& c = *result;

    // 未设环境变量时使用默认值
    EXPECT_EQ(c.kafka.consumer.group_id, "fallback-group");
    EXPECT_EQ(c.kafka.consumer.topic, "fallback-topic");
    EXPECT_EQ(c.kafka.producer.topic, "fallback-alert-topic");
}

// ============================================================================
// 8. ReloadConfig — reload() 只更新可热加载字段
// ============================================================================

TEST_F(ConfigLoaderTest, ReloadConfig) {
    // 基础配置
    const char* base_yaml = R"(
kafka:
  consumer:
    bootstrap_servers: "localhost:9092"
    topic: "http-access"
  producer:
    bootstrap_servers: "localhost:9092"
    topic: "wge-alert"
wge:
  rule_files:
    - "/etc/wge/rules/basic.conf"
mapping:
  log_mapping_path: "config/log_mapping.yaml"
observability:
  log_level: "info"
detector:
  poll_interval_ms: 100
)";

    std::string base_path = writeYaml("base.yaml", base_yaml);
    auto base_result = ConfigLoader::loadFromFile(base_path);
    ASSERT_TRUE(base_result.has_value());
    AppConfig base = std::move(*base_result);

    // 新配置只修改部分字段（如 log_level 和 poll_interval_ms）
    const char* reload_yaml = R"(
observability:
  log_level: "debug"
detector:
  poll_interval_ms: 50
)";

    std::string reload_path = writeYaml("reload.yaml", reload_yaml);
    auto reload_result = ConfigLoader::reload(base, reload_path);
    ASSERT_TRUE(reload_result.has_value()) << "Error: " << reload_result.error();

    const auto& merged = *reload_result;

    // 热加载的字段被更新
    EXPECT_EQ(merged.observability.log_level, "debug");
    EXPECT_EQ(merged.detector.poll_interval_ms, 50);

    // 未在 reload 文件中提及的字段保持 base 值
    EXPECT_EQ(merged.kafka.consumer.topic, "http-access");
    EXPECT_EQ(merged.kafka.producer.topic, "wge-alert");
    EXPECT_EQ(merged.wge.rule_files.size(), 1u);
    EXPECT_EQ(merged.mapping.log_mapping_path, "config/log_mapping.yaml");
}

// ============================================================================
// 9. DefaultValues — 未设置的可选字段使用默认值
// ============================================================================

TEST_F(ConfigLoaderTest, DefaultValues) {
    // 验证 AppConfig 的默认值
    AppConfig cfg;

    // Consumer 默认值
    EXPECT_EQ(cfg.kafka.consumer.bootstrap_servers, "localhost:9092");
    EXPECT_EQ(cfg.kafka.consumer.group_id, "wge-kafka-detector");
    EXPECT_TRUE(cfg.kafka.consumer.topic.empty());  // 必填，无默认值
    EXPECT_EQ(cfg.kafka.consumer.auto_offset_reset, "latest");
    EXPECT_FALSE(cfg.kafka.consumer.enable_auto_commit);
    EXPECT_EQ(cfg.kafka.consumer.session_timeout_ms, 30'000);
    EXPECT_EQ(cfg.kafka.consumer.max_poll_records, 500);
    EXPECT_EQ(cfg.kafka.consumer.heartbeat_interval_ms, 10'000);
    EXPECT_EQ(cfg.kafka.consumer.max_poll_interval_ms, 300'000);
    EXPECT_EQ(cfg.kafka.consumer.fetch_wait_max_ms, 500);
    EXPECT_EQ(cfg.kafka.consumer.fetch_min_bytes, 1);
    EXPECT_EQ(cfg.kafka.consumer.fetch_max_bytes, 50 * 1024 * 1024);

    // Producer 默认值
    EXPECT_EQ(cfg.kafka.producer.bootstrap_servers, "localhost:9092");
    EXPECT_TRUE(cfg.kafka.producer.topic.empty());  // 必填
    EXPECT_EQ(cfg.kafka.producer.dlq_topic, "http-access-dlq");
    EXPECT_EQ(cfg.kafka.producer.acks, -1);
    EXPECT_EQ(cfg.kafka.producer.compression_type, "lz4");
    EXPECT_DOUBLE_EQ(cfg.kafka.producer.linger_ms, 5.0);
    EXPECT_EQ(cfg.kafka.producer.batch_size, 1'048'576);
    EXPECT_EQ(cfg.kafka.producer.max_request_size, 1'048'576);
    EXPECT_EQ(cfg.kafka.producer.retries, 3);
    EXPECT_EQ(cfg.kafka.producer.retry_backoff_ms, 100);

    // WGE 默认值
    EXPECT_TRUE(cfg.wge.rule_files.empty());  // 必填
    EXPECT_EQ(cfg.wge.engine_config_path, "/etc/wge/engine.json");
    EXPECT_EQ(cfg.wge.rule_update_interval_sec, 60);
    EXPECT_EQ(cfg.wge.engine_init_timeout_ms, 30'000);
    EXPECT_FALSE(cfg.wge.strict_mode);

    // Mapping 默认值
    EXPECT_EQ(cfg.mapping.log_mapping_path, "config/log_mapping.yaml");

    // Detector 默认值
    EXPECT_EQ(cfg.detector.poll_interval_ms, 100);
    EXPECT_EQ(cfg.detector.worker_threads, 0);
    EXPECT_EQ(cfg.detector.batch_size, 64);
    EXPECT_EQ(cfg.detector.max_pending_tasks, 1024);
    EXPECT_EQ(cfg.detector.task_timeout_ms, 5'000);
    EXPECT_EQ(cfg.detector.health_check_port, 8080);
    EXPECT_EQ(cfg.detector.graceful_shutdown_timeout_ms, 30'000);
    EXPECT_TRUE(cfg.detector.enable_dlq);
    EXPECT_EQ(cfg.detector.wal_dir, "/var/lib/wge-detector/wal");
    EXPECT_EQ(cfg.detector.wal_segment_max_size, 256 * 1024 * 1024);

    // Observability 默认值
    EXPECT_TRUE(cfg.observability.prometheus_enabled);
    EXPECT_EQ(cfg.observability.prometheus_port, 9090);
    EXPECT_EQ(cfg.observability.prometheus_path, "/metrics");
    EXPECT_FALSE(cfg.observability.otel_enabled);
    EXPECT_EQ(cfg.observability.otel_endpoint, "http://localhost:4317");
    EXPECT_EQ(cfg.observability.log_level, "info");
    EXPECT_EQ(cfg.observability.log_format, "json");
    EXPECT_TRUE(cfg.observability.log_file_path.empty());

    // 顶层
    EXPECT_EQ(cfg.app_version, "0.1.0");
    EXPECT_EQ(cfg.instance_name, "wge-detector-01");
}

// ============================================================================
// 10. LoadEmptyYaml — 空 YAML 文档返回错误 (所有必填字段缺失)
// ============================================================================

TEST_F(ConfigLoaderTest, LoadEmptyYaml) {
    std::string path = writeYaml("empty.yaml", "---\n");

    auto result = ConfigLoader::loadFromFile(path);
    EXPECT_FALSE(result.has_value());
    // 至少缺少 kafka.consumer.topic
}

// ============================================================================
// 11. ReloadWithMissingFile — reload() 文件不存在返回错误
// ============================================================================

TEST_F(ConfigLoaderTest, ReloadWithMissingFile) {
    AppConfig base;
    base.kafka.consumer.bootstrap_servers = "localhost:9092";
    base.kafka.consumer.topic = "test";
    base.kafka.producer.bootstrap_servers = "localhost:9092";
    base.kafka.producer.topic = "test-out";
    base.wge.rule_files = {"/rules/basic.conf"};
    base.mapping.log_mapping_path = "config/log_mapping.yaml";

    auto result = ConfigLoader::reload(base, "/nonexistent/reload.yaml");
    EXPECT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("Failed to open"), std::string::npos);
}

// ============================================================================
// 12. PartialConfig — 部分配置段缺失不影响加载
// ============================================================================

TEST_F(ConfigLoaderTest, PartialConfig) {
    // 只提供 kafka 和 wge，其他段使用默认值
    const char* yaml = R"(
kafka:
  consumer:
    bootstrap_servers: "localhost:9092"
    topic: "http-access"
  producer:
    bootstrap_servers: "localhost:9092"
    topic: "wge-alert"
wge:
  rule_files:
    - "/etc/wge/rules/basic.conf"
mapping:
  log_mapping_path: "config/log_mapping.yaml"
)";

    std::string path = writeYaml("partial.yaml", yaml);

    auto result = ConfigLoader::loadFromFile(path);
    ASSERT_TRUE(result.has_value()) << "Error: " << result.error();

    const auto& c = *result;

    // detector 和 observability 使用默认值
    EXPECT_EQ(c.detector.poll_interval_ms, 100);
    EXPECT_EQ(c.detector.worker_threads, 0);
    EXPECT_EQ(c.observability.log_level, "info");
    EXPECT_FALSE(c.observability.otel_enabled);
}

// ============================================================================
// 13. EnvironmentVariableUnset — 未设环境变量替换为空字符串
// ============================================================================

TEST_F(ConfigLoaderTest, EnvironmentVariableUnset) {
    unsetenv("DEFINITELY_NOT_SET_VAR_12345");

    const char* yaml = R"(
kafka:
  consumer:
    bootstrap_servers: "${DEFINITELY_NOT_SET_VAR_12345}"
    topic: "http-access"
  producer:
    bootstrap_servers: "localhost:9092"
    topic: "wge-alert"
wge:
  rule_files:
    - "/etc/wge/rules/basic.conf"
mapping:
  log_mapping_path: "config/log_mapping.yaml"
)";

    std::string path = writeYaml("unset_env.yaml", yaml);

    auto result = ConfigLoader::loadFromFile(path);
    // bootstrap_servers 为空，验证会失败
    EXPECT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("kafka.consumer.bootstrap_servers"),
              std::string::npos);
}
