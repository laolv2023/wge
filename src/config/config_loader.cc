/**
 * @file config_loader.cc
 * @brief ConfigLoader 实现
 */

#include "config/config_loader.h"

#include <cstdlib>
#include <fstream>
#include <regex>
#include <sstream>

#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"

namespace wge::kafka::config {

// ============================================================================
// 辅助: 递归设置 YAML 节点值到目标字段
// ============================================================================

namespace {

/**
 * @brief 从 YAML 节点读取字符串，缺失返回空 optional
 */
std::optional<std::string> getOptionalString(const YAML::Node& node,
                                             const std::string& key) {
    if (!node || !node[key] || node[key].IsNull()) return std::nullopt;
    return node[key].as<std::string>();
}

/**
 * @brief 从 YAML 节点读取 int32，缺失返回空 optional
 */
std::optional<int32_t> getOptionalInt32(const YAML::Node& node,
                                        const std::string& key) {
    if (!node || !node[key] || node[key].IsNull()) return std::nullopt;
    return node[key].as<int32_t>();
}

/**
 * @brief 从 YAML 节点读取 int64，缺失返回空 optional
 */
std::optional<int64_t> getOptionalInt64(const YAML::Node& node,
                                        const std::string& key) {
    if (!node || !node[key] || node[key].IsNull()) return std::nullopt;
    return node[key].as<int64_t>();
}

/**
 * @brief 从 YAML 节点读取 double，缺失返回空 optional
 */
std::optional<double> getOptionalDouble(const YAML::Node& node,
                                        const std::string& key) {
    if (!node || !node[key] || node[key].IsNull()) return std::nullopt;
    return node[key].as<double>();
}

/**
 * @brief 从 YAML 节点读取 bool，缺失返回空 optional
 */
std::optional<bool> getOptionalBool(const YAML::Node& node,
                                    const std::string& key) {
    if (!node || !node[key] || node[key].IsNull()) return std::nullopt;
    return node[key].as<bool>();
}

/**
 * @brief 从 YAML 节点读取字符串数组，缺失返回空 optional
 */
std::optional<std::vector<std::string>> getOptionalStringList(
    const YAML::Node& node, const std::string& key) {
    if (!node || !node[key] || !node[key].IsSequence())
        return std::nullopt;
    std::vector<std::string> result;
    for (const auto& item : node[key]) {
        result.emplace_back(item.as<std::string>());
    }
    return result;
}

/**
 * @brief 设置字符串字段（若 optional 有值）
 */
void setIfPresent(std::string& field,
                  const std::optional<std::string>& opt) {
    if (opt) field = ConfigLoader::substituteEnvVars(*opt);
}

/**
 * @brief 设置 int32 字段
 */
void setIfPresent(int32_t& field, const std::optional<int32_t>& opt) {
    if (opt) field = *opt;
}

/**
 * @brief 设置 int64 字段
 */
void setIfPresent(int64_t& field, const std::optional<int64_t>& opt) {
    if (opt) field = *opt;
}

/**
 * @brief 设置 double 字段
 */
void setIfPresent(double& field, const std::optional<double>& opt) {
    if (opt) field = *opt;
}

/**
 * @brief 设置 bool 字段
 */
void setIfPresent(bool& field, const std::optional<bool>& opt) {
    if (opt) field = *opt;
}

/**
 * @brief 设置字符串 vector 字段
 */
void setIfPresent(std::vector<std::string>& field,
                  const std::optional<std::vector<std::string>>& opt) {
    if (!opt) return;
    field.clear();
    for (const auto& s : *opt) {
        field.emplace_back(ConfigLoader::substituteEnvVars(s));
    }
}

/**
 * @brief 解析 ConsumerConfig
 */
void parseConsumer(ConsumerConfig& cfg, const YAML::Node& node) {
    setIfPresent(cfg.bootstrap_servers,
                 getOptionalString(node, "bootstrap_servers"));
    setIfPresent(cfg.group_id, getOptionalString(node, "group_id"));
    setIfPresent(cfg.topic, getOptionalString(node, "topic"));
    setIfPresent(cfg.auto_offset_reset,
                 getOptionalString(node, "auto_offset_reset"));
    setIfPresent(cfg.enable_auto_commit,
                 getOptionalBool(node, "enable_auto_commit"));
    setIfPresent(cfg.session_timeout_ms,
                 getOptionalInt32(node, "session_timeout_ms"));
    setIfPresent(cfg.max_poll_records,
                 getOptionalInt32(node, "max_poll_records"));
    setIfPresent(cfg.heartbeat_interval_ms,
                 getOptionalInt32(node, "heartbeat_interval_ms"));
    setIfPresent(cfg.max_poll_interval_ms,
                 getOptionalInt32(node, "max_poll_interval_ms"));
    setIfPresent(cfg.fetch_wait_max_ms,
                 getOptionalInt32(node, "fetch_wait_max_ms"));
    setIfPresent(cfg.fetch_min_bytes,
                 getOptionalInt32(node, "fetch_min_bytes"));
    setIfPresent(cfg.fetch_max_bytes,
                 getOptionalInt32(node, "fetch_max_bytes"));
}

/**
 * @brief 解析 ProducerConfig
 */
void parseProducer(ProducerConfig& cfg, const YAML::Node& node) {
    setIfPresent(cfg.bootstrap_servers,
                 getOptionalString(node, "bootstrap_servers"));
    setIfPresent(cfg.topic, getOptionalString(node, "topic"));
    setIfPresent(cfg.dlq_topic, getOptionalString(node, "dlq_topic"));
    setIfPresent(cfg.acks, getOptionalInt32(node, "acks"));
    setIfPresent(cfg.compression_type,
                 getOptionalString(node, "compression_type"));
    setIfPresent(cfg.linger_ms, getOptionalDouble(node, "linger_ms"));
    setIfPresent(cfg.batch_size, getOptionalInt32(node, "batch_size"));
    setIfPresent(cfg.max_request_size,
                 getOptionalInt32(node, "max_request_size"));
    setIfPresent(cfg.retries, getOptionalInt32(node, "retries"));
    setIfPresent(cfg.retry_backoff_ms,
                 getOptionalInt32(node, "retry_backoff_ms"));
}

/**
 * @brief 解析 WgeConfig
 */
void parseWge(WgeConfig& cfg, const YAML::Node& node) {
    setIfPresent(cfg.rule_files, getOptionalStringList(node, "rule_files"));
    setIfPresent(cfg.engine_config_path,
                 getOptionalString(node, "engine_config_path"));
    setIfPresent(cfg.rule_update_interval_sec,
                 getOptionalInt32(node, "rule_update_interval_sec"));
    setIfPresent(cfg.engine_init_timeout_ms,
                 getOptionalInt32(node, "engine_init_timeout_ms"));
    setIfPresent(cfg.strict_mode, getOptionalBool(node, "strict_mode"));
}

/**
 * @brief 解析 MappingConfig
 */
void parseMapping(MappingConfig& cfg, const YAML::Node& node) {
    setIfPresent(cfg.log_mapping_path,
                 getOptionalString(node, "log_mapping_path"));
}

/**
 * @brief 解析 DetectorConfig
 */
void parseDetector(DetectorConfig& cfg, const YAML::Node& node) {
    setIfPresent(cfg.poll_interval_ms,
                 getOptionalInt32(node, "poll_interval_ms"));
    setIfPresent(cfg.worker_threads,
                 getOptionalInt32(node, "worker_threads"));
    setIfPresent(cfg.batch_size, getOptionalInt32(node, "batch_size"));
    setIfPresent(cfg.max_pending_tasks,
                 getOptionalInt32(node, "max_pending_tasks"));
    setIfPresent(cfg.task_timeout_ms,
                 getOptionalInt32(node, "task_timeout_ms"));
    setIfPresent(cfg.health_check_port,
                 getOptionalInt32(node, "health_check_port"));
    setIfPresent(cfg.graceful_shutdown_timeout_ms,
                 getOptionalInt32(node, "graceful_shutdown_timeout_ms"));
    setIfPresent(cfg.enable_dlq, getOptionalBool(node, "enable_dlq"));
    setIfPresent(cfg.wal_dir, getOptionalString(node, "wal_dir"));
    setIfPresent(cfg.wal_segment_max_size,
                 getOptionalInt64(node, "wal_segment_max_size"));
}

/**
 * @brief 解析 ObservabilityConfig
 */
void parseObservability(ObservabilityConfig& cfg, const YAML::Node& node) {
    setIfPresent(cfg.prometheus_enabled,
                 getOptionalBool(node, "prometheus_enabled"));
    setIfPresent(cfg.prometheus_port,
                 getOptionalInt32(node, "prometheus_port"));
    setIfPresent(cfg.prometheus_path,
                 getOptionalString(node, "prometheus_path"));
    setIfPresent(cfg.otel_enabled, getOptionalBool(node, "otel_enabled"));
    setIfPresent(cfg.otel_endpoint,
                 getOptionalString(node, "otel_endpoint"));
    setIfPresent(cfg.log_level, getOptionalString(node, "log_level"));
    setIfPresent(cfg.log_format, getOptionalString(node, "log_format"));
    setIfPresent(cfg.log_file_path,
                 getOptionalString(node, "log_file_path"));
}

}  // namespace

// ============================================================================
// 环境变量替换
// ============================================================================

std::string ConfigLoader::substituteEnvVars(const std::string& value) {
    // 匹配 ${VAR_NAME} 或 ${VAR_NAME:-default}
    thread_local const std::regex env_pattern(R"(\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\})");

    std::string result = value;
    std::smatch match;

    // 循环替换，最多 32 层（防止无限递归）
    for (int iteration = 0; iteration < 32; ++iteration) {
        std::string working = result;
        if (!std::regex_search(working, match, env_pattern)) break;

        std::string replaced;
        size_t last_pos = 0;

        auto it = std::sregex_iterator(working.begin(), working.end(),
                                       env_pattern);
        auto end = std::sregex_iterator();

        for (; it != end; ++it) {
            match = *it;
            // 追加匹配前的文本
            replaced.append(working, last_pos,
                            static_cast<size_t>(match.position()) - last_pos);

            std::string var_name = match[1].str();
            std::string default_val = match[2].matched ? match[2].str() : "";

            const char* env_val = std::getenv(var_name.c_str());
            if (env_val != nullptr) {
                replaced.append(env_val);
            } else {
                replaced.append(default_val);
            }

            last_pos = static_cast<size_t>(match.position() + match.length());
        }
        // 追加剩余文本
        replaced.append(working, last_pos, working.length() - last_pos);

        result = std::move(replaced);
    }

    return result;
}

// ============================================================================
// 验证
// ============================================================================

std::expected<void, std::string> ConfigLoader::validate(
    const AppConfig& config) {
    // 验证 Kafka consumer topic
    if (config.kafka.consumer.topic.empty()) {
        return std::unexpected(
            "Missing required field: kafka.consumer.topic");
    }

    // 验证 Kafka producer topic
    if (config.kafka.producer.topic.empty()) {
        return std::unexpected(
            "Missing required field: kafka.producer.topic");
    }

    // 验证 WGE rule_files
    if (config.wge.rule_files.empty()) {
        return std::unexpected(
            "Missing required field: wge.rule_files (at least one rule file "
            "must be specified)");
    }

    // 验证 Mapping 配置路径
    if (config.mapping.log_mapping_path.empty()) {
        return std::unexpected(
            "Missing required field: mapping.log_mapping_path");
    }

    // 验证 worker_threads 合理性
    if (config.detector.worker_threads < 0) {
        return std::unexpected(
            "Invalid value for detector.worker_threads: must be >= 0");
    }

    // 验证 batch_size 合理性
    if (config.detector.batch_size < 1 || config.detector.batch_size > 10'000) {
        return std::unexpected(
            "Invalid value for detector.batch_size: must be in [1, 10000]");
    }

    // 验证 task_timeout_ms 合理性
    if (config.detector.task_timeout_ms < 100 ||
        config.detector.task_timeout_ms > 300'000) {
        return std::unexpected(
            "Invalid value for detector.task_timeout_ms: must be in [100, "
            "300000]");
    }

    // 验证 consumer bootstrap_servers
    if (config.kafka.consumer.bootstrap_servers.empty()) {
        return std::unexpected(
            "Missing required field: kafka.consumer.bootstrap_servers");
    }

    // 验证 producer bootstrap_servers
    if (config.kafka.producer.bootstrap_servers.empty()) {
        return std::unexpected(
            "Missing required field: kafka.producer.bootstrap_servers");
    }

    return {};
}

// ============================================================================
// loadFromFile
// ============================================================================

std::expected<AppConfig, std::string> ConfigLoader::loadFromFile(
    const std::string& path) {
    AppConfig config;

    // 1. 读取 YAML 文件
    YAML::Node root;
    try {
        root = YAML::LoadFile(path);
    } catch (const YAML::BadFile& e) {
        return std::unexpected(
            std::string("Failed to open config file: ") + path +
            " - " + e.what());
    } catch (const YAML::ParserException& e) {
        return std::unexpected(
            std::string("YAML parse error in ") + path + ": " + e.what());
    }

    if (!root || root.IsNull()) {
        return std::unexpected(
            std::string("Empty or null YAML root in: ") + path);
    }

    // 2. 解析各子配置段（缺失的段使用默认值，不报错）
    try {
        if (root["kafka"] && root["kafka"]["consumer"]) {
            parseConsumer(config.kafka.consumer, root["kafka"]["consumer"]);
        }
        if (root["kafka"] && root["kafka"]["producer"]) {
            parseProducer(config.kafka.producer, root["kafka"]["producer"]);
        }
        if (root["wge"]) {
            parseWge(config.wge, root["wge"]);
        }
        if (root["mapping"]) {
            parseMapping(config.mapping, root["mapping"]);
        }
        if (root["detector"]) {
            parseDetector(config.detector, root["detector"]);
        }
        if (root["observability"]) {
            parseObservability(config.observability, root["observability"]);
        }

        // 顶层字段
        setIfPresent(config.app_version,
                     getOptionalString(root, "app_version"));
        setIfPresent(config.instance_name,
                     getOptionalString(root, "instance_name"));
    } catch (const YAML::Exception& e) {
        return std::unexpected(
            std::string("YAML traversal error in ") + path + ": " + e.what());
    } catch (const std::exception& e) {
        return std::unexpected(
            std::string("Unexpected error parsing config: ") + e.what());
    }

    // 3. 验证必填字段
    auto validation_result = validate(config);
    if (!validation_result) {
        return std::unexpected(validation_result.error());
    }

    return config;
}

// ============================================================================
// reload
// ============================================================================

std::expected<AppConfig, std::string> ConfigLoader::reload(
    const AppConfig& base, const std::string& path) {
    // 从 base 开始，使得新配置中未指定的字段回退到 base 的值
    AppConfig merged = base;

    // 加载新文件
    YAML::Node root;
    try {
        root = YAML::LoadFile(path);
    } catch (const YAML::BadFile& e) {
        return std::unexpected(
            std::string("Failed to open config file for reload: ") + path +
            " - " + e.what());
    } catch (const YAML::ParserException& e) {
        return std::unexpected(
            std::string("YAML parse error during reload: ") + path + " - " +
            e.what());
    }

    if (!root || root.IsNull()) {
        return std::unexpected("Empty YAML document during reload: " + path);
    }

    try {
        if (root["kafka"] && root["kafka"]["consumer"]) {
            parseConsumer(merged.kafka.consumer, root["kafka"]["consumer"]);
        }
        if (root["kafka"] && root["kafka"]["producer"]) {
            parseProducer(merged.kafka.producer, root["kafka"]["producer"]);
        }
        if (root["wge"]) {
            parseWge(merged.wge, root["wge"]);
        }
        if (root["mapping"]) {
            parseMapping(merged.mapping, root["mapping"]);
        }
        if (root["detector"]) {
            parseDetector(merged.detector, root["detector"]);
        }
        if (root["observability"]) {
            parseObservability(merged.observability, root["observability"]);
        }

        setIfPresent(merged.app_version,
                     getOptionalString(root, "app_version"));
        setIfPresent(merged.instance_name,
                     getOptionalString(root, "instance_name"));
    } catch (const YAML::Exception& e) {
        return std::unexpected(
            std::string("YAML traversal error during reload: ") + e.what());
    } catch (const std::exception& e) {
        return std::unexpected(
            std::string("Unexpected error during reload: ") + e.what());
    }

    // 验证合并后的配置
    auto validation_result = validate(merged);
    if (!validation_result) {
        return std::unexpected(validation_result.error());
    }

    spdlog::info("Config reloaded successfully from {}", path);
    return merged;
}

}  // namespace wge::kafka::config
