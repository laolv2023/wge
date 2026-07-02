#pragma once

/**
 * @file config.h
 * @brief 全局配置结构体定义
 *
 * 本文件定义 WGE-Kafka Detector 所需的所有配置结构体。
 * 所有字段均使用 C++ 默认成员初始化器提供生产级默认值，
 * 未显式配置的字段自动回退到安全默认值。
 *
 * 线程安全: 所有结构体为 Plain-Old-Data (POD) 风格，
 *           读取操作天然线程安全。修改操作需外部同步。
 */

#include <cstdint>
#include <string>
#include <vector>

namespace wge::kafka::config {

// ============================================================================
// Kafka 消费者配置
// ============================================================================

/**
 * @brief Kafka Consumer 配置
 *
 * 控制消费者如何连接到 Kafka 集群、消费哪些 topic 以及消费行为参数。
 */
struct ConsumerConfig {
    /// @brief Kafka broker 地址列表，逗号分隔。默认 "localhost:9092"
    std::string bootstrap_servers{"localhost:9092"};

    /// @brief Consumer group ID，用于 offset 管理和负载均衡
    std::string group_id{"wge-kafka-detector"};

    /// @brief 要订阅的 Kafka topic 名称（必填，运行时验证）
    std::string topic{};

    /// @brief 初始 offset 策略: "earliest" | "latest" | "error"
    std::string auto_offset_reset{"latest"};

    /// @brief 是否启用自动提交 offset。生产环境建议 false，由应用手动提交
    bool enable_auto_commit{false};

    /// @brief 会话超时时间 (ms)，超过此时间无心跳则触发 rebalance
    int32_t session_timeout_ms{30'000};

    /// @brief 单次 poll 最大拉取消息数
    int32_t max_poll_records{500};

    /// @brief 心跳间隔 (ms)，应小于 session_timeout_ms 的 1/3
    int32_t heartbeat_interval_ms{10'000};

    /// @brief 两次 poll 之间的最大间隔 (ms)，超过则被认为失活
    int32_t max_poll_interval_ms{300'000};

    /// @brief fetch 等待最大时间 (ms)
    int32_t fetch_wait_max_ms{500};

    /// @brief 分区 fetch 最小字节数
    int32_t fetch_min_bytes{1};

    /// @brief 分区 fetch 最大字节数
    int32_t fetch_max_bytes{50 * 1024 * 1024};  // 50 MB

    /// @brief 安全协议: "plaintext" | "ssl" | "sasl_plaintext" | "sasl_ssl"
    std::string security_protocol{"plaintext"};

    /// @brief SASL 机制: "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512" | "GSSAPI"
    std::string sasl_mechanism{};

    /// @brief SASL 用户名
    std::string sasl_username{};

    /// @brief SASL 密码
    std::string sasl_password{};

    /// @brief 额外配置属性 (key=value)，传递给底层 rdkafka
    std::vector<std::pair<std::string, std::string>> extra_properties{};
};

// ============================================================================
// Kafka 生产者配置
// ============================================================================

/**
 * @brief Kafka Producer 配置
 *
 * 控制生产者如何将检测结果和 DLQ 消息写入 Kafka。
 */
struct ProducerConfig {
    /// @brief Kafka broker 地址列表，逗号分隔。默认 "localhost:9092"
    std::string bootstrap_servers{"localhost:9092"};

    /// @brief 输出的告警 topic 名称（必填，运行时验证）
    std::string topic{};

    /// @brief 死信队列 (DLQ) topic 名称
    std::string dlq_topic{"http-access-dlq"};

    /// @brief 确认模式: 0 (无确认), 1 (leader), -1 (all ISR)
    int32_t acks{-1};

    /// @brief 压缩类型: "none" | "gzip" | "snappy" | "lz4" | "zstd"
    std::string compression_type{"lz4"};

    /// @brief 消息批次累积等待时间 (ms)
    double linger_ms{5.0};

    /// @brief 单个批次最大字节数
    int32_t batch_size{1'048'576};  // 1 MB

    /// @brief 请求最大字节数
    int32_t max_request_size{1'048'576};

    /// @brief 重试次数
    int32_t retries{3};

    /// @brief 重试间隔 (ms)
    int32_t retry_backoff_ms{100};

    /// @brief 安全协议: "plaintext" | "ssl" | "sasl_plaintext" | "sasl_ssl"
    std::string security_protocol{"plaintext"};

    /// @brief SASL 机制: "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512" | "GSSAPI"
    std::string sasl_mechanism{};

    /// @brief SASL 用户名
    std::string sasl_username{};

    /// @brief SASL 密码
    std::string sasl_password{};

    /// @brief 额外配置属性
    std::vector<std::pair<std::string, std::string>> extra_properties{};
};

// ============================================================================
// 完整 Kafka 配置
// ============================================================================

/**
 * @brief Kafka 配置聚合
 */
struct KafkaConfig {
    ConsumerConfig consumer{};
    ProducerConfig producer{};
};

// ============================================================================
// WGE 引擎配置
// ============================================================================

/**
 * @brief WGE (Web Gateway Engine) 检测引擎配置
 */
struct WgeConfig {
    /// @brief WGE 规则文件路径列表（必填，运行时验证）
    std::vector<std::string> rule_files{};

    /// @brief WGE 引擎配置文件路径 (JSON/YAML)
    std::string engine_config_path{"/etc/wge/engine.json"};

    /// @brief 规则更新检查间隔 (秒)。0 表示不自动更新
    int32_t rule_update_interval_sec{60};

    /// @brief WGE 引擎初始化超时时间 (ms)
    int32_t engine_init_timeout_ms{30'000};

    /// @brief 是否启用严格模式（规则加载失败则启动失败）
    bool strict_mode{false};
};

// ============================================================================
// Mapping 配置
// ============================================================================

/**
 * @brief 日志字段映射配置文件引用
 */
struct MappingConfig {
    /// @brief log_mapping.yaml 文件路径，定义如何将原始日志映射到 HttpAccessEvent
    std::string log_mapping_path{"config/log_mapping.yaml"};
};

// ============================================================================
// Detector 运行时配置
// ============================================================================

/**
 * @brief Detector 服务运行时配置
 */
struct DetectorConfig {
    /// @brief Kafka poll 间隔 (ms)
    int32_t poll_interval_ms{100};

    /// @brief 工作线程数。0 表示自动检测 (std::thread::hardware_concurrency)
    int32_t worker_threads{0};

    /// @brief 每次批量处理的消-息数
    int32_t batch_size{64};

    /// @brief 最大待处理任务数（背压控制）
    int32_t max_pending_tasks{1024};

    /// @brief 单个检测任务超时时间 (ms)
    int32_t task_timeout_ms{5'000};

    /// @brief 健康检查 HTTP 端口
    int32_t health_check_port{8080};

    /// @brief 优雅关闭超时 (ms)
    int32_t graceful_shutdown_timeout_ms{30'000};

    /// @brief 是否启用死信队列
    bool enable_dlq{true};

    /// @brief WAL (Write-Ahead Log) 目录
    std::string wal_dir{"/var/lib/wge-detector/wal"};

    /// @brief WAL 段文件最大大小 (字节)
    int64_t wal_segment_max_size{256 * 1024 * 1024};  // 256 MB
};

// ============================================================================
// 可观测性配置
// ============================================================================

/**
 * @brief 可观测性配置 (Prometheus + OpenTelemetry)
 */
struct ObservabilityConfig {
    /// @brief 是否启用 Prometheus metrics 暴露
    bool prometheus_enabled{true};

    /// @brief Prometheus HTTP 端口
    int32_t prometheus_port{9090};

    /// @brief Prometheus metrics 路径
    std::string prometheus_path{"/metrics"};

    /// @brief 是否启用 OpenTelemetry 追踪
    bool otel_enabled{false};

    /// @brief OpenTelemetry Collector 端点
    std::string otel_endpoint{"http://localhost:4317"};

    /// @brief 日志级别: "trace" | "debug" | "info" | "warn" | "error" | "critical" | "off"
    std::string log_level{"info"};

    /// @brief 日志输出格式: "json" | "text"
    std::string log_format{"json"};

    /// @brief 日志文件路径。空表示仅输出到 stdout
    std::string log_file_path{};
};

// ============================================================================
// 顶层应用配置
// ============================================================================

/**
 * @brief 应用顶层配置，聚合所有子系统配置
 *
 * @note 本结构体使用聚合初始化，所有子结构体均已提供默认值。
 *       可直接 `AppConfig cfg;` 获得全默认配置。
 *       读取操作线程安全，修改需外部同步。
 *
 * 使用示例:
 * @code
 *   auto result = ConfigLoader::loadFromFile("config/app.yaml");
 *   if (!result) { spdlog::error("Config error: {}", result.error()); return 1; }
 *   AppConfig cfg = std::move(*result);
 * @endcode
 */
struct AppConfig {
    KafkaConfig kafka{};
    WgeConfig wge{};
    MappingConfig mapping{};
    DetectorConfig detector{};
    ObservabilityConfig observability{};

    /// @brief 应用版本号 (从 CMake 注入或运行时覆盖)
    std::string app_version{"0.1.0"};

    /// @brief 应用实例名，用于 metrics label 和日志标识
    std::string instance_name{"wge-detector-01"};
};

}  // namespace wge::kafka::config
