/**
 * @file main.cc
 * @brief WGE-Kafka Detector — 程序入口
 *
 * WGE-Kafka Detector 是连接 Kafka 和 WGE 安全检测引擎的桥梁服务。
 * 它从 Kafka 消费 HTTP 访问日志，通过 WGE 引擎进行安全检测，
 * 将检测结果 (告警) 发送回 Kafka。
 *
 * 命令行参数:
 *   --config <path>   (必需) YAML 配置文件路径
 *   --version         打印版本信息并退出
 *
 * 信号处理:
 *   SIGTERM / SIGINT  优雅停止
 *   SIGHUP            热重载配置
 *
 * 组件初始化顺序:
 *   1. 加载配置
 *   2. 初始化日志 (spdlog)
 *   3. 初始化 WGE Engine
 *   4. 初始化 LogMapper
 *   5. 初始化 KafkaConsumer
 *   6. 初始化 AlertProducer + DeadLetterQueue
 *   7. 初始化 WgeWorkerPool
 *   8. 初始化 DetectorService
 *   9. 启动服务 + 进入事件循环
 */

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "config/config.h"
#include "config/config_loader.h"
#include "detector/detector_service.h"
#include "detector/worker_pool.h"
#include "kafka/consumer.h"
#include "kafka/dlq.h"
#include "kafka/producer.h"
#include "mapper/mapper.h"
#include "mapper/mapper_config.h"
#include "metrics/metrics.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

// WGE SDK
#include "wge/engine.h"

// ============================================================================
// 编译时版本号 (由 CMake 注入)
// ============================================================================

#ifndef WGE_DETECTOR_VERSION
#define WGE_DETECTOR_VERSION "0.1.0"
#endif

// ============================================================================
// 全局变量 — 用于信号处理
// ============================================================================

namespace {

/// @brief 全局停止标志，由信号处理器设置
std::atomic<bool> g_shutdown_requested{false};

/// @brief 全局重载配置标志
std::atomic<bool> g_reload_requested{false};

}  // namespace

// ============================================================================
// 信号处理器
// ============================================================================

/**
 * @brief 统一信号处理器
 *
 * 使用 sigaction 注册，SA_RESTART 标志确保被信号中断的系统调用自动重启。
 * 原子变量保证 signal-unsafe 上下文中的安全写入。
 *
 * @param signum 信号编号 (SIGTERM/SIGINT/SIGHUP)
 */
void signalHandler(int signum) {
    switch (signum) {
        case SIGTERM:
        case SIGINT:
            // 优雅停止：设置原子标志，主循环检测到后执行 shutdown 流程
            g_shutdown_requested.store(true, std::memory_order_release);
            break;

        case SIGHUP:
            // 热重载：设置重载标志，主循环检测到后重新加载配置
            g_reload_requested.store(true, std::memory_order_release);
            break;

        default:
            break;
    }
}

/**
 * @brief 注册信号处理器
 *
 * 注册 SIGTERM/SIGINT（优雅停止）、SIGHUP（配置重载），
 * 并显式忽略 SIGPIPE（防止写已关闭的 socket 导致进程退出）。
 *
 * @return true 所有关键信号处理器注册成功
 * @return false SIGTERM 或 SIGINT 处理器注册失败
 */
bool installSignalHandlers() {
    struct sigaction sa;
    std::memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signalHandler;
    sa.sa_flags = SA_RESTART;  // 被信号中断的系统调用自动重启
    sigemptyset(&sa.sa_mask);  // 处理信号期间不阻塞其他信号

    if (sigaction(SIGTERM, &sa, nullptr) != 0) {
        std::cerr << "Failed to install SIGTERM handler: "
                  << std::strerror(errno) << std::endl;
        return false;
    }

    if (sigaction(SIGINT, &sa, nullptr) != 0) {
        std::cerr << "Failed to install SIGINT handler: "
                  << std::strerror(errno) << std::endl;
        return false;
    }

    // SIGHUP 注册失败仅警告，不影响程序启动
    struct sigaction sa_hup;
    std::memset(&sa_hup, 0, sizeof(sa_hup));
    sa_hup.sa_handler = signalHandler;
    sa_hup.sa_flags = SA_RESTART;
    sigemptyset(&sa_hup.sa_mask);

    if (sigaction(SIGHUP, &sa_hup, nullptr) != 0) {
        SPDLOG_WARN("Failed to install SIGHUP handler: {} "
                    "(config reload disabled)",
                    std::strerror(errno));
    }

    // 显式忽略 SIGPIPE：防止写已关闭的 socket/fifo 时进程被 kill
    struct sigaction sa_pipe;
    std::memset(&sa_pipe, 0, sizeof(sa_pipe));
    sa_pipe.sa_handler = SIG_IGN;
    sigemptyset(&sa_pipe.sa_mask);
    sigaction(SIGPIPE, &sa_pipe, nullptr);

    return true;
}

// ============================================================================
// 日志初始化
// ============================================================================

void initLogging(const wge::kafka::config::AppConfig& config) {
    auto level = spdlog::level::from_str(config.observability.log_level);
    spdlog::set_level(level);

    auto console_sink =
        std::make_shared<spdlog::sinks::stdout_color_sink_mt>();

    if (config.observability.log_format == "json") {
        spdlog::set_pattern(
            R"({"time":"%Y-%m-%dT%H:%M:%S.%e%z","level":"%l",)"
            R"("logger":"%n","msg":"%v","thread":%t})");
    } else {
        spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%t] %v");
    }

    auto logger =
        std::make_shared<spdlog::logger>("wge-detector", console_sink);
    logger->set_level(level);
    spdlog::set_default_logger(logger);

    SPDLOG_INFO("Logging initialized: level={}, format={}",
                config.observability.log_level,
                config.observability.log_format);
}

// ============================================================================
// 版本信息与使用说明
// ============================================================================

void printVersion() {
    std::cout << "wge-kafka-detector version " << WGE_DETECTOR_VERSION
              << "\n"
              << "  Build: " << __DATE__ << " " << __TIME__ << "\n"
              << "  C++ Standard: " << __cplusplus << "\n";
}

void printUsage(const char* prog_name) {
    std::cerr << "Usage: " << prog_name << " [OPTIONS]\n\n"
              << "Options:\n"
              << "  --config <path>   YAML config file (required)\n"
              << "  --version         Print version and exit\n"
              << "  --help            Print this help\n\n"
              << "Signals:\n"
              << "  SIGTERM / SIGINT  Graceful shutdown\n"
              << "  SIGHUP            Reload configuration\n\n"
              << "Example:\n"
              << "  " << prog_name
              << " --config /etc/wge-detector/config.yaml\n";
}

// ============================================================================
// 命令行参数解析
// ============================================================================

std::expected<std::string, int> parseArguments(int argc, char* argv[]) {
    std::string config_path;

    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);

        if (arg == "--help" || arg == "-h") {
            printUsage(argv[0]);
            return std::unexpected(0);
        }

        if (arg == "--version" || arg == "-v") {
            printVersion();
            return std::unexpected(0);
        }

        if (arg == "--config") {
            if (i + 1 < argc) {
                config_path = argv[++i];
            } else {
                std::cerr << "Error: --config requires a path argument\n";
                printUsage(argv[0]);
                return std::unexpected(1);
            }
        } else if (arg.starts_with("--config=")) {
            config_path = arg.substr(9);
        }
    }

    if (config_path.empty()) {
        std::cerr << "Error: --config is required\n";
        printUsage(argv[0]);
        return std::unexpected(1);
    }

    return config_path;
}

// ============================================================================
// Config 桥接: config:: 类型 → kafka:: 类型
// ============================================================================

/**
 * @brief 将 config::ConsumerConfig 桥接到 kafka::ConsumerConfig
 *
 * 两个命名空间各自定义了 ConsumerConfig 结构体（config:: 是 YAML 解析层，
 * kafka:: 是 Kafka 客户端层），此函数进行字段映射和默认值补全。
 *
 * @param cfg YAML 解析得到的配置
 * @param poll_interval_ms 从 detector config 传入的 poll 间隔
 * @return kafka::ConsumerConfig 桥接后的 Kafka 客户端配置
 */
wge::kafka::ConsumerConfig bridgeConsumerConfig(
    const wge::kafka::config::ConsumerConfig& cfg,
    int32_t poll_interval_ms) {
    wge::kafka::ConsumerConfig out;
    out.bootstrap_servers = cfg.bootstrap_servers;
    out.group_id = cfg.group_id;
    out.topic = cfg.topic;
    out.max_poll_records = cfg.max_poll_records;
    out.poll_interval_ms = poll_interval_ms;  // 从 detector 配置传入，而非 YAML
    out.session_timeout_ms = cfg.session_timeout_ms;
    out.enable_auto_commit = cfg.enable_auto_commit;
    return out;
}

/**
 * @brief 将 config::ProducerConfig 桥接到 kafka::ProducerConfig
 *
 * @param cfg YAML 解析得到的配置
 * @param batch_size_override 从 detector config 传入的批次大小（覆盖 YAML 设置）
 * @return kafka::ProducerConfig 桥接后的 Kafka 客户端配置
 */
wge::kafka::ProducerConfig bridgeProducerConfig(
    const wge::kafka::config::ProducerConfig& cfg,
    int32_t batch_size_override) {
    wge::kafka::ProducerConfig out;
    out.bootstrap_servers = cfg.bootstrap_servers;
    out.topic = cfg.topic;
    out.compression_type = cfg.compression_type;
    out.batch_size = batch_size_override;  // 从 detector 配置覆盖
    out.linger_ms = static_cast<int32_t>(cfg.linger_ms);  // double → int32
    out.retries = cfg.retries;
    out.enable_idempotence = true;  // 默认启用幂等性
    return out;
}

// ============================================================================
// 主函数
// ============================================================================

int main(int argc, char* argv[]) {
    using namespace wge::kafka;
    using namespace wge::kafka::config;
    using namespace wge::kafka::detector;
    using namespace wge::kafka::metrics;
    using namespace wge::kafka::mapper;

    // ---- 1. 解析命令行参数 ----
    auto arg_result = parseArguments(argc, argv);
    if (!arg_result) {
        return arg_result.error();
    }
    std::string config_path = *arg_result;

    // ---- 2. 加载配置 ----
    auto config_result = ConfigLoader::loadFromFile(config_path);
    if (!config_result) {
        std::cerr << "Failed to load configuration from '"
                  << config_path << "': " << config_result.error()
                  << std::endl;
        return 1;
    }
    AppConfig config = std::move(*config_result);

    // ---- 3. 初始化日志 ----
    initLogging(config);

    SPDLOG_INFO("wge-kafka-detector v{} starting", WGE_DETECTOR_VERSION);
    SPDLOG_INFO("Configuration loaded from: {}", config_path);

    // ---- 4. 安装信号处理器 ----
    if (!installSignalHandlers()) {
        SPDLOG_ERROR("Failed to install signal handlers");
        return 1;
    }

    // ---- 5. 初始化 WGE Engine ----
    // WGE 引擎是核心安全检测组件，需要加载规则文件和引擎配置
    SPDLOG_INFO("Initializing WGE Engine...");

    wge::Engine engine;
    try {
        // 加载所有 WGE 规则文件
        for (const auto& rule_file : config.wge.rule_files) {
            SPDLOG_INFO("Loading WGE rules from: {}", rule_file);
            engine.loadRules(rule_file);
        }
        // 初始化引擎（加载引擎配置，如变量定义、变换规则等）
        engine.init(config.wge.engine_config_path);
    } catch (const std::exception& e) {
        SPDLOG_ERROR("Failed to initialize WGE Engine: {}", e.what());
        // strict_mode: 严格模式下引擎初始化失败则退出
        // 非严格模式下继续运行（告警可能不准确）
        if (config.wge.strict_mode) {
            return 1;
        }
        SPDLOG_WARN("WGE Engine init failed but strict_mode=false, "
                    "continuing");
    }

    // ---- 6. 初始化 LogMapper ----
    SPDLOG_INFO("Initializing LogMapper...");
    auto mapper_cfg_result =
        MapperConfig::loadFromFile(config.mapping.log_mapping_path);
    if (!mapper_cfg_result) {
        SPDLOG_ERROR("Failed to load mapper config from '{}': {}",
                     config.mapping.log_mapping_path,
                     mapper_cfg_result.error());
        return 1;
    }
    std::unique_ptr<LogMapper> mapper_up;
    try {
        mapper_up = std::make_unique<LogMapper>(*mapper_cfg_result);
    } catch (const std::exception& e) {
        SPDLOG_ERROR("Failed to initialize LogMapper: {}", e.what());
        return 1;
    }
    LogMapper& mapper = *mapper_up;
    SPDLOG_INFO("LogMapper initialized: format={}",
                formatToString(mapper.config().format));

    // ---- 7. 初始化 KafkaConsumer ----
    SPDLOG_INFO("Initializing KafkaConsumer...");
    auto consumer_cfg = bridgeConsumerConfig(
        config.kafka.consumer, config.detector.poll_interval_ms);

    KafkaConsumer consumer(consumer_cfg);
    SPDLOG_INFO("KafkaConsumer initialized: brokers={}, group={}, topic={}",
                consumer_cfg.bootstrap_servers, consumer_cfg.group_id,
                consumer_cfg.topic);

    // ---- 8. 初始化 AlertProducer ----
    SPDLOG_INFO("Initializing AlertProducer...");
    auto producer_cfg = bridgeProducerConfig(
        config.kafka.producer, config.detector.batch_size);

    AlertProducer producer(producer_cfg);
    SPDLOG_INFO("AlertProducer initialized: brokers={}, topic={}",
                producer_cfg.bootstrap_servers, producer_cfg.topic);

    // ---- 9. 初始化 DeadLetterQueue ----
    SPDLOG_INFO("Initializing DeadLetterQueue...");
    DeadLetterQueue dlq(
        config.kafka.producer.bootstrap_servers,
        config.kafka.producer.dlq_topic);
    SPDLOG_INFO("DeadLetterQueue initialized: topic={}",
                config.kafka.producer.dlq_topic);

    // ---- 10. 初始化 WgeWorkerPool ----
    WorkerConfig worker_cfg;
    worker_cfg.worker_threads = config.detector.worker_threads;
    worker_cfg.max_pending_tasks = config.detector.max_pending_tasks;
    worker_cfg.task_timeout_ms = config.detector.task_timeout_ms;

    WgeWorkerPool pool(engine, producer, Metrics::instance(), worker_cfg);
    SPDLOG_INFO("WorkerPool config: threads={}, max_pending={}, "
                "timeout_ms={}",
                worker_cfg.worker_threads, worker_cfg.max_pending_tasks,
                worker_cfg.task_timeout_ms);

    // ---- 11. 初始化 DetectorService ----
    DetectorService detector_service(
        config, consumer, producer, dlq, mapper, pool,
        Metrics::instance());

    // ---- 12. 启动服务 ----
    SPDLOG_INFO("Starting DetectorService...");
    try {
        detector_service.start();
    } catch (const std::exception& e) {
        SPDLOG_CRITICAL("Failed to start DetectorService: {}", e.what());
        return 1;
    }

    SPDLOG_INFO("============================================");
    SPDLOG_INFO("  WGE-Kafka Detector is RUNNING");
    SPDLOG_INFO("  Config:      {}", config_path);
    SPDLOG_INFO("  Kafka Input:  {} / {}",
                config.kafka.consumer.topic,
                config.kafka.consumer.group_id);
    SPDLOG_INFO("  Kafka Output: {}", config.kafka.producer.topic);
    SPDLOG_INFO("  Workers:      {}",
                worker_cfg.worker_threads);
    SPDLOG_INFO("============================================");

    // ---- 13. 主事件循环 (等待信号) ----
    // 主线程在此循环中：1) 检查关闭/重载信号 2) 更新 metrics
    while (!g_shutdown_requested.load(std::memory_order_acquire)) {
        // ---- 检查配置重载请求 ----
        // exchange 原子地读取并清除标志，防止重复重载
        if (g_reload_requested.exchange(false, std::memory_order_acq_rel)) {
            SPDLOG_INFO("Received SIGHUP, processing configuration reload from: {}",
                        config_path);
            // reload: 从 base config 开始合并新文件
            auto reload_result = ConfigLoader::reload(config, config_path);
            if (reload_result) {
                config = std::move(*reload_result);
                SPDLOG_INFO("Configuration reloaded successfully");

                // 更新日志级别（配置重载后生效）
                auto level =
                    spdlog::level::from_str(config.observability.log_level);
                spdlog::set_level(level);
                auto logger = spdlog::get("wge-detector");
                if (logger) {
                    logger->set_level(level);
                }
            } else {
                SPDLOG_ERROR("Configuration reload failed: {}",
                             reload_result.error());
            }
        }

        // ---- 更新 consumer lag metrics ----
        // 供 Prometheus / 监控系统使用
        try {
            int64_t lag = consumer.consumerLag();
            if (lag >= 0) {
                Metrics::instance().consumer_lag.store(
                    lag, std::memory_order_relaxed);
            }
        } catch (...) {
            // 静默处理 lag 查询失败（非关键路径）
        }

        // 短暂休眠避免 busy-wait，同时确保对信号的响应延迟 ≤ 500ms
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    // ---- 14. 优雅停止 ----
    SPDLOG_INFO("Received shutdown signal, initiating graceful shutdown...");

    auto shutdown_start = std::chrono::steady_clock::now();

    // detector_service.stop() 触发级联停止：
    //   consumer.stop() → producer.close() → pool.stop() → wal 清理
    detector_service.stop();

    auto shutdown_elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - shutdown_start)
            .count();

    // 输出最终统计
    SPDLOG_INFO("============================================");
    SPDLOG_INFO("  WGE-Kafka Detector SHUTDOWN COMPLETE");
    SPDLOG_INFO("  Duration: {}ms", shutdown_elapsed);
    SPDLOG_INFO("  Events consumed:  {}",
                Metrics::instance().events_consumed.load());
    SPDLOG_INFO("  Events processed: {}",
                Metrics::instance().events_processed.load());
    SPDLOG_INFO("  Alerts produced:  {}",
                Metrics::instance().alerts_produced.load());
    SPDLOG_INFO("  Events dropped:   {}",
                Metrics::instance().events_dropped.load());
    SPDLOG_INFO("============================================");

    return 0;
}
