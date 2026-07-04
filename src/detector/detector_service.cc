/**
 * @file detector_service.cc
 * @brief DetectorService 实现
 */

#include "detector/detector_service.h"

#include <chrono>
#include <stdexcept>

#include "config/config.h"
#include "detector/worker_pool.h"
#include "http_access.pb.h"
#include "kafka/consumer.h"
#include "kafka/dlq.h"
#include "kafka/producer.h"
#include "librdkafka/rdkafkacpp.h"
#include "mapper/mapper.h"
#include "metrics/metrics.h"
#include "spdlog/spdlog.h"

// Akto 预处理器 (条件包含，仅在启用 akto 预处理时需要)
#if __has_include("akto/akto_preprocessor.h")
#include "akto/akto_preprocessor.h"
#define WGE_HAS_AKTO_PREPROCESSOR 1
#else
#define WGE_HAS_AKTO_PREPROCESSOR 0
#endif

namespace wge::kafka::detector {

// ============================================================================
// 构造与析构
// ============================================================================

DetectorService::DetectorService(const config::AppConfig& config,
                                 KafkaConsumer& consumer,
                                 AlertProducer& producer,
                                 DeadLetterQueue& dlq,
                                 mapper::LogMapper& mapper,
                                 WgeWorkerPool& pool,
                                 metrics::Metrics& metrics)
    : config_(config)
    , consumer_(consumer)
    , producer_(producer)
    , dlq_(dlq)
    , mapper_(mapper)
    , pool_(pool)
    , metrics_(metrics) {

    // 根据配置初始化预处理器
    if (config_.mapping.preprocessor == "akto") {
#if WGE_HAS_AKTO_PREPROCESSOR
        akto_preprocessor_ = std::make_unique<adapter::AktoPreprocessor>();
        SPDLOG_INFO("DetectorService: AktoPreprocessor enabled "
                    "(preprocessing Akto JSON logs before mapping)");
#else
        SPDLOG_WARN("DetectorService: 'akto' preprocessor requested but "
                    "akto_preprocessor.h not available, falling back to direct mapping");
#endif
    }

    SPDLOG_INFO("DetectorService created: instance={}, version={}, preprocessor={}",
                config_.instance_name, config_.app_version,
                config_.mapping.preprocessor.empty() ? "none" : config_.mapping.preprocessor.c_str());
}

DetectorService::~DetectorService() {
    try {
        stop();
    } catch (const std::exception& e) {
        SPDLOG_ERROR("DetectorService destructor error: {}", e.what());
    } catch (...) {
        SPDLOG_ERROR("DetectorService destructor: unknown error");
    }
}

// ============================================================================
// start
// ============================================================================

void DetectorService::start() {
    if (running_.exchange(true, std::memory_order_acq_rel)) {
        throw std::runtime_error("DetectorService::start: already running");
    }

    stopped_.store(false, std::memory_order_release);

    SPDLOG_INFO("DetectorService::start: beginning startup sequence");

    // 1. 初始化事务 (若启用)
    try {
        producer_.initTransactions();
    } catch (const std::exception& e) {
        SPDLOG_ERROR("DetectorService::start: initTransactions failed: {}", e.what());
        running_.store(false, std::memory_order_release);
        throw;
    }

    // 2. 启动 AlertProducer flush 线程
    // 使用 lambda 每次调用时获取最新的 group_metadata，
    // 确保 rebalance 后仍能获取有效的 ConsumerGroupMetadata。
    producer_.flushLoop([this]() {
        return consumer_.groupMetadata();
    });
    SPDLOG_INFO("DetectorService::start: AlertProducer flush loop started");

    // 3. 启动 Worker 线程池
    pool_.start();
    SPDLOG_INFO("DetectorService::start: WgeWorkerPool started");

    // 4. 启动 Kafka Consumer poll 线程 (最后启动)
    consumer_.start([this](auto messages) {
        this->onConsumerBatch(std::move(messages));
    });
    SPDLOG_INFO("DetectorService::start: KafkaConsumer poll started");

    SPDLOG_INFO("DetectorService::start: all components started successfully");
}

// ============================================================================
// stop
// ============================================================================

void DetectorService::stop() {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel,
                                          std::memory_order_relaxed)) {
        return;  // 已停止
    }

    SPDLOG_INFO("DetectorService::stop: beginning graceful shutdown sequence");

    auto shutdown_start = std::chrono::steady_clock::now();
    auto timeout = std::chrono::milliseconds(
        config_.detector.graceful_shutdown_timeout_ms);
    auto deadline = shutdown_start + timeout;

    auto remaining_ms = [&]() -> int32_t {
        auto now = std::chrono::steady_clock::now();
        if (now >= deadline) return 0;
        return static_cast<int32_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - now).count());
    };

    // 1. 停止 Consumer (不再接收新消息)
    SPDLOG_INFO("DetectorService::stop: step 1/4 - stopping KafkaConsumer (remaining={}ms)",
                remaining_ms());
    try {
        consumer_.stop();
    } catch (const std::exception& e) {
        SPDLOG_ERROR("DetectorService::stop: KafkaConsumer::stop error: {}", e.what());
    }

    if (remaining_ms() == 0) {
        SPDLOG_WARN("DetectorService::stop: shutdown timeout exceeded after step 1, forcing stop");
    }

    // 2. 停止 Worker Pool (等待 in-flight 任务完成)
    SPDLOG_INFO("DetectorService::stop: step 2/4 - draining WgeWorkerPool (remaining={}ms)",
                remaining_ms());
    try {
        pool_.stop();
    } catch (const std::exception& e) {
        SPDLOG_ERROR("DetectorService::stop: WgeWorkerPool::stop error: {}", e.what());
    }

    if (remaining_ms() == 0) {
        SPDLOG_WARN("DetectorService::stop: shutdown timeout exceeded after step 2, forcing stop");
    }

    // 3. 关闭 AlertProducer (清空队列 + 最后事务提交)
    SPDLOG_INFO("DetectorService::stop: step 3/4 - closing AlertProducer (remaining={}ms)",
                remaining_ms());
    try {
        producer_.close();
    } catch (const std::exception& e) {
        SPDLOG_ERROR("DetectorService::stop: AlertProducer::close error: {}", e.what());
    }

    // 4. 关闭 DLQ
    SPDLOG_INFO("DetectorService::stop: step 4/4 - closing DeadLetterQueue (remaining={}ms)",
                remaining_ms());
    try {
        dlq_.close();
    } catch (const std::exception& e) {
        SPDLOG_ERROR("DetectorService::stop: DeadLetterQueue::close error: {}", e.what());
    }

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - shutdown_start)
                       .count();

    running_.store(false, std::memory_order_release);

    SPDLOG_INFO("DetectorService::stop: shutdown complete in {}ms", elapsed);
}

// ============================================================================
// onConsumerBatch — Consumer 批次回调
// ============================================================================

void DetectorService::onConsumerBatch(
    std::vector<std::unique_ptr<RdKafka::Message>> messages) {

    if (messages.empty()) {
        return;
    }

    SPDLOG_DEBUG("DetectorService::onConsumerBatch: received {} messages",
                 messages.size());

    std::vector<std::shared_ptr<HttpAccessEvent>> events;
    events.reserve(messages.size());

    int64_t parse_errors = 0;

    for (auto& msg : messages) {
        if (!msg) {
            continue;
        }

        // 检查消息错误
        if (msg->err() != RdKafka::ERR_NO_ERROR) {
            if (msg->err() == RdKafka::ERR__PARTITION_EOF) {
                continue;  // 分区 EOF，正常情况
            }
            SPDLOG_WARN("DetectorService: message error: {}",
                        RdKafka::err2str(msg->err()));
            continue;
        }

        // 提取 payload
        const char* payload = static_cast<const char*>(msg->payload());
        size_t payload_len = msg->len();

        if (!payload || payload_len == 0) {
            SPDLOG_WARN("DetectorService: empty message payload");
            metrics_.incrementEventsDropped();
            if (config_.detector.enable_dlq) {
                dlq_.sendRaw(*msg, "Empty payload");
            }
            continue;
        }

        std::string_view raw_payload(payload, payload_len);

        // ===== 预处理阶段 =====
        // 当启用 Akto 预处理器时，先将 Akto 原始 JSON 日志预处理为标准格式:
        //   1. 展开 JSON 字符串格式的 requestHeaders/responseHeaders
        //   2. 秒级时间戳 → 毫秒级
        //   3. HTTP 版本号 "HTTP/1.1" → "1.1"
        std::string preprocessed_payload;
        std::string_view map_input = raw_payload;

        if (akto_preprocessor_) {
            auto pp_result = akto_preprocessor_->preprocess(raw_payload);
            if (pp_result) {
                preprocessed_payload = std::move(*pp_result);
                map_input = preprocessed_payload;
            } else {
                // 预处理失败 → DLQ
                SPDLOG_WARN("DetectorService: akto preprocessor failed: {} (payload preview: {})",
                            pp_result.error(),
                            raw_payload.substr(0, std::min(raw_payload.size(), size_t(200))));
                ++parse_errors;
                metrics_.incrementEventsDropped();
                if (config_.detector.enable_dlq) {
                    dlq_.sendRaw(*msg, "Akto preprocessor error: " + pp_result.error());
                }
                continue;
            }
        }

        // 通过 LogMapper 映射为 HttpAccessEvent
        auto map_result = mapper_.map(map_input);

        if (!map_result) {
            // 映射失败 → DLQ
            SPDLOG_WARN("DetectorService: mapper failed: {} (payload preview: {})",
                        map_result.error(),
                        raw_payload.substr(0, std::min(raw_payload.size(), size_t(200))));

            ++parse_errors;
            metrics_.incrementEventsDropped();

            if (config_.detector.enable_dlq) {
                dlq_.sendRaw(*msg, "Mapper error: " + map_result.error());
            }
            continue;
        }

        events.push_back(std::move(*map_result));
    }

    // 更新消费计数
    metrics_.addEventsConsumed(messages.size());

    if (parse_errors > 0) {
        SPDLOG_WARN("DetectorService: {} messages failed to parse out of {}",
                    parse_errors, messages.size());
    }

    // 提交到 Worker Pool
    if (!events.empty()) {
        try {
            [[maybe_unused]] int64_t submitted = pool_.submitBatch(std::move(events));
            SPDLOG_DEBUG("DetectorService: submitted {} events to worker pool",
                         submitted);
        } catch (const std::exception& e) {
            SPDLOG_ERROR("DetectorService: submitBatch failed: {}", e.what());
            // 异常情况下 events 已被 move，无法恢复。
            // 记录错误并增加 dropped 计数，便于监控告警。
            metrics_.incrementEventsDropped();
            // 注意: events 已被 std::move，此处无法发送到 DLQ。
            // 生产环境应确保 submitBatch 不抛异常（内部应有容错）。
        }
    }
}

}  // namespace wge::kafka::detector
