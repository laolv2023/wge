/**
 * @file producer.cc
 * @brief AlertProducer 实现 — Kafka 告警生产者
 *
 * ## 模块职责
 * AlertProducer 负责将 WGE 检测生成的告警（WgeAlertEvent）发送到 Kafka。
 *
 * ## 核心设计
 * - **异步批处理**: 使用独立的 flush 线程从内存队列（std::deque）中批量取出告警，
 *   通过 librdkafka 的 produce() API 发送
 * - **事务支持 (Exactly-Once Semantics)**:
 *   - 若配置了 transactional.id，则启用 Kafka 事务
 *   - 每批次告警在 begin_transaction → produce → send_offsets_to_transaction (CTP) →
 *     commit_transaction 的事务边界内发送
 *   - 若 produce 中有任何错误则 abort_transaction，确保原子性
 * - **CTP (Consume-Transform-Produce) 模式**:
 *   send_offsets_to_transaction 将 consumer offset 纳入同一事务，
 *   保证 "消费 → 检测 → 发送告警 → 提交 offset" 的端到端 exactly-once
 * - **幂等性**: enable_idempotence=true 时设置 acks=all + max.in.flight=5，
 *   确保即使在非事务模式下也不会产生重复消息
 *
 * ## 线程模型
 * - 生产者线程（worker pool）: 调用 sendAlert() 将告警入队
 * - Flush 线程: 运行 flushLoopImpl()，从队列取批量消息并 produce
 * - 队列由 queue_mutex_ + queue_cv_ 保护
 */

#include "kafka/producer.h"

#include <chrono>
#include <sstream>
#include <stdexcept>

#include "librdkafka/rdkafkacpp.h"
#include "spdlog/spdlog.h"
#include "wge_alert.pb.h"

namespace wge::kafka {

// ============================================================================
// 辅助函数
// ============================================================================

namespace {

/**
 * @brief 设置 RdKafka Conf 字符串属性，失败时抛出异常
 * @param conf RdKafka 配置对象（非空）
 * @param key  配置项名称
 * @param value 配置项值
 * @throws std::runtime_error 若设置失败
 */
void setConfOrThrow(RdKafka::Conf* conf, const std::string& key,
                    const std::string& value) {
    std::string err_str;
    RdKafka::Conf::ConfResult result = conf->set(key, value, err_str);
    if (result != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error(
            "Failed to set producer config '" + key + "' = '" + value +
            "': " + err_str);
    }
}

/**
 * @brief 条件设置字符串属性（仅非空时才设置）
 * @note 用于 SASL 等可选配置，避免设置空值导致 rdkafka 报错
 */
void setConfIfNotEmpty(RdKafka::Conf* conf, const std::string& key,
                       const std::string& value) {
    if (!value.empty()) {
        setConfOrThrow(conf, key, value);
    }
}

/**
 * @brief 检查 RdKafka ErrorCode，非 NO_ERROR 则抛出异常
 * @param err     RdKafka 错误码
 * @param context 错误上下文描述（用于异常消息）
 */
void checkError(RdKafka::ErrorCode err, const std::string& context) {
    if (err != RdKafka::ERR_NO_ERROR) {
        throw std::runtime_error(context + ": " + RdKafka::err2str(err));
    }
}

}  // namespace

// ============================================================================
// AlertProducer 实现
// ============================================================================

AlertProducer::AlertProducer(const ProducerConfig& config)
    : config_(config)
    , conf_(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)) {  // 创建全局配置对象

    if (!conf_) {
        throw std::runtime_error("Failed to create RdKafka::Conf");
    }

    // ---- 基础配置 ----
    setConfOrThrow(conf_.get(), "bootstrap.servers",
                   config_.bootstrap_servers);
    setConfOrThrow(conf_.get(), "compression.type", config_.compression_type);
    // message.max.bytes: 10 MB，防止超大告警导致 produce 失败
    setConfOrThrow(conf_.get(), "message.max.bytes",
                   std::to_string(10 * 1024 * 1024));  // 10 MB
    setConfOrThrow(conf_.get(), "retries",
                   std::to_string(config_.retries));
    // linger.ms: 批量发送的延迟时间，提高吞吐量
    setConfOrThrow(conf_.get(), "linger.ms",
                   std::to_string(config_.linger_ms));

    // ---- 幂等性配置 ----
    // enable.idempotence=true 确保即使在重试场景下也不会产生重复消息
    if (config_.enable_idempotence) {
        setConfOrThrow(conf_.get(), "enable.idempotence", "true");
        // 幂等生产者要求 acks=all（等待所有 ISR 确认）
        setConfOrThrow(conf_.get(), "acks", "all");
        // 幂等生产者要求 max.in.flight <= 5（保证消息顺序）
        setConfOrThrow(conf_.get(), "max.in.flight.requests.per.connection", "5");
    }

    // ---- 事务 ID ----
    // transactional.id 非空时启用 Kafka 事务（Exactly-Once Semantics）
    if (!config_.transactional_id.empty()) {
        setConfOrThrow(conf_.get(), "transactional.id",
                       config_.transactional_id);
    }

    // ---- SASL 认证 ----
    applySaslConfig(conf_.get());

    // ---- 创建底层 RdKafka::Producer 实例 ----
    std::string err_str;
    producer_ = RdKafka::Producer::create(conf_.get(), err_str);
    if (!producer_) {
        throw std::runtime_error(
            "Failed to create RdKafka::Producer: " + err_str);
    }

    SPDLOG_INFO("AlertProducer created: brokers={}, topic={}, transactional_id={}, "
                "idempotent={}, compression={}",
                config_.bootstrap_servers, config_.topic,
                config_.transactional_id.empty() ? "(none)" : config_.transactional_id,
                config_.enable_idempotence, config_.compression_type);
}

AlertProducer::~AlertProducer() {
    try {
        close();
    } catch (const std::exception& e) {
        SPDLOG_ERROR("AlertProducer destructor error: {}", e.what());
    } catch (...) {
        SPDLOG_ERROR("AlertProducer destructor: unknown error");
    }
}

void AlertProducer::applySaslConfig(RdKafka::Conf* conf) const {
    setConfIfNotEmpty(conf, "security.protocol", config_.security_protocol);
    setConfIfNotEmpty(conf, "sasl.mechanism", config_.sasl_mechanism);
    setConfIfNotEmpty(conf, "sasl.username", config_.sasl_username);
    setConfIfNotEmpty(conf, "sasl.password", config_.sasl_password);
}

void AlertProducer::initTransactions() {
    if (!producer_) {
        throw std::runtime_error("AlertProducer::initTransactions: producer is null");
    }

    if (config_.transactional_id.empty()) {
        SPDLOG_WARN("AlertProducer::initTransactions: no transactional.id set, "
                    "skipping init_transactions (idempotent-only mode)");
        return;
    }

    RdKafka::ErrorCode err = producer_->init_transactions(30000);  // 30s timeout
    checkError(err, "init_transactions failed");

    SPDLOG_INFO("AlertProducer: transactions initialized (txn_id={})",
                config_.transactional_id);
}

void AlertProducer::sendAlert(std::shared_ptr<WgeAlertEvent> alert) {
    if (!alert) {
        SPDLOG_WARN("AlertProducer::sendAlert: null alert ignored");
        return;
    }

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        // 队列背压: 防止 broker 宕机时队列无限增长导致 OOM
        if (alert_queue_.size() >= config_.max_queue_size) {
            SPDLOG_ERROR("AlertProducer::sendAlert: queue full (size={}, max={}), "
                         "dropping alert. Broker may be unreachable.",
                         alert_queue_.size(), config_.max_queue_size);
            return;  // 丢弃并记录，避免 OOM
        }
        alert_queue_.push_back(std::move(alert));  // 入队，转移所有权
    }
    queue_cv_.notify_one();  // 通知 flush 线程有新消息
}

void AlertProducer::flushLoop(
    std::function<RdKafka::ConsumerGroupMetadata*()> group_metadata_provider) {
    if (running_.exchange(true, std::memory_order_acq_rel)) {
        throw std::runtime_error(
            "AlertProducer::flushLoop: flush thread already running");
    }

    stopped_.store(false, std::memory_order_release);
    flush_thread_ = std::thread(
        &AlertProducer::flushLoopImpl, this, std::move(group_metadata_provider));

    SPDLOG_INFO("AlertProducer flush loop started: batch_size={}, linger_ms={}",
                config_.batch_size, config_.linger_ms);
}

void AlertProducer::close() {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel,
                                          std::memory_order_relaxed)) {
        return;  // 已经关闭
    }

    SPDLOG_INFO("AlertProducer::close: signaling flush thread to stop");

    // 唤醒 flush 线程处理剩余消息
    queue_cv_.notify_all();

    if (flush_thread_.joinable()) {
        flush_thread_.join();
        SPDLOG_INFO("AlertProducer::close: flush thread joined");
    }

    if (producer_) {
        // 最后一次 flush，等待所有消息发送完毕
        RdKafka::ErrorCode flush_err = producer_->flush(30'000);  // 30s
        if (flush_err != RdKafka::ERR_NO_ERROR) {
            SPDLOG_ERROR("AlertProducer::close: final flush failed: {}",
                         RdKafka::err2str(flush_err));
        }
        delete producer_;
        producer_ = nullptr;
        SPDLOG_INFO("AlertProducer::close: producer closed");
    }

    running_.store(false, std::memory_order_release);
}

std::string AlertProducer::errorString(RdKafka::ErrorCode err) {
    return RdKafka::err2str(err);
}

// ============================================================================
// 内部实现
// ============================================================================

size_t AlertProducer::drainQueue(
    std::vector<std::shared_ptr<WgeAlertEvent>>& out) {
    std::lock_guard<std::mutex> lock(queue_mutex_);

    // 最多取 batch_size 条，防止单次 produce 过大
    size_t count = std::min(alert_queue_.size(),
                            static_cast<size_t>(config_.batch_size));
    out.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        out.push_back(std::move(alert_queue_.front()));
        alert_queue_.pop_front();
    }

    return count;
}

std::string AlertProducer::serializeAlert(const WgeAlertEvent& alert) const {
    std::string serialized;
    if (!alert.SerializeToString(&serialized)) {
        throw std::runtime_error(
            "Failed to serialize WgeAlertEvent (alert_id=" +
            alert.alert_id() + ")");
    }
    return serialized;
}

void AlertProducer::flushLoopImpl(
    std::function<RdKafka::ConsumerGroupMetadata*()> group_metadata_provider) {

    SPDLOG_INFO("AlertProducer flush loop started");

    // 主循环：定期从队列中取批量告警发送
    while (!stopped_.load(std::memory_order_acquire)) {
        std::vector<std::shared_ptr<WgeAlertEvent>> batch;
        size_t batch_size = 0;

        // ===== 等待消息到达或超时 (linger_ms) =====
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            // wait_for: 最多等待 linger_ms 毫秒
            // predicate: 队列非空 或 已请求停止
            queue_cv_.wait_for(lock,
                std::chrono::milliseconds(config_.linger_ms),
                [this] {
                    return !alert_queue_.empty() ||
                           stopped_.load(std::memory_order_acquire);
                });

            // 停止 + 空队列 = 退出循环
            if (stopped_.load(std::memory_order_acquire) &&
                alert_queue_.empty()) {
                break;
            }

            // 从队列取出批量消息（最多 batch_size 条）
            size_t count = std::min(alert_queue_.size(),
                                    static_cast<size_t>(config_.batch_size));
            batch.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                batch.push_back(std::move(alert_queue_.front()));
                alert_queue_.pop_front();
            }
            batch_size = count;
        }
        // 锁外：不持锁进行 Kafka I/O

        if (batch.empty()) {
            // 超时无消息，非阻塞 flush 确保待发送缓冲区被推送到 broker
            if (producer_) {
                producer_->flush(0);  // timeout=0: 非阻塞
            }
            continue;
        }

        // ===== 事务性发送 =====
        // 若配置了 transactional_id，则整个批次的 produce 在一个事务内
        bool has_transaction =
            !config_.transactional_id.empty() && producer_;

        try {
            if (has_transaction) {
                // 1. 开始事务
                RdKafka::ErrorCode begin_err =
                    producer_->begin_transaction();
                if (begin_err != RdKafka::ERR_NO_ERROR) {
                    SPDLOG_ERROR("begin_transaction failed: {}",
                                 RdKafka::err2str(begin_err));
                    producer_->abort_transaction(30'000);  // 清理事务状态
                    continue;  // 跳过此批次，下次重试
                }
            }

            // 2. Produce 批次中的所有告警
            int32_t produce_errors = 0;
            for (auto& alert : batch) {
                if (!alert) continue;

                std::string payload;
                try {
                    payload = serializeAlert(*alert);
                } catch (const std::exception& e) {
                    SPDLOG_ERROR("Alert serialization failed: {}", e.what());
                    produce_errors++;
                    continue;  // 序列化失败跳过此条，继续处理下一条
                }

                // 使用 alert_id 作为消息 key，保证同一告警有序且支持 compaction
                const std::string& key = alert->alert_id();

                // produce() 是异步的，消息先进入 librdkafka 内部缓冲区
                RdKafka::ErrorCode produce_err = producer_->produce(
                    config_.topic,                          // topic
                    RdKafka::Topic::PARTITION_UA,          // 任意分区（由 key hash 决定）
                    RdKafka::Producer::RK_MSG_COPY,        // 拷贝 payload（不要求外部保持缓冲区）
                    const_cast<char*>(payload.data()),      // payload 数据
                    payload.size(),                         // payload 大小
                    key.data(),                             // key 数据
                    key.size(),                             // key 大小
                    0,                                      // timestamp: 0=由 broker 设置
                    nullptr                                 // opaque: 无需回调
                );

                if (produce_err != RdKafka::ERR_NO_ERROR) {
                    SPDLOG_ERROR("produce failed for alert {}: {}",
                                 alert->alert_id(),
                                 RdKafka::err2str(produce_err));
                    produce_errors++;
                }
            }

            // 3. 提交事务（含 consumer offset — CTP 模式）
            if (has_transaction) {
                // produce 有错误时 abort 事务，确保原子性
                if (produce_errors > 0) {
                    SPDLOG_ERROR("AlertProducer: {} produce errors, aborting transaction",
                                 produce_errors);
                    producer_->abort_transaction(30'000);
                    continue;
                }

                // CTP: 每次事务前重新获取 group_metadata
                // （rebalance 后 metadata 可能失效，需要重新获取）
                RdKafka::ConsumerGroupMetadata* group_metadata =
                    group_metadata_provider ? group_metadata_provider() : nullptr;
                if (group_metadata) {
                    // send_offsets_to_transaction: 将 consumer offset 纳入事务
                    // 保证 "消费 → 检测 → 告警 → 提交 offset" 原子性
                    RdKafka::ErrorCode offset_err =
                        producer_->send_offsets_to_transaction(
                            group_metadata, 30'000);
                    if (offset_err != RdKafka::ERR_NO_ERROR) {
                        SPDLOG_ERROR(
                            "send_offsets_to_transaction failed: {}",
                            RdKafka::err2str(offset_err));
                        producer_->abort_transaction(30'000);
                        continue;
                    }
                }

                // 提交事务（两阶段提交的 commit 阶段）
                RdKafka::ErrorCode commit_err =
                    producer_->commit_transaction(30'000);
                if (commit_err != RdKafka::ERR_NO_ERROR) {
                    SPDLOG_ERROR("commit_transaction failed: {}",
                                 RdKafka::err2str(commit_err));
                    producer_->abort_transaction(30'000);
                    continue;
                }
            }

            SPDLOG_DEBUG("AlertProducer: batch sent: {} alerts, {} errors",
                         batch_size, produce_errors);

        } catch (const std::exception& e) {
            SPDLOG_ERROR("AlertProducer flush loop exception: {}", e.what());
            if (has_transaction) {
                producer_->abort_transaction(30'000);  // 异常时确保事务被清理
            }
        }
    }

    // ===== 处理停止后剩余的队列消息 =====
    // swap 到局部变量释放锁，避免持锁调用阻塞的 produce
    std::deque<std::shared_ptr<WgeAlertEvent>> remaining;
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (!alert_queue_.empty()) {
            SPDLOG_INFO("AlertProducer: draining {} remaining alerts on shutdown",
                        alert_queue_.size());
            remaining.swap(alert_queue_);
        }
    }

    if (!remaining.empty()) {
        bool has_transaction =
            !config_.transactional_id.empty() && producer_;

        try {
            if (has_transaction) {
                RdKafka::ErrorCode begin_err =
                    producer_->begin_transaction();
                if (begin_err != RdKafka::ERR_NO_ERROR) {
                    SPDLOG_ERROR(
                        "AlertProducer shutdown drain: begin_transaction failed: {}",
                        RdKafka::err2str(begin_err));
                    // 无法开始事务，跳过 drain
                    return;
                }
            }

            int32_t drain_errors = 0;
            for (auto& alert : remaining) {
                if (!alert) continue;
                std::string payload;
                try {
                    payload = serializeAlert(*alert);
                } catch (...) {
                    drain_errors++;
                    continue;
                }
                RdKafka::ErrorCode produce_err = producer_->produce(
                    config_.topic,
                    RdKafka::Topic::PARTITION_UA,
                    RdKafka::Producer::RK_MSG_COPY,
                    const_cast<char*>(payload.data()),
                    payload.size(),
                    alert->alert_id().data(),
                    alert->alert_id().size(),
                    0, nullptr);
                if (produce_err != RdKafka::ERR_NO_ERROR) {
                    SPDLOG_ERROR(
                        "AlertProducer shutdown drain: produce failed for {}: {}",
                        alert->alert_id(), RdKafka::err2str(produce_err));
                    drain_errors++;
                }
            }

            if (has_transaction) {
                if (drain_errors > 0) {
                    SPDLOG_ERROR(
                        "AlertProducer shutdown drain: {} produce errors, aborting",
                        drain_errors);
                    producer_->abort_transaction(30'000);
                } else {
                    // 每次事务前从 consumer 重新获取 group_metadata
                    RdKafka::ConsumerGroupMetadata* group_metadata =
                        group_metadata_provider ? group_metadata_provider() : nullptr;
                    if (group_metadata) {
                        RdKafka::ErrorCode offset_err =
                            producer_->send_offsets_to_transaction(
                                group_metadata, 30'000);
                        if (offset_err != RdKafka::ERR_NO_ERROR) {
                            SPDLOG_ERROR(
                                "AlertProducer shutdown drain: "
                                "send_offsets_to_transaction failed: {}",
                                RdKafka::err2str(offset_err));
                            producer_->abort_transaction(30'000);
                            return;
                        }
                    }

                    RdKafka::ErrorCode commit_err =
                        producer_->commit_transaction(30'000);
                    if (commit_err != RdKafka::ERR_NO_ERROR) {
                        SPDLOG_ERROR(
                            "AlertProducer shutdown drain: "
                            "commit_transaction failed: {}",
                            RdKafka::err2str(commit_err));
                        producer_->abort_transaction(30'000);
                    }
                }
            }
        } catch (const std::exception& e) {
            SPDLOG_ERROR("AlertProducer shutdown drain error: {}", e.what());
            if (has_transaction) {
                producer_->abort_transaction(30'000);
            }
        }
    }

    SPDLOG_INFO("AlertProducer flush loop exited");
}

}  // namespace wge::kafka
