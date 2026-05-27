/**
 * @file producer.cc
 * @brief AlertProducer 实现
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
 * @brief 设置 RdKafka Conf 字符串属性
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
 * @brief 条件设置字符串属性（非空才设置）
 */
void setConfIfNotEmpty(RdKafka::Conf* conf, const std::string& key,
                       const std::string& value) {
    if (!value.empty()) {
        setConfOrThrow(conf, key, value);
    }
}

/**
 * @brief 检查 RdKafka ErrorCode，非 NO_ERROR 则抛异常
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
    , conf_(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)) {

    if (!conf_) {
        throw std::runtime_error("Failed to create RdKafka::Conf");
    }

    // ---- 基础配置 ----
    setConfOrThrow(conf_.get(), "bootstrap.servers",
                   config_.bootstrap_servers);
    setConfOrThrow(conf_.get(), "compression.type", config_.compression_type);
    setConfOrThrow(conf_.get(), "message.max.bytes",
                   std::to_string(10 * 1024 * 1024));  // 10 MB
    setConfOrThrow(conf_.get(), "retries",
                   std::to_string(config_.retries));
    setConfOrThrow(conf_.get(), "linger.ms",
                   std::to_string(config_.linger_ms));

    // ---- 幂等性 ----
    if (config_.enable_idempotence) {
        setConfOrThrow(conf_.get(), "enable.idempotence", "true");
        // 幂等要求 acks=all
        setConfOrThrow(conf_.get(), "acks", "all");
        // 幂等要求 max.in.flight <= 5
        setConfOrThrow(conf_.get(), "max.in.flight.requests.per.connection", "5");
    }

    // ---- 事务 ID ----
    if (!config_.transactional_id.empty()) {
        setConfOrThrow(conf_.get(), "transactional.id",
                       config_.transactional_id);
    }

    // ---- SASL ----
    applySaslConfig(conf_.get());

    // ---- 创建 Producer ----
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
        alert_queue_.push_back(std::move(alert));
    }
    queue_cv_.notify_one();
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

    while (!stopped_.load(std::memory_order_acquire)) {
        std::vector<std::shared_ptr<WgeAlertEvent>> batch;
        size_t batch_size = 0;

        // 等待消息到达或超时
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait_for(lock,
                std::chrono::milliseconds(config_.linger_ms),
                [this] {
                    return !alert_queue_.empty() ||
                           stopped_.load(std::memory_order_acquire);
                });

            // 检查是否应该停止且队列为空
            if (stopped_.load(std::memory_order_acquire) &&
                alert_queue_.empty()) {
                break;
            }

            // 从队列取出消息
            size_t count = std::min(alert_queue_.size(),
                                    static_cast<size_t>(config_.batch_size));
            batch.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                batch.push_back(std::move(alert_queue_.front()));
                alert_queue_.pop_front();
            }
            batch_size = count;
        }

        if (batch.empty()) {
            // 超时无消息，可能需 flush 待发送缓冲区
            if (producer_) {
                producer_->flush(0);  // 非阻塞 flush
            }
            continue;
        }

        // ---- 事务性发送 ----
        bool has_transaction =
            !config_.transactional_id.empty() && producer_;

        try {
            if (has_transaction) {
                RdKafka::ErrorCode begin_err =
                    producer_->begin_transaction();
                if (begin_err != RdKafka::ERR_NO_ERROR) {
                    SPDLOG_ERROR("begin_transaction failed: {}",
                                 RdKafka::err2str(begin_err));
                    // 重启事务或跳过此批次
                    producer_->abort_transaction(5000);
                    continue;
                }
            }

            // Produce 所有消息
            int32_t produce_errors = 0;
            for (auto& alert : batch) {
                if (!alert) continue;

                std::string payload;
                try {
                    payload = serializeAlert(*alert);
                } catch (const std::exception& e) {
                    SPDLOG_ERROR("Alert serialization failed: {}", e.what());
                    produce_errors++;
                    continue;
                }

                // 使用 alert_id 作为 key 以保证同一告警的有序性
                const std::string& key = alert->alert_id();

                RdKafka::ErrorCode produce_err = producer_->produce(
                    config_.topic,                          // topic
                    RdKafka::Topic::PARTITION_UA,          // 任意分区
                    RdKafka::Producer::RK_MSG_COPY,        // 拷贝 payload
                    const_cast<char*>(payload.data()),      // payload
                    payload.size(),                         // payload size
                    key.data(),                             // key
                    key.size(),                             // key size
                    0,                                      // timestamp (now)
                    nullptr                                 // opaque
                );

                if (produce_err != RdKafka::ERR_NO_ERROR) {
                    SPDLOG_ERROR("produce failed for alert {}: {}",
                                 alert->alert_id(),
                                 RdKafka::err2str(produce_err));
                    produce_errors++;
                }
            }

            // 提交事务 (含 offset)
            if (has_transaction) {
                // 先检查 produce 是否有错误，有则 abort
                if (produce_errors > 0) {
                    SPDLOG_ERROR("AlertProducer: {} produce errors, aborting transaction",
                                 produce_errors);
                    producer_->abort_transaction(30'000);
                    continue;
                }

                // 每次事务前从 consumer 重新获取 group_metadata（rebalance 后不失效）
                RdKafka::ConsumerGroupMetadata* group_metadata =
                    group_metadata_provider ? group_metadata_provider() : nullptr;
                if (group_metadata) {
                    // CTP: 将 consumer offset 也纳入事务
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

                RdKafka::ErrorCode commit_err =
                    producer_->commit_transaction(30'000);
                if (commit_err != RdKafka::ERR_NO_ERROR) {
                    SPDLOG_ERROR("commit_transaction failed: {}",
                                 RdKafka::err2str(commit_err));
                    producer_->abort_transaction(5'000);
                    continue;
                }
            }

            SPDLOG_DEBUG("AlertProducer: batch sent: {} alerts, {} errors",
                         batch_size, produce_errors);

        } catch (const std::exception& e) {
            SPDLOG_ERROR("AlertProducer flush loop exception: {}", e.what());
            if (has_transaction) {
                producer_->abort_transaction(5'000);
            }
        }
    }

    // ---- 处理停止信号到达后剩余的队列消息 ----
    // 先 swap 到局部变量，释放锁后再逐条 produce，避免持锁调用阻塞操作
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
                        producer_->abort_transaction(5'000);
                    }
                }
            }
        } catch (const std::exception& e) {
            SPDLOG_ERROR("AlertProducer shutdown drain error: {}", e.what());
            if (has_transaction) {
                producer_->abort_transaction(5'000);
            }
        }
    }

    SPDLOG_INFO("AlertProducer flush loop exited");
}

}  // namespace wge::kafka
