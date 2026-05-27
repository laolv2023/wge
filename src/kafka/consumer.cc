/**
 * @file consumer.cc
 * @brief KafkaConsumer 实现
 */

#include "kafka/consumer.h"

#include <chrono>
#include <sstream>
#include <stdexcept>

#include "librdkafka/rdkafkacpp.h"
#include "spdlog/spdlog.h"

namespace wge::kafka {

// ============================================================================
// 辅助: 配置设置宏和函数
// ============================================================================

namespace {

/**
 * @brief 设置 RdKafka Conf 字符串属性，失败时抛出异常
 *
 * @param conf RdKafka Conf 对象
 * @param key 属性键
 * @param value 属性值
 * @throws std::runtime_error 若设置失败
 */
void setConfOrThrow(RdKafka::Conf* conf, const std::string& key,
                    const std::string& value) {
    std::string err_str;
    RdKafka::Conf::ConfResult result =
        conf->set(key, value, err_str);
    if (result != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error(
            "Failed to set Kafka config '" + key + "' = '" + value +
            "': " + err_str);
    }
}

/**
 * @brief 设置 RdKafka Conf 整型属性，失败时抛出异常
 */
void setConfOrThrow(RdKafka::Conf* conf, const std::string& key,
                    int32_t value) {
    setConfOrThrow(conf, key, std::to_string(value));
}

/**
 * @brief 设置 RdKafka Conf 整型属性 (int64_t)
 */
void setConfOrThrow(RdKafka::Conf* conf, const std::string& key,
                    int64_t value) {
    setConfOrThrow(conf, key, std::to_string(value));
}

/**
 * @brief 条件性设置 RdKafka Conf 字符串属性
 *
 * 仅当 value 非空时设置。
 */
void setConfIfNotEmpty(RdKafka::Conf* conf, const std::string& key,
                       const std::string& value) {
    if (!value.empty()) {
        setConfOrThrow(conf, key, value);
    }
}

}  // namespace

// ============================================================================
// KafkaConsumer 实现
// ============================================================================

KafkaConsumer::KafkaConsumer(const ConsumerConfig& config)
    : config_(config)
    , global_conf_(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL))
    , topic_conf_(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC)) {

    if (!global_conf_ || !topic_conf_) {
        throw std::runtime_error("Failed to create RdKafka Conf objects");
    }

    // ---- 全局配置 ----
    setConfOrThrow(global_conf_.get(), "bootstrap.servers",
                   config_.bootstrap_servers);
    setConfOrThrow(global_conf_.get(), "group.id", config_.group_id);
    setConfOrThrow(global_conf_.get(), "session.timeout.ms",
                   config_.session_timeout_ms);
    setConfOrThrow(global_conf_.get(), "enable.auto.commit",
                   config_.enable_auto_commit ? "true" : "false");
    setConfOrThrow(global_conf_.get(), "max.poll.interval.ms",
                   std::to_string(config_.session_timeout_ms * 3));

    // 安全协议
    applySaslConfig(global_conf_.get());

    // 默认事件回调（错误日志）
    // 通过 set "log_level" 控制 rdkafka 日志级别
    setConfOrThrow(global_conf_.get(), "log_level", "6");  // LOG_INFO

    // ---- Topic 配置 ----
    setConfOrThrow(topic_conf_.get(), "auto.offset.reset", "latest");

    // ---- 创建 Consumer ----
    std::string err_str;
    consumer_ = RdKafka::KafkaConsumer::create(global_conf_.get(), err_str);
    if (!consumer_) {
        throw std::runtime_error("Failed to create KafkaConsumer: " + err_str);
    }

    // ---- 订阅 ----
    std::vector<std::string> topics = {config_.topic};
    RdKafka::ErrorCode err = consumer_->subscribe(topics);
    if (err != RdKafka::ERR_NO_ERROR) {
        std::string msg = "Failed to subscribe to topic '" +
                          config_.topic + "': " + errorString(err);
        delete consumer_;
        consumer_ = nullptr;
        throw std::runtime_error(msg);
    }

    SPDLOG_INFO("KafkaConsumer created: brokers={}, group={}, topic={}",
                config_.bootstrap_servers, config_.group_id, config_.topic);
}

KafkaConsumer::~KafkaConsumer() {
    try {
        stop();
    } catch (const std::exception& e) {
        SPDLOG_ERROR("KafkaConsumer destructor error: {}", e.what());
    } catch (...) {
        SPDLOG_ERROR("KafkaConsumer destructor: unknown error");
    }
}

void KafkaConsumer::applySaslConfig(RdKafka::Conf* conf) const {
    setConfIfNotEmpty(conf, "security.protocol", config_.security_protocol);
    setConfIfNotEmpty(conf, "sasl.mechanism", config_.sasl_mechanism);
    setConfIfNotEmpty(conf, "sasl.username", config_.sasl_username);
    setConfIfNotEmpty(conf, "sasl.password", config_.sasl_password);
}

void KafkaConsumer::start(
    std::function<void(std::vector<std::unique_ptr<RdKafka::Message>>)> on_batch) {

    if (running_.exchange(true, std::memory_order_acq_rel)) {
        throw std::runtime_error("KafkaConsumer::start: poll thread already running");
    }

    on_batch_ = std::move(on_batch);
    stopped_.store(false, std::memory_order_release);

    poll_thread_ = std::thread(&KafkaConsumer::pollLoop, this);

    SPDLOG_INFO("KafkaConsumer poll thread started: topic={}, max_poll_records={}, interval_ms={}",
                config_.topic, config_.max_poll_records, config_.poll_interval_ms);
}

void KafkaConsumer::stop() {
    // 幂等: 已停止则直接返回
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel,
                                          std::memory_order_relaxed)) {
        return;  // 已经在停止中
    }

    SPDLOG_INFO("KafkaConsumer::stop: signaling poll thread to stop");

    // 唤醒可能在 poll 中阻塞的线程
    {
        std::lock_guard<std::mutex> lock(stop_mutex_);
    }
    stop_cv_.notify_all();

    // 等待 poll 线程退出
    if (poll_thread_.joinable()) {
        poll_thread_.join();
        SPDLOG_INFO("KafkaConsumer::stop: poll thread joined");
    }

    // 关闭 consumer
    if (consumer_) {
        consumer_->unsubscribe();
        consumer_->close();
        delete consumer_;
        consumer_ = nullptr;
        SPDLOG_INFO("KafkaConsumer::stop: consumer closed");
    }

    running_.store(false, std::memory_order_release);
}

RdKafka::ConsumerGroupMetadata* KafkaConsumer::groupMetadata() const {
    if (!consumer_) {
        return nullptr;
    }
    return consumer_->groupMetadata();
}

int64_t KafkaConsumer::consumerLag() const {
    if (!consumer_) {
        return -1;
    }

    std::lock_guard<std::mutex> lock(query_mutex_);

    std::vector<RdKafka::TopicPartition*> partitions;
    RdKafka::ErrorCode err = consumer_->assignment(partitions);
    if (err != RdKafka::ERR_NO_ERROR) {
        SPDLOG_WARN("Failed to get assignment for lag query: {}",
                    errorString(err));
        return -1;
    }

    // 查询每个分区的高水位 offset
    int64_t total_lag = 0;
    for (auto* tp : partitions) {
        // committed offset
        int64_t committed = -1;
        {
            std::vector<RdKafka::TopicPartition*> tp_vec = {tp};
            RdKafka::ErrorCode committed_err =
                consumer_->committed(tp_vec, 5000);  // 5s timeout
            if (committed_err == RdKafka::ERR_NO_ERROR) {
                committed = tp_vec[0]->offset();
            }
        }

        // 高水位 (log end offset / watermark)
        int64_t low = 0;
        int64_t high = -1;
        RdKafka::ErrorCode watermark_err =
            consumer_->get_watermark_offsets(
                tp->topic(), tp->partition(), &low, &high, 5000);

        if (watermark_err == RdKafka::ERR_NO_ERROR && high >= 0) {
            if (committed >= 0) {
                total_lag += (high - committed);
            } else {
                total_lag += high;
            }
        }

        delete tp;
    }

    return total_lag;
}

int64_t KafkaConsumer::committedOffset() const {
    if (!consumer_) {
        return -1;
    }

    std::lock_guard<std::mutex> lock(query_mutex_);

    std::vector<RdKafka::TopicPartition*> partitions;
    RdKafka::ErrorCode err = consumer_->assignment(partitions);
    if (err != RdKafka::ERR_NO_ERROR) {
        return -1;
    }

    int64_t total_committed = 0;
    for (auto* tp : partitions) {
        std::vector<RdKafka::TopicPartition*> tp_vec = {tp};
        RdKafka::ErrorCode committed_err =
            consumer_->committed(tp_vec, 5000);
        if (committed_err == RdKafka::ERR_NO_ERROR) {
            total_committed += tp_vec[0]->offset();
        }
        delete tp;
    }

    return total_committed;
}

std::string KafkaConsumer::errorString(RdKafka::ErrorCode err) {
    return RdKafka::err2str(err);
}

// ============================================================================
// Poll 循环
// ============================================================================

void KafkaConsumer::pollLoop() {
    SPDLOG_INFO("KafkaConsumer poll loop started");

    std::vector<std::unique_ptr<RdKafka::Message>> batch;
    batch.reserve(static_cast<size_t>(config_.max_poll_records));

    while (!stopped_.load(std::memory_order_acquire)) {
        // 单次 poll
        std::unique_ptr<RdKafka::Message> msg(
            consumer_->consume(config_.poll_interval_ms));

        if (!msg) {
            continue;
        }

        switch (msg->err()) {
        case RdKafka::ERR_NO_ERROR:
            // 正常消息: 加入批次
            batch.push_back(std::move(msg));

            // 达到批次大小: 回调并清空
            if (static_cast<int32_t>(batch.size()) >= config_.max_poll_records) {
                try {
                    if (on_batch_) {
                        on_batch_(std::move(batch));
                    }
                } catch (const std::exception& e) {
                    SPDLOG_ERROR("KafkaConsumer on_batch exception: {}", e.what());
                }
                batch.clear();
                batch.reserve(static_cast<size_t>(config_.max_poll_records));
            }
            break;

        case RdKafka::ERR__TIMED_OUT:
        case RdKafka::ERR__PARTITION_EOF:
            // poll 超时或分区 EOF: 投递当前批次
            if (!batch.empty()) {
                try {
                    if (on_batch_) {
                        on_batch_(std::move(batch));
                    }
                } catch (const std::exception& e) {
                    SPDLOG_ERROR("KafkaConsumer on_batch exception: {}", e.what());
                }
                batch.clear();
                batch.reserve(static_cast<size_t>(config_.max_poll_records));
            }
            break;

        default:
            // 错误
            SPDLOG_ERROR("KafkaConsumer poll error: {} ({})",
                         msg->errstr(), static_cast<int>(msg->err()));

            // 对于可恢复错误，投递当前批次
            if (!batch.empty()) {
                try {
                    if (on_batch_) {
                        on_batch_(std::move(batch));
                    }
                } catch (const std::exception& e) {
                    SPDLOG_ERROR("KafkaConsumer on_batch exception: {}", e.what());
                }
                batch.clear();
                batch.reserve(static_cast<size_t>(config_.max_poll_records));
            }
            break;
        }
    }

    // 停止前投递剩余消息
    if (!batch.empty()) {
        try {
            if (on_batch_) {
                on_batch_(std::move(batch));
            }
        } catch (const std::exception& e) {
            SPDLOG_ERROR("KafkaConsumer on_batch exception on shutdown: {}",
                         e.what());
        }
    }

    SPDLOG_INFO("KafkaConsumer poll loop exited");
}

}  // namespace wge::kafka
