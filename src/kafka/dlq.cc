/**
 * @file dlq.cc
 * @brief DeadLetterQueue 实现
 */

#include "kafka/dlq.h"

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
 * @brief 设置 RdKafka Conf 属性
 */
void setConfOrThrow(RdKafka::Conf* conf, const std::string& key,
                    const std::string& value) {
    std::string err_str;
    RdKafka::Conf::ConfResult result = conf->set(key, value, err_str);
    if (result != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error(
            "Failed to set DLQ config '" + key + "' = '" + value +
            "': " + err_str);
    }
}

/**
 * @brief 条件设置属性
 */
void setConfIfNotEmpty(RdKafka::Conf* conf, const std::string& key,
                       const std::string& value) {
    if (!value.empty()) {
        setConfOrThrow(conf, key, value);
    }
}

}  // namespace

// ============================================================================
// DeadLetterQueue 实现
// ============================================================================

DeadLetterQueue::DeadLetterQueue(
    const std::string& bootstrap_servers,
    const std::string& dlq_topic,
    const std::string& security_protocol,
    const std::string& sasl_mechanism,
    const std::string& sasl_username,
    const std::string& sasl_password)
    : dlq_topic_(dlq_topic)
    , security_protocol_(security_protocol)
    , sasl_mechanism_(sasl_mechanism)
    , sasl_username_(sasl_username)
    , sasl_password_(sasl_password)
    , conf_(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)) {

    if (!conf_) {
        throw std::runtime_error("Failed to create RdKafka::Conf for DLQ");
    }

    // ---- DLQ Producer 配置 ----
    // 非事务性、ack=all 保证可靠性
    setConfOrThrow(conf_.get(), "bootstrap.servers", bootstrap_servers);
    setConfOrThrow(conf_.get(), "compression.type", "lz4");
    setConfOrThrow(conf_.get(), "acks", "all");
    setConfOrThrow(conf_.get(), "enable.idempotence", "false");
    setConfOrThrow(conf_.get(), "retries", "3");
    setConfOrThrow(conf_.get(), "linger.ms", "5");
    setConfOrThrow(conf_.get(), "message.send.max.retries", "3");
    setConfOrThrow(conf_.get(), "retry.backoff.ms", "100");

    // SASL
    applySaslConfig(conf_.get());

    // 创建 producer
    std::string err_str;
    producer_ = RdKafka::Producer::create(conf_.get(), err_str);
    if (!producer_) {
        throw std::runtime_error(
            "Failed to create DLQ RdKafka::Producer: " + err_str);
    }

    SPDLOG_INFO("DeadLetterQueue created: brokers={}, topic={}",
                bootstrap_servers, dlq_topic_);
}

DeadLetterQueue::~DeadLetterQueue() {
    try {
        close();
    } catch (const std::exception& e) {
        SPDLOG_ERROR("DeadLetterQueue destructor error: {}", e.what());
    } catch (...) {
        SPDLOG_ERROR("DeadLetterQueue destructor: unknown error");
    }
}

void DeadLetterQueue::applySaslConfig(RdKafka::Conf* conf) const {
    setConfIfNotEmpty(conf, "security.protocol", security_protocol_);
    setConfIfNotEmpty(conf, "sasl.mechanism", sasl_mechanism_);
    setConfIfNotEmpty(conf, "sasl.username", sasl_username_);
    setConfIfNotEmpty(conf, "sasl.password", sasl_password_);
}

bool DeadLetterQueue::send(const DeadLetterEvent& event) {
    std::lock_guard<std::mutex> lock(send_mutex_);
    if (closed_ || !producer_) {
        SPDLOG_ERROR("DeadLetterQueue::send: producer is null or closed");
        return false;
    }

    // 序列化
    std::string payload;
    if (!event.SerializeToString(&payload)) {
        SPDLOG_ERROR("DeadLetterQueue::send: failed to serialize DeadLetterEvent");
        return false;
    }

    // 使用 original_offset 转字符串作为 key
    std::string key = std::to_string(event.original_offset());

    RdKafka::ErrorCode err = producer_->produce(
        dlq_topic_,
        RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(payload.data()),
        payload.size(),
        key.data(),
        key.size(),
        0,
        nullptr);

    if (err != RdKafka::ERR_NO_ERROR) {
        SPDLOG_ERROR("DeadLetterQueue::send: produce failed: {}",
                     RdKafka::err2str(err));
        return false;
    }

    SPDLOG_DEBUG("DeadLetterQueue: event sent to {} (offset={})",
                 dlq_topic_, event.original_offset());
    return true;
}

bool DeadLetterQueue::sendRaw(const RdKafka::Message& msg,
                               const std::string& error) {
    std::lock_guard<std::mutex> lock(send_mutex_);
    if (closed_ || !producer_) {
        SPDLOG_ERROR("DeadLetterQueue::sendRaw: producer is null or closed");
        return false;
    }

    // 构造 DeadLetterEvent
    DeadLetterEvent event;

    // 原始 topic (从 msg 获取)
    if (msg.topic_name()) {
        event.set_original_topic(msg.topic_name());
    }
    event.set_original_partition(msg.partition());
    event.set_original_offset(msg.offset());

    // 原始 payload
    if (msg.payload()) {
        event.set_original_payload(
            msg.payload(), msg.len());
    }

    event.set_error_message(error);

    // 时间戳
    auto now = std::chrono::system_clock::now();
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    event.set_timestamp_ms(now_ms);

    return send(event);
}

void DeadLetterQueue::close() {
    // 先 swap 出 producer_，释放锁后再 flush，防止持锁阻塞 send() 调用者
    RdKafka::Producer* producer_to_close = nullptr;
    {
        std::lock_guard<std::mutex> lock(send_mutex_);
        if (closed_) {
            return;
        }
        closed_ = true;
        producer_to_close = producer_;
        producer_ = nullptr;
    }

    if (producer_to_close) {
        // Flush 等待所有消息确认 (10s 超时)
        RdKafka::ErrorCode err = producer_to_close->flush(10'000);
        if (err != RdKafka::ERR_NO_ERROR) {
            SPDLOG_WARN("DeadLetterQueue::close: flush incomplete: {}",
                        RdKafka::err2str(err));
        }

        delete producer_to_close;
        SPDLOG_INFO("DeadLetterQueue closed");
    }
}

}  // namespace wge::kafka
