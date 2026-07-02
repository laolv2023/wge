/**
 * @file akto_dlq.cc
 * @brief Akto Adapter 死信队列实现
 *
 * 将 Adapter 发送失败的告警写入独立 DLQ topic, 确保告警不丢失。
 */

#include "akto_dlq.h"

#include <chrono>
#include <sstream>
#include <stdexcept>

#include "librdkafka/rdkafkacpp.h"
#include "spdlog/spdlog.h"

namespace wge::akto {

namespace {

/// @brief 设置 RdKafka Conf 属性, 失败时抛出异常
void setConfOrThrow(RdKafka::Conf* conf, const std::string& key,
                    const std::string& value) {
    std::string err_str;
    if (conf->set(key, value, err_str) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error(
            "AktoDlq: failed to set config '" + key + "' = '" + value +
            "': " + err_str);
    }
}

/// @brief 条件设置属性 (仅非空时)
void setConfIfNotEmpty(RdKafka::Conf* conf, const std::string& key,
                       const std::string& value) {
    if (!value.empty()) {
        setConfOrThrow(conf, key, value);
    }
}

}  // namespace

// ============================================================================
// 构造 / 析构
// ============================================================================

AktoDlq::AktoDlq(const std::string& bootstrap_servers,
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
        throw std::runtime_error("AktoDlq: failed to create RdKafka::Conf");
    }

    // 非事务性 Producer, ack=all 保证可靠性
    setConfOrThrow(conf_.get(), "bootstrap.servers", bootstrap_servers);
    setConfOrThrow(conf_.get(), "compression.type", "lz4");
    setConfOrThrow(conf_.get(), "acks", "all");
    setConfOrThrow(conf_.get(), "enable.idempotence", "false");
    setConfOrThrow(conf_.get(), "retries", "3");
    setConfOrThrow(conf_.get(), "linger.ms", "5");

    applySaslConfig(conf_.get());

    std::string err_str;
    producer_ = RdKafka::Producer::create(conf_.get(), err_str);
    if (!producer_) {
        throw std::runtime_error(
            "AktoDlq: failed to create RdKafka::Producer: " + err_str);
    }

    SPDLOG_INFO("AktoDlq created: brokers={}, topic={}", bootstrap_servers, dlq_topic_);
}

AktoDlq::~AktoDlq() {
    try {
        close();
    } catch (const std::exception& e) {
        SPDLOG_ERROR("AktoDlq destructor error: {}", e.what());
    } catch (...) {
        SPDLOG_ERROR("AktoDlq destructor: unknown error");
    }
}

void AktoDlq::applySaslConfig(RdKafka::Conf* conf) const {
    setConfIfNotEmpty(conf, "security.protocol", security_protocol_);
    setConfIfNotEmpty(conf, "sasl.mechanism", sasl_mechanism_);
    setConfIfNotEmpty(conf, "sasl.username", sasl_username_);
    setConfIfNotEmpty(conf, "sasl.password", sasl_password_);
}

// ============================================================================
// send / close
// ============================================================================

bool AktoDlq::send(const std::string& alert_json, const std::string& error) {
    std::lock_guard<std::mutex> lock(send_mutex_);
    if (closed_ || !producer_) {
        SPDLOG_ERROR("AktoDlq::send: producer is null or closed");
        return false;
    }

    // 构造 DLQ 消息体: JSON 格式, 包含原始告警 + 错误信息 + 时间戳
    std::ostringstream oss;
    oss << "{\"timestamp_ms\":" 
        << std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count()
        << ",\"error\":\"" << error << "\""
        << ",\"original_alert\":" << alert_json << "}";

    std::string payload = oss.str();

    RdKafka::ErrorCode err = producer_->produce(
        dlq_topic_,
        RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(payload.data()),
        payload.size(),
        nullptr,  // 无 key
        0,
        0,       // timestamp: 0 = broker 设置
        nullptr  // 无 opaque
    );

    if (err != RdKafka::ERR_NO_ERROR) {
        SPDLOG_ERROR("AktoDlq::send: produce failed: {}", RdKafka::err2str(err));
        return false;
    }

    SPDLOG_DEBUG("AktoDlq: event sent to {}", dlq_topic_);
    return true;
}

void AktoDlq::close() {
    RdKafka::Producer* producer_to_close = nullptr;
    {
        std::lock_guard<std::mutex> lock(send_mutex_);
        if (closed_) return;
        closed_ = true;
        producer_to_close = producer_;
        producer_ = nullptr;
    }

    if (producer_to_close) {
        RdKafka::ErrorCode err = producer_to_close->flush(10'000);
        if (err != RdKafka::ERR_NO_ERROR) {
            SPDLOG_WARN("AktoDlq::close: flush incomplete: {}", RdKafka::err2str(err));
        }
        delete producer_to_close;
        SPDLOG_INFO("AktoDlq closed");
    }
}

}  // namespace wge::akto
