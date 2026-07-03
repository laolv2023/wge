#pragma once

/**
 * @file akto_dlq.h
 * @brief Akto Adapter 死信队列 — 捕获 Kafka 发送失败的告警
 *
 * 设计报告第 9.6 节要求: "Adapter 捕获 Kafka 发送异常, 将失败告警写入死信队列"
 *
 * 当 AktoAdapter 向 akto.threat_detection.malicious_events topic 发送消息失败时,
 * 将原始告警 JSON 和错误信息写入独立的 DLQ topic (wge-akto-adapter-dlq),
 * 确保告警不丢失, 供后续排查和重放。
 *
 * 线程安全: 内部使用 mutex 保护 RdKafka::Producer::produce 调用。
 */

#include <memory>
#include <mutex>
#include <string>

namespace RdKafka {
class Producer;
class Conf;
}  // namespace RdKafka

namespace wge::akto {

/**
 * @brief Akto Adapter 死信队列
 *
 * 使用独立的非事务性 RdKafka::Producer, 与主告警 Producer 隔离,
 * 确保主 Producer 的事务不受 DLQ 写入失败影响。
 */
class AktoDlq {
public:
    /**
     * @brief 构造函数
     *
     * @param bootstrap_servers Kafka broker 地址
     * @param dlq_topic DLQ topic 名称 (如 "wge-akto-adapter-dlq")
     * @param security_protocol 安全协议 (可选)
     * @param sasl_mechanism SASL 机制 (可选)
     * @param sasl_username SASL 用户名 (可选)
     * @param sasl_password SASL 密码 (可选)
     * @throws std::runtime_error 若 Producer 创建失败
     */
    AktoDlq(const std::string& bootstrap_servers,
            const std::string& dlq_topic,
            const std::string& security_protocol = "",
            const std::string& sasl_mechanism = "",
            const std::string& sasl_username = "",
            const std::string& sasl_password = "");

    ~AktoDlq();

    // 禁止拷贝和移动
    AktoDlq(const AktoDlq&) = delete;
    AktoDlq& operator=(const AktoDlq&) = delete;
    AktoDlq(AktoDlq&&) = delete;
    AktoDlq& operator=(AktoDlq&&) = delete;

    /**
     * @brief 发送失败告警到 DLQ
     *
     * @param alert_json 原始告警 JSON 字符串 (WgeAlertEvent 序列化后的 JSON)
     * @param error 错误描述 (如 "Kafka produce failed: MSG_SIZE_TOO_LARGE")
     * @return true 表示 produce 调用成功 (异步, 不保证 broker 确认)
     */
    bool send(const std::string& alert_json, const std::string& error);

    /// @brief 优雅关闭: flush 所有待发送消息并释放 Producer
    void close();

private:
    void applySaslConfig(RdKafka::Conf* conf) const;

    std::string dlq_topic_;
    std::string security_protocol_;
    std::string sasl_mechanism_;
    std::string sasl_username_;
    std::string sasl_password_;

    std::unique_ptr<RdKafka::Conf> conf_;
    RdKafka::Producer* producer_{nullptr};
    bool closed_{false};
    mutable std::mutex send_mutex_;
};

}  // namespace wge::akto
