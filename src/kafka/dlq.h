#pragma once

/**
 * @file dlq.h
 * @brief 死信队列 (Dead Letter Queue) 生产者
 *
 * DeadLetterQueue 使用独立的非事务性 RdKafka::Producer，
 * 将无法正常处理的消息（解析失败、检测超时等）发送到 DLQ topic。
 *
 * 线程安全: send/sendRaw 可从任意线程调用（RdKafka::Producer::produce 线程安全）。
 */

#include <memory>
#include <string>

// 前向声明
namespace RdKafka {
class Producer;
class Message;
}  // namespace RdKafka

namespace wge::kafka {

// 前向声明 protobuf 类型
class DeadLetterEvent;

// ============================================================================
// DeadLetterQueue
// ============================================================================

/**
 * @brief 死信队列生产者
 *
 * 将处理失败的消息（如 protobuf 解析错误、映射失败）发送到 DLQ topic，
 * 保留原始消息 payload 和错误信息，供后续排查和重放。
 *
 * 使用示例:
 * @code
 *   DeadLetterQueue dlq("kafka:9092", "http-access-dlq");
 *   dlq.send(event);
 *   // 或包装原始 RdKafka Message
 *   dlq.sendRaw(*msg, "Protobuf parse error: invalid field");
 *   dlq.close();
 * @endcode
 *
 * @note 线程安全: RdKafka::Producer::produce 内部线程安全。
 * @note 非事务性: DLQ 使用独立 producer，不参与事务。
 */
class DeadLetterQueue {
public:
    /**
     * @brief 构造函数
     *
     * 创建独立的非事务性 RdKafka::Producer，配置为:
     * - 压缩: lz4
     * - 确认: acks=all
     * - 幂等: 关闭
     *
     * @param bootstrap_servers Kafka broker 地址
     * @param dlq_topic DLQ topic 名称
     * @param security_protocol 安全协议 (可选)
     * @param sasl_mechanism SASL 机制 (可选)
     * @param sasl_username SASL 用户名 (可选)
     * @param sasl_password SASL 密码 (可选)
     * @throws std::runtime_error 若 producer 创建失败
     */
    DeadLetterQueue(const std::string& bootstrap_servers,
                    const std::string& dlq_topic,
                    const std::string& security_protocol = "",
                    const std::string& sasl_mechanism = "",
                    const std::string& sasl_username = "",
                    const std::string& sasl_password = "");

    /**
     * @brief 析构函数
     *
     * 自动 close()，异常内部捕获。
     */
    ~DeadLetterQueue();

    // 禁止拷贝和移动
    DeadLetterQueue(const DeadLetterQueue&) = delete;
    DeadLetterQueue& operator=(const DeadLetterQueue&) = delete;
    DeadLetterQueue(DeadLetterQueue&&) = delete;
    DeadLetterQueue& operator=(DeadLetterQueue&&) = delete;

    /**
     * @brief 发送 DeadLetterEvent 到 DLQ topic
     *
     * 序列化 protobuf 消息并通过 producer 发送。
     *
     * @param event 完整的 DeadLetterEvent
     * @return bool true 表示 produce 调用成功（异步，不保证 broker 确认）
     * @note 线程安全
     */
    bool send(const DeadLetterEvent& event);

    /**
     * @brief 包装原始消息后发送到 DLQ
     *
     * 从 RdKafka::Message 提取原始 topic/partition/offset/payload，
     * 构造 DeadLetterEvent 并发送。
     *
     * @param msg 原始 Kafka 消息
     * @param error 错误描述字符串
     * @return bool true 表示 produce 调用成功
     * @note 线程安全
     */
    bool sendRaw(const RdKafka::Message& msg, const std::string& error);

    /**
     * @brief 优雅关闭
     *
     * Flush 所有待发送消息并关闭 producer。
     *
     * @note 幂等: 多次调用安全。
     */
    void close();

    /**
     * @brief 获取 DLQ topic 名称
     */
    [[nodiscard]] const std::string& topic() const noexcept {
        return dlq_topic_;
    }

private:
    /**
     * @brief 设置 SASL 配置
     */
    void applySaslConfig(RdKafka::Conf* conf) const;

    /// @brief DLQ topic 名称
    std::string dlq_topic_;

    /// @brief SASL 配置
    std::string security_protocol_;
    std::string sasl_mechanism_;
    std::string sasl_username_;
    std::string sasl_password_;

    /// @brief RdKafka 配置对象
    std::unique_ptr<RdKafka::Conf> conf_;

    /// @brief RdKafka Producer 实例（非事务性）
    RdKafka::Producer* producer_{nullptr};

    /// @brief 关闭标志，保证幂等
    bool closed_{false};
};

}  // namespace wge::kafka
