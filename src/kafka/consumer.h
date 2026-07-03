#pragma once

/**
 * @file consumer.h
 * @brief Kafka 消费者封装
 *
 * KafkaConsumer 封装 RdKafka::KafkaConsumer，提供批量 poll、
 * 优雅停止、consumer lag 查询和 group metadata 访问能力。
 *
 * 线程安全: 所有公有方法均为线程安全。内部 poll 循环运行于独立线程，
 *           通过 atomic flag + condition_variable 实现优雅关闭。
 */

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// 前向声明 RdKafka 类型
namespace RdKafka {
class KafkaConsumer;
class Message;
class TopicPartition;
class ConsumerGroupMetadata;
}  // namespace RdKafka

namespace wge::kafka {

// ============================================================================
// ConsumerConfig
// ============================================================================

/**
 * @brief Kafka Consumer 配置结构体
 *
 * 控制消费者连接、订阅和消费行为的参数集合。
 * 所有字段提供生产级默认值。
 *
 * @note 本结构体为 POD 风格，读取线程安全，修改需外部同步。
 */
struct ConsumerConfig {
    /// @brief Kafka broker 地址列表，逗号分隔
    std::string bootstrap_servers{"localhost:9092"};

    /// @brief Consumer group ID
    std::string group_id{"wge-kafka-detector"};

    /// @brief 要订阅的 topic
    std::string topic{};

    /// @brief 死信队列 topic 名称
    std::string dlq_topic{"http-access-dlq"};

    /// @brief 单次 poll 最大拉取消息数
    int32_t max_poll_records{500};

    /// @brief poll 间隔 (ms)
    int32_t poll_interval_ms{100};

    /// @brief 会话超时时间 (ms)
    int64_t session_timeout_ms{30'000};

    /// @brief 心跳间隔 (ms)，通常为 session_timeout 的 1/3
    int64_t heartbeat_interval_ms{3'000};

    /// @brief 两次 poll 之间最大间隔 (ms)，超时则消费者被踢出组
    int64_t max_poll_interval_ms{300'000};

    /// @brief auto.offset.reset: earliest | latest | none | error
    std::string auto_offset_reset{"latest"};

    /// @brief 分区分配策略: range | roundrobin | cooperative-sticky | ...
    std::string partition_assignment_strategy{"cooperative-sticky"};

    /// @brief 是否启用自动提交 offset
    bool enable_auto_commit{false};

    /// @brief 安全协议: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
    std::string security_protocol{};

    /// @brief SASL 机制: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI
    std::string sasl_mechanism{};

    /// @brief SASL 用户名
    std::string sasl_username{};

    /// @brief SASL 密码
    std::string sasl_password{};
};

// ============================================================================
// KafkaConsumer
// ============================================================================

/**
 * @brief Kafka 消费者
 *
 * 封装 RdKafka::KafkaConsumer，提供:
 * - 构造时自动订阅指定 topic
 * - 独立线程中的批量 poll 循环
 * - 通过回调函数投递消息批次
 * - 优雅停止和资源清理
 * - Consumer lag 和 committed offset 查询
 *
 * 使用示例:
 * @code
 *   ConsumerConfig cfg;
 *   cfg.bootstrap_servers = "kafka:9092";
 *   cfg.topic = "http-access";
 *   cfg.group_id = "my-group";
 *
 *   KafkaConsumer consumer(cfg);
 *   consumer.start([](auto messages) {
 *       // 处理消息批次
 *   });
 *   // ...
 *   consumer.stop();
 * @endcode
 *
 * @note 线程安全: start/stop 可被外部线程调用，内部使用 atomic + mutex 同步。
 * @note 不可拷贝或移动: 持有线程和 rdkafka 资源。
 */
class KafkaConsumer {
public:
    /**
     * @brief 构造函数
     *
     * 创建 RdKafka::KafkaConsumer 实例，设置所有配置项并订阅 topic。
     * 若任何步骤失败，抛出 std::runtime_error。
     *
     * @param config 消费者配置
     * @throws std::runtime_error 若 consumer 创建或订阅失败
     */
    explicit KafkaConsumer(const ConsumerConfig& config);

    /**
     * @brief 析构函数
     *
     * 若尚未调用 stop()，自动执行优雅停止。
     * 所有异常在析构函数内捕获并记录日志，不向外传播。
     */
    ~KafkaConsumer();

    // 禁止拷贝和移动
    KafkaConsumer(const KafkaConsumer&) = delete;
    KafkaConsumer& operator=(const KafkaConsumer&) = delete;
    KafkaConsumer(KafkaConsumer&&) = delete;
    KafkaConsumer& operator=(KafkaConsumer&&) = delete;

    /**
     * @brief 启动消费循环
     *
     * 在独立线程中启动 poll 循环，以 poll_interval_ms 间隔轮询 Kafka。
     * 消息累积至 max_poll_records 条或 poll 返回空后，
     * 通过 on_batch 回调投递整个批次。
     *
     * @param on_batch 批次回调函数，接收 unique_ptr<Message> 的 vector
     *                 回调中抛出异常会导致 poll 线程终止，需调用方自行处理。
     * @throws std::runtime_error 若 poll 线程已运行
     */
    void start(std::function<void(std::vector<std::unique_ptr<RdKafka::Message>>)> on_batch);

    /**
     * @brief 优雅停止消费
     *
     * 设置停止标志，唤醒 poll 线程，等待线程退出，
     * 然后关闭 consumer 并主动离开 group。
     *
     * @note 幂等: 多次调用安全。
     * @note 阻塞直到 poll 线程完全退出。
     */
    void stop();

    /**
     * @brief 获取 Consumer Group Metadata
     *
     * 用于 CTP (Consume-Transform-Produce) 模式中
     * send_offsets_to_transaction 调用。
     *
     * @return RdKafka::ConsumerGroupMetadata* 指向 group metadata 的指针
     *         调用方不拥有该指针，consumer 析构后指针失效。
     * @note 线程安全
     */
    [[nodiscard]] RdKafka::ConsumerGroupMetadata* groupMetadata() const;

    /**
     * @brief 查询 consumer lag
     *
     * 计算所有分配分区的高水位 offset 与当前 committed offset 的差值之和。
     *
     * @return int64_t 总 lag，-1 表示查询失败
     * @note 线程安全，可在任意线程调用
     */
    [[nodiscard]] int64_t consumerLag() const;

    /**
     * @brief 查询当前 committed offset 总和
     *
     * @return int64_t 所有分配分区的 committed offset 之和，-1 表示查询失败
     * @note 线程安全
     */
    [[nodiscard]] int64_t committedOffset() const;

    /**
     * @brief 将 RdKafka 错误码转为可读字符串
     *
     * @param err RdKafka 错误码
     * @return std::string 人类可读的错误描述
     * @note 静态方法，线程安全
     */
    [[nodiscard]] static std::string errorString(RdKafka::ErrorCode err);

private:
    /**
     * @brief poll 循环主函数，运行在独立线程中
     */
    void pollLoop();

    /**
     * @brief 设置 SASL 相关配置
     *
     * @param conf RdKafka Conf 对象指针
     */
    void applySaslConfig(RdKafka::Conf* conf) const;

    /// @brief 消费者配置（不可变）
    ConsumerConfig config_;

    /// @brief RdKafka 全局配置对象
    std::unique_ptr<RdKafka::Conf> global_conf_;

    /// @brief RdKafka topic 配置对象
    std::unique_ptr<RdKafka::Conf> topic_conf_;

    /// @brief RdKafka KafkaConsumer 实例
    RdKafka::KafkaConsumer* consumer_{nullptr};

    /// @brief Poll 线程
    std::thread poll_thread_;

    /// @brief 批次回调函数
    std::function<void(std::vector<std::unique_ptr<RdKafka::Message>>)> on_batch_;

    /// @brief 停止标志 (atomic, relaxed 顺序即可)
    std::atomic<bool> stopped_{false};

    /// @brief 线程运行标志，防止重复启动
    std::atomic<bool> running_{false};

    /// @brief 保护 lag/offset 查询的 mutex
    mutable std::mutex query_mutex_;
};

}  // namespace wge::kafka
