#pragma once

/**
 * @file producer.h
 * @brief Kafka 告警生产者封装
 *
 * AlertProducer 封装 RdKafka::Producer，提供事务性批量发送能力。
 * 使用内部队列 + 独立线程实现异步批处理，保证告警发送的原子性和幂等性。
 *
 * 线程安全: sendAlert 可从任意线程调用（内部 mutex + deque + condvar），
 *           flushLoop 运行在独立线程中。
 */

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <librdkafka/rdkafkacpp.h>

namespace wge::kafka {

// 前向声明 protobuf 类型
class WgeAlertEvent;

// ============================================================================
// ProducerConfig
// ============================================================================

/**
 * @brief Kafka Producer 配置结构体
 *
 * 控制告警生产者的连接、事务、批处理和安全参数。
 *
 * @note POD 风格，读取线程安全，修改需外部同步。
 */
struct ProducerConfig {
    /// @brief Kafka broker 地址列表，逗号分隔
    std::string bootstrap_servers{"localhost:9092"};

    /// @brief 输出的告警 topic 名称
    std::string topic{};

    /// @brief 事务 ID，用于 exactly-once 语义。
    ///        为空则仅启用幂等（不启用事务）。
    std::string transactional_id{};

    /// @brief 批量发送大小（累积多少条消息后提交事务）
    int32_t batch_size{50};

    /// @brief 消息批次累积等待时间 (ms)
    int32_t linger_ms{50};

    /// @brief 是否启用幂等生产者
    bool enable_idempotence{true};

    /// @brief 每个连接的最大在途请求数（幂等模式下建议 ≤ 5）
    int32_t max_in_flight_requests_per_connection{5};

    /// @brief 消息发送重试次数
    int32_t retries{3};

    /// @brief 压缩类型: none, gzip, snappy, lz4, zstd
    std::string compression_type{"lz4"};

    /// @brief 死信队列 Topic 名称
    std::string dlq_topic{"wge-dlq"};

    /// @brief 内部队列最大容量（防止 broker 宕机时 OOM）
    /// 当队列满时，新告警将被丢弃并记录警告日志
    size_t max_queue_size{100000};

    /// @brief 安全协议
    std::string security_protocol{};

    /// @brief SASL 机制
    std::string sasl_mechanism{};

    /// @brief SASL 用户名
    std::string sasl_username{};

    /// @brief SASL 密码
    std::string sasl_password{};
};

// ============================================================================
// AlertProducer
// ============================================================================

/**
 * @brief Kafka 告警生产者
 *
 * 提供事务性的告警批量发送能力:
 * - sendAlert() 线程安全地将告警加入内部队列
 * - flushLoop() 在独立线程中批量消费队列，使用事务发送
 * - initTransactions() 初始化事务（需在 start 之前调用）
 *
 * 事务流程:
 *   1. begin_transaction()
 *   2. produce() 所有队列中的消息
 *   3. send_offsets_to_transaction() (CTP 模式)
 *   4. commit_transaction()
 *
 * 使用示例:
 * @code
 *   ProducerConfig cfg;
 *   cfg.bootstrap_servers = "kafka:9092";
 *   cfg.topic = "wge-alert";
 *   cfg.transactional_id = "wge-detector-01";
 *
 *   auto producer = std::make_shared<AlertProducer>(cfg);
 *   producer->initTransactions();
 *   producer->flushLoop([&consumer]() { return consumer.groupMetadata(); });
 *
 *   producer->sendAlert(alert_event);
 *   // ...
 *   producer->close();
 * @endcode
 *
 * @note 不可拷贝或移动。
 */
class AlertProducer {
public:
    /**
     * @brief 构造函数
     *
     * 创建 RdKafka::Producer 实例并设置所有配置。
     *
     * @param config 生产者配置
     * @throws std::runtime_error 若 producer 创建失败
     */
    explicit AlertProducer(const ProducerConfig& config);

    /**
     * @brief 析构函数
     *
     * 若尚未 close()，自动执行清理。
     * 异常在析构函数内捕获并记录。
     */
    ~AlertProducer();

    // 禁止拷贝和移动
    AlertProducer(const AlertProducer&) = delete;
    AlertProducer& operator=(const AlertProducer&) = delete;
    AlertProducer(AlertProducer&&) = delete;
    AlertProducer& operator=(AlertProducer&&) = delete;

    /**
     * @brief 初始化事务
     *
     * 调用 producer_->init_transactions()。
     * 需在 flushLoop 启动前调用。
     *
     * @throws std::runtime_error 若初始化失败
     */
    void initTransactions();

    /**
     * @brief 线程安全的告警入队
     *
     * 将告警事件加入内部队列，由 flushLoop 线程负责发送。
     * 使用 mutex + deque 保护队列。
     *
     * @param alert 告警事件智能指针，调用方可共享所有权
     * @note 线程安全: 可从任意线程调用
     */
    void sendAlert(std::shared_ptr<WgeAlertEvent> alert);

    /**
     * @brief 启动批量发送循环
     *
     * 在独立线程中运行，持续从内部队列消费告警，
     * 以事务方式批量发送到 Kafka。
     *
     * @param group_metadata_provider 回调函数，每次事务前调用以获取最新的
     *                                Consumer group metadata（避免 rebalance 后过时）。
     *                                若返回 nullptr，跳过 offset 提交。
     * @note 每次事务前都会调用该回调，确保 rebalance 后 metadata 不失效。
     */
    void flushLoop(std::function<RdKafka::ConsumerGroupMetadata*()> group_metadata_provider);

    /**
     * @brief 优雅关闭
     *
     * 停止 flush 线程，清空剩余队列（最后一次事务发送），
     * flush 所有待发送消息，关闭 producer。
     *
     * @note 幂等: 多次调用安全。
     * @note 阻塞直到 flush 线程退出。
     */
    void close();

    /**
     * @brief 获取底层 RdKafka::Producer 指针
     *
     * 供需要直接操作 producer 的场景使用（如 DLQ 共享 producer）。
     *
     * @return RdKafka::Producer* 原始指针，本类拥有所有权
     */
    [[nodiscard]] RdKafka::Producer* rawProducer() const noexcept {
        return producer_;
    }

    /**
     * @brief 将 RdKafka 错误码转为可读字符串
     */
    [[nodiscard]] static std::string errorString(RdKafka::ErrorCode err);

private:
    /**
     * @brief 内部 flush 循环
     */
    void flushLoopImpl(std::function<RdKafka::ConsumerGroupMetadata*()> group_metadata_provider);

    /**
     * @brief 设置 SASL 配置
     */
    void applySaslConfig(RdKafka::Conf* conf) const;

    /**
     * @brief 从队列批量取出消息
     *
     * @param out 输出参数，存放取出的消息
     * @return size_t 取出的消息数
     */
    size_t drainQueue(std::vector<std::shared_ptr<WgeAlertEvent>>& out);

    /**
     * @brief 序列化 WgeAlertEvent 为 protobuf 字节流
     */
    [[nodiscard]] std::string serializeAlert(const WgeAlertEvent& alert) const;

    /// @brief 生产者配置（不可变）
    ProducerConfig config_;

    /// @brief RdKafka 配置对象
    std::unique_ptr<RdKafka::Conf> conf_;

    /// @brief RdKafka Producer 实例
    RdKafka::Producer* producer_{nullptr};

    /// @brief 告警队列互斥锁
    mutable std::mutex queue_mutex_;

    /// @brief 告警队列
    std::deque<std::shared_ptr<WgeAlertEvent>> alert_queue_;

    /// @brief 队列条件变量，唤醒 flush 线程
    std::condition_variable queue_cv_;

    /// @brief Flush 线程
    std::thread flush_thread_;

    /// @brief 停止标志
    std::atomic<bool> stopped_{false};

    /// @brief 运行标志，防止重复启动
    std::atomic<bool> running_{false};
};

}  // namespace wge::kafka
