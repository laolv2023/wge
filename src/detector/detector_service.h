#pragma once

/**
 * @file detector_service.h
 * @brief DetectorService — 顶层编排服务
 *
 * DetectorService 是 WGE-Kafka Detector 的顶层协调者，负责:
 * - 启动 Kafka Consumer poll 线程
 * - 协调 Consumer → LogMapper → WorkerPool → AlertProducer 的数据流
 * - 管理优雅关闭序列
 * - 管理 DLQ (死信队列)
 * - Offset 管理 (CTP 模式下由 AlertProducer 事务管理)
 *
 * 线程安全: 内部使用 atomic flag 管理状态，start/stop 可跨线程调用。
 */

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

// 前向声明 RdKafka 类型
namespace RdKafka {
class Message;
}

namespace wge::kafka {

// 前向声明
class KafkaConsumer;
class AlertProducer;
class DeadLetterQueue;

namespace config {
struct AppConfig;
}

namespace mapper {
class LogMapper;
}

namespace adapter {
class AktoPreprocessor;
}

namespace detector {

class WgeWorkerPool;

}  // namespace detector

namespace metrics {
class Metrics;
}  // namespace metrics

namespace detector {

// ============================================================================
// DetectorService
// ============================================================================

/**
 * @brief 检测器顶层服务
 *
 * 编排 Consumer → Mapper → WorkerPool → Producer 的数据管道。
 *
 * 数据流:
 *   Kafka → Consumer.poll → LogMapper.map → WorkerPool.submitBatch
 *                                              ↓
 *                                    detect() → AlertBuilder.build
 *                                              ↓
 *                                    AlertProducer.sendAlert → Kafka
 *
 * 使用示例:
 * @code
 *   DetectorService service(cfg, consumer, producer, dlq, mapper, pool, metrics);
 *   service.start();
 *   // ... 等待信号 ...
 *   service.stop();
 * @endcode
 */
class DetectorService {
public:
    /**
     * @brief 构造函数
     *
     * @param config   应用配置 (只读引用)
     * @param consumer Kafka 消费者
     * @param producer 告警生产者
     * @param dlq      死信队列
     * @param mapper   日志映射器
     * @param pool     Worker 线程池
     * @param metrics  Metrics 引用
     *
     * @note 所有引用参数的生命周期必须长于 DetectorService
     */
    DetectorService(const config::AppConfig& config,
                    KafkaConsumer& consumer,
                    AlertProducer& producer,
                    DeadLetterQueue& dlq,
                    mapper::LogMapper& mapper,
                    WgeWorkerPool& pool,
                    metrics::Metrics& metrics);

    /**
     * @brief 析构函数
     *
     * 若尚未调用 stop()，自动优雅停止。
     * 异常内部捕获并记录。
     */
    ~DetectorService();

    // 禁止拷贝和移动
    DetectorService(const DetectorService&) = delete;
    DetectorService& operator=(const DetectorService&) = delete;
    DetectorService(DetectorService&&) = delete;
    DetectorService& operator=(DetectorService&&) = delete;

    /**
     * @brief 启动所有子组件
     *
     * 启动序列:
     * 1. AlertProducer::initTransactions() (若启用事务)
     * 2. AlertProducer::flushLoop() — 启动批量发送线程
     * 3. WgeWorkerPool::start() — 启动 worker 线程
     * 4. KafkaConsumer::start() — 启动 poll 线程 (最后启动)
     *
     * @throws std::runtime_error 若已启动
     */
    void start();

    /**
     * @brief 优雅停止
     *
     * 关闭序列 (与启动顺序相反):
     * 1. KafkaConsumer::stop() — 停止消费新消息
     * 2. WgeWorkerPool::stop() — 等待 in-flight 任务完成
     * 3. AlertProducer::close() — 清空队列并提交最后事务
     * 4. DeadLetterQueue::close() — 清空 DLQ
     *
     * @note 幂等: 多次调用安全
     * @note 阻塞直到所有子组件完全停止
     */
    void stop();

    /**
     * @brief 检查服务是否在运行
     */
    [[nodiscard]] bool isRunning() const noexcept {
        return running_.load(std::memory_order_acquire);
    }

private:
    /**
     * @brief Consumer 批次回调
     *
     * 由 KafkaConsumer poll 线程调用:
     * 1. 遍历每条消息
     * 2. 反序列化 Protobuf 为 HttpAccessEvent (或通过 LogMapper.map)
     * 3. 成功 → WgeWorkerPool::submitBatch()
     * 4. 失败 → DeadLetterQueue::sendRaw()
     *
     * @param messages RdKafka::Message 的 unique_ptr vector
     */
    void onConsumerBatch(
        std::vector<std::unique_ptr<RdKafka::Message>> messages);

    // ---- 配置和依赖 ----
    const config::AppConfig& config_;
    KafkaConsumer& consumer_;
    AlertProducer& producer_;
    DeadLetterQueue& dlq_;
    mapper::LogMapper& mapper_;
    WgeWorkerPool& pool_;
    metrics::Metrics& metrics_;

    /// @brief 可选的 Akto 预处理器 (当 config_.mapping.preprocessor == "akto" 时启用)
    std::unique_ptr<adapter::AktoPreprocessor> akto_preprocessor_;

    // ---- 状态 ----
    std::atomic<bool> running_{false};
    std::atomic<bool> stopped_{false};
};

}  // namespace detector
}  // namespace wge::kafka
