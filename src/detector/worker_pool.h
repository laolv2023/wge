#pragma once

/**
 * @file worker_pool.h
 * @brief WgeWorkerPool — WGE 检测线程池
 *
 * ## 模块职责
 * WgeWorkerPool 管理固定数量的 worker 线程，每个线程从有界阻塞队列中
 * 消费 HttpAccessEvent，调用 WGE 引擎进行安全检测，并将结果通过
 * AlertProducer 发送到下游 Kafka topic。
 *
 * ## 核心设计
 * - **固定线程池 (Thread-per-core 模型)**: 启动时创建固定数量的 worker 线程，
 *   线程数默认等于 CPU 核心数，避免过度订阅和上下文切换开销。
 * - **有界阻塞队列 (背压控制)**: 使用 std::deque + 两个条件变量实现
 *   BoundedBlockingQueue。队列满时 submitBatch 阻塞等待，防止内存无限增长。
 * - **Per-task 超时机制 (cooperative)**: 在 detect() 流程的每个步骤间插入
 *   超时检查点，超时后提前返回空结果，避免因 WGE 规则执行过慢导致 worker 阻塞。
 * - **优雅停止**: 先设置停止标志唤醒所有线程，排空剩余队列任务并逐个处理，
 *   最后等待所有 worker 线程退出。析构函数自动调用 stop()。
 *
 * ## 线程模型
 * - 1 个生产者线程（通常是 Kafka consumer poll 线程）：调用 submitBatch()
 * - N 个消费者线程（worker 线程）：运行 workerLoop()
 * - 共享资源：task_queue_（有界阻塞队列）、stopped_/started_ 原子标志
 *
 * ## 线程安全
 * - 所有公有方法线程安全
 * - submitBatch() 使用 queue_mutex_ + not_full_（满等待）
 * - workerLoop() 使用 queue_mutex_ + not_empty_（空等待）
 * - started_/stopped_ 使用 std::atomic 保证可见性
 * - 析构函数捕获所有异常，保证不抛出
 */

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// 引入真实 WGE SDK 头文件
#include "engine.h"
#include "rule.h"
#include "transaction.h"

namespace wge::kafka {

// 前向声明
class HttpAccessEvent;
class WgeAlertEvent;

namespace detector {

struct AlertResult;

}  // namespace detector

namespace metrics {
class Metrics;
}  // namespace metrics

// 前向声明 AlertProducer
class AlertProducer;

namespace detector {

// ============================================================================
// WorkerConfig
// ============================================================================

/**
 * @brief Worker 线程池配置结构体
 *
 * 集中管理检测线程池的所有可调参数。默认值适用于中等负载场景，
 * 高吞吐场景建议根据 CPU 核心数和 WGE 规则复杂度调优。
 */
struct WorkerConfig {
    /// @brief Worker 线程数。0 表示自动检测 (std::thread::hardware_concurrency())
    /// @note 线程数 = 0 时，构造函数中自动设为核心数，若检测失败则回退为 4
    int worker_threads{8};

    /// @brief 最大待处理任务数（有界队列容量），控制内存使用上限
    /// @note 队列满时 submitBatch() 阻塞等待，形成背压，
    ///       防止 Kafka consumer 拉取速度远超 worker 处理速度导致 OOM
    int max_pending_tasks{2000};

    /// @brief 单任务超时时间 (毫秒)
    /// @note 超时后 detect() 在下一个检查点提前返回，
    ///       已匹配的规则结果仍会保留并生成告警
    int task_timeout_ms{5000};
};

// ============================================================================
// WgeWorkerPool
// ============================================================================

/**
 * @brief WGE 检测工作线程池
 *
 * 管理 N 个 worker 线程，从有界阻塞队列中消费检测任务。
 * 每个任务调用 detect() 函数执行 WGE 规则匹配，结果通过 AlertProducer 发送。
 *
 * ## 生命周期
 * 1. **构造**: 校验并补全 config，不启动线程
 * 2. **start()**: 创建 N 个 worker 线程，开始消费队列
 * 3. **运行**: 通过 submitBatch() 提交任务，worker 自动处理
 * 4. **stop()**: 设置停止标志 → 唤醒线程 → 等待退出 → 排空剩余任务
 * 5. **析构**: 若未 stop 则自动调用 stop()（异常安全）
 *
 * ## 使用示例:
 * @code
 *   // 构造（不启动线程）
 *   WgeWorkerPool pool(engine, producer, metrics, config);
 *   // 启动所有 worker 线程
 *   pool.start();
 *
 *   // 批量提交检测任务（可能阻塞若队列满）
 *   int64_t submitted = pool.submitBatch(std::move(events));
 *
 *   // 优雅停止（阻塞直到所有 worker 退出，并排空队列）
 *   pool.stop();
 *   // 或依赖析构函数自动停止
 * @endcode
 *
 * @note engine 和 producer 必须为引用，生命周期需长于 WgeWorkerPool
 * @note 不可复制、不可移动（管理线程资源）
 */
class WgeWorkerPool {
public:
    /**
     * @brief 构造函数
     *
     * @param engine   WGE 检测引擎引用 (共享，非所有权)
     * @param producer 告警生产者引用
     * @param metrics  Metrics 引用
     * @param config   Worker 配置
     *
     * @note engine 和 producer 生命周期必须长于 WgeWorkerPool
     */
    WgeWorkerPool(const Wge::Engine& engine,
                  AlertProducer& producer,
                  metrics::Metrics& metrics,
                  const WorkerConfig& config);

    /**
     * @brief 析构函数
     *
     * 若尚未调用 stop()，自动优雅停止。
     * 异常内部捕获并记录日志。
     */
    ~WgeWorkerPool();

    // 禁止拷贝和移动
    WgeWorkerPool(const WgeWorkerPool&) = delete;
    WgeWorkerPool& operator=(const WgeWorkerPool&) = delete;
    WgeWorkerPool(WgeWorkerPool&&) = delete;
    WgeWorkerPool& operator=(WgeWorkerPool&&) = delete;

    /**
     * @brief 启动所有 worker 线程
     *
     * 创建 N 个线程，每个线程运行 workerLoop。
     * 必须在 submitBatch 之前调用。
     *
     * @throws std::runtime_error 若线程已启动
     */
    void start();

    /**
     * @brief 优雅停止
     *
     * 1. 设置停止标志
     * 2. 唤醒所有等待线程
     * 3. 等待所有 worker 线程退出
     * 4. 排空并处理队列中剩余任务
     *
     * @note 幂等: 多次调用安全
     * @note 阻塞直到所有 worker 线程退出
     */
    void stop();

    /**
     * @brief 批量提交检测任务
     *
     * 将所有 events 加入有界队列。若队列满则阻塞等待消费者取走。
     *
     * @param events 要提交的 HttpAccessEvent 列表
     * @return int64_t 实际提交的任务数
     * @throws std::runtime_error 若线程池未启动
     */
    int64_t submitBatch(std::vector<std::shared_ptr<HttpAccessEvent>>&& events);

    /**
     * @brief 获取当前队列待处理任务数
     */
    [[nodiscard]] size_t pendingCount() const;

    /**
     * @brief 获取活跃 worker 线程数
     */
    [[nodiscard]] size_t activeCount() const;

private:
    /**
     * @brief Worker 线程主循环
     *
     * 从队列中取 event，调用 detect() 执行检测，
     * 通过 AlertProducer 发送结果告警。
     *
     * @param worker_id Worker 编号 (0..N-1)，用于日志标识
     */
    void workerLoop(int worker_id);

    /**
     * @brief 执行 WGE 检测
     *
     * 对单个 HttpAccessEvent 执行完整的 WGE 检测流程:
     * 1. 创建 Transaction
     * 2. processConnection()
     * 3. processUri()
     * 4. processRequestHeaders()
     * 5. processRequestBody()
     * 6. processResponseHeaders()
     * 7. processResponseBody()
     * 8. 从 Transaction 获取 matched_variables 和匹配规则
     * 9. 构建 AlertResult
     *
     * @param event 待检测的 HTTP 访问事件
     * @return AlertResult 检测结果
     */
    AlertResult detect(const HttpAccessEvent& event,
                       std::chrono::steady_clock::time_point deadline
                           = std::chrono::steady_clock::time_point::max());

    /**
     * @brief LogCallback — WGE 规则匹配回调
     *
     * 由 WGE 引擎在每次规则匹配时调用，将匹配信息追加到 AlertResult。
     *
     * @param rule      匹配的规则
     * @param user_data AlertResult 指针
     */
    static void onRuleMatch(const Wge::Rule& rule, void* user_data);

    // ---- 配置 ----
    const Wge::Engine& engine_;          ///< WGE 检测引擎引用（非所有权，外部管理生命周期）
    AlertProducer& producer_;            ///< 告警生产者引用，用于发送检测结果
    metrics::Metrics& metrics_;          ///< Metrics 单例引用，用于指标上报
    WorkerConfig config_;                ///< Worker 配置（构造时补全默认值）

    // ---- 线程池 ----
    std::vector<std::thread> workers_;   ///< Worker 线程容器，start() 时创建
    std::atomic<bool> started_{false};   ///< 是否已启动（CAS 防护重复 start）
    std::atomic<bool> stopped_{false};   ///< 是否已请求停止（CAS 防护重复 stop）
    std::atomic<size_t> active_workers_{0}; ///< 当前正在执行 detect() 的线程数

    // ---- 任务队列 (有界阻塞队列) ----
    mutable std::mutex queue_mutex_;     ///< 保护 task_queue_ 的互斥锁
    std::condition_variable not_empty_;  ///< worker 等待条件：队列非空
    std::condition_variable not_full_;   ///< submitBatch 等待条件：队列未满
    std::deque<std::shared_ptr<HttpAccessEvent>> task_queue_; ///< 任务队列（FIFO）

    // ---- Per-task 超时支持 ----
    // 超时检测通过 workerLoop 中的 chrono::steady_clock 实现，
    // detect() 在各步骤间插入 timed_out() 检查点实现协作式超时
};

}  // namespace detector
}  // namespace wge::kafka
