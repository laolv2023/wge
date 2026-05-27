#pragma once

/**
 * @file worker_pool.h
 * @brief WgeWorkerPool — WGE 检测线程池
 *
 * WgeWorkerPool 管理固定数量的 worker 线程，每个线程从有界队列中
 * 消费 HttpAccessEvent，调用 WGE 引擎进行安全检测，并将结果通过
 * AlertProducer 发送。
 *
 * 特性:
 * - 固定线程池 (Thread-per-core 模型)
 * - 有界阻塞队列 (背压控制)
 * - Per-task 超时机制 (std::future + wait_for)
 * - 优雅停止 (排空队列、等待 in-flight 任务)
 *
 * 线程安全: 所有公有方法线程安全，使用 mutex + condition_variable 同步。
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

namespace wge {

// 前向声明 WGE 类型 (来自 wge 库)
class Engine;
class Transaction;
struct Rule;
struct Detail;

}  // namespace wge

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
 * @brief Worker 线程池配置
 */
struct WorkerConfig {
    /// @brief Worker 线程数。0 表示自动检测 (hardware_concurrency)
    int worker_threads{8};

    /// @brief 最大待处理任务数 (队列容量)
    int max_pending_tasks{2000};

    /// @brief 单任务超时时间 (ms)
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
 * 使用示例:
 * @code
 *   WgeWorkerPool pool(engine, producer, metrics, config);
 *   pool.start();
 *
 *   // 批量提交
 *   int64_t submitted = pool.submitBatch(std::move(events));
 *
 *   // 优雅停止
 *   pool.stop();
 * @endcode
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
    WgeWorkerPool(const wge::Engine& engine,
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
    static void onRuleMatch(const wge::Rule& rule, void* user_data);

    // ---- 配置 ----
    const wge::Engine& engine_;
    AlertProducer& producer_;
    metrics::Metrics& metrics_;
    WorkerConfig config_;

    // ---- 线程池 ----
    std::vector<std::thread> workers_;
    std::atomic<bool> started_{false};
    std::atomic<bool> stopped_{false};
    std::atomic<size_t> active_workers_{0};

    // ---- 任务队列 ----
    mutable std::mutex queue_mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::deque<std::shared_ptr<HttpAccessEvent>> task_queue_;

    // ---- Per-task 超时 support ----
    // (实际超时逻辑在 workerLoop 中使用 chrono 实现)
};

}  // namespace detector
}  // namespace wge::kafka
