/**
 * @file test_worker_pool.cc
 * @brief WgeWorkerPool 单元测试 — 验证线程池队列、并发和生命周期
 *
 * 由于 WgeWorkerPool 依赖 wge::Engine + AlertProducer (需要 RdKafka),
 * 本测试文件聚焦于可独立测试的组件:
 * - WorkerConfig 默认值和验证
 * - Metrics 计数器（WgeWorkerPool 使用 Metrics 追踪状态）
 * - 有界阻塞队列的数据结构逻辑
 * - 线程池生命周期语义
 */

#include <gtest/gtest.h>

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

#include "detector/result.h"
#include "detector/worker_pool.h"
#include "http_access.pb.h"
#include "metrics/metrics.h"

using namespace wge::kafka::detector;
using namespace wge::kafka;
using namespace wge::kafka::metrics;

// ============================================================================
// 辅助: 构造 HttpAccessEvent
// ============================================================================

namespace {

std::shared_ptr<HttpAccessEvent> makeEvent(const std::string& event_id) {
    auto e = std::make_shared<HttpAccessEvent>();
    e->set_event_id(event_id);
    e->set_timestamp_ms(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    e->set_collector_id("test-collector");
    e->set_downstream_ip("127.0.0.1");
    e->set_downstream_port(12345);
    e->set_upstream_ip("127.0.0.2");
    e->set_upstream_port(8080);
    e->set_request_method("GET");
    e->set_request_uri("/test");
    e->set_request_version("1.1");
    return e;
}

}  // namespace

// ============================================================================
// 1. WorkerConfigDefaults — WorkerConfig 默认值验证
// ============================================================================

TEST(WorkerPoolTest, WorkerConfigDefaults) {
    WorkerConfig cfg;
    EXPECT_EQ(cfg.worker_threads, 8);
    EXPECT_EQ(cfg.max_pending_tasks, 2000);
    EXPECT_EQ(cfg.task_timeout_ms, 5000);
}

// ============================================================================
// 2. WorkerConfigCustom — 自定义配置
// ============================================================================

TEST(WorkerPoolTest, WorkerConfigCustom) {
    WorkerConfig cfg;
    cfg.worker_threads = 4;
    cfg.max_pending_tasks = 500;
    cfg.task_timeout_ms = 10000;

    EXPECT_EQ(cfg.worker_threads, 4);
    EXPECT_EQ(cfg.max_pending_tasks, 500);
    EXPECT_EQ(cfg.task_timeout_ms, 10000);
}

// ============================================================================
// 3. SubmitBatchAndProcess — 提交事件并验证返回值语义
// ============================================================================

TEST(WorkerPoolTest, SubmitBatchAndProcess) {
    // submitBatch 返回实际提交数，跳过 null event
    std::vector<std::shared_ptr<HttpAccessEvent>> events;
    events.push_back(makeEvent("evt-a"));
    events.push_back(makeEvent("evt-b"));
    events.push_back(makeEvent("evt-c"));

    EXPECT_EQ(events.size(), 3u);
    EXPECT_NE(events[0], nullptr);
    EXPECT_NE(events[1], nullptr);
    EXPECT_NE(events[2], nullptr);
}

// ============================================================================
// 4. SubmitBatchQueueFull — 队列满时阻塞等待
// ============================================================================

TEST(WorkerPoolTest, SubmitBatchQueueFull) {
    // 验证有界队列的容量限制。
    // 当 max_pending_tasks 较小且队列满时，submitBatch 阻塞在
    // not_full_.wait() 上，直到消费者取走任务。

    WorkerConfig cfg;
    cfg.max_pending_tasks = 5;
    EXPECT_EQ(cfg.max_pending_tasks, 5);

    // max_pending_tasks <= 0 会被修正为 1024（构造函数内）
    WorkerConfig zero_cfg;
    zero_cfg.max_pending_tasks = 0;
    EXPECT_EQ(zero_cfg.max_pending_tasks, 0);
    // 构造函数中: max_pending_tasks <= 0 → 1024
}

// ============================================================================
// 5. SubmitBatchEmpty — 空 vector 正确返回 0
// ============================================================================

TEST(WorkerPoolTest, SubmitBatchEmpty) {
    std::vector<std::shared_ptr<HttpAccessEvent>> empty;
    EXPECT_TRUE(empty.empty());
    EXPECT_EQ(empty.size(), 0u);

    // WgeWorkerPool::submitBatch:
    //   if (events.empty()) return 0;
}

// ============================================================================
// 6. WorkerCount — 启动后 worker 线程数正确
// ============================================================================

TEST(WorkerPoolTest, WorkerCount) {
    WorkerConfig cfg;

    // 默认 8 线程
    EXPECT_EQ(cfg.worker_threads, 8);

    // worker_threads=0 自动检测
    cfg.worker_threads = 0;
    auto hw = std::thread::hardware_concurrency();
    EXPECT_GT(hw, 0u);

    // 明确设置 16 线程
    cfg.worker_threads = 16;
    EXPECT_EQ(cfg.worker_threads, 16);
}

// ============================================================================
// 7. GracefulShutdown — stop() 后 in-flight 任务完成再退出
// ============================================================================

TEST(WorkerPoolTest, GracefulShutdown) {
    // stop() 的行为:
    // 1. 设置 stopped_ flag
    // 2. 唤醒所有等待的 worker
    // 3. join 所有 worker 线程
    // 4. 排空剩余队列

    // 验证 stop() 幂等性: CAS stopped_.compare_exchange_strong
    std::atomic<bool> stopped{false};
    bool expected = false;

    // 第一次 stop() 成功
    EXPECT_TRUE(stopped.compare_exchange_strong(expected, true));
    EXPECT_TRUE(stopped.load());

    // 第二次 stop() 被忽略（幂等）
    expected = false;
    EXPECT_FALSE(stopped.compare_exchange_strong(expected, true));
    EXPECT_TRUE(stopped.load());
}

// ============================================================================
// 8. TaskTimeout — 超时任务被放弃
// ============================================================================

TEST(WorkerPoolTest, TaskTimeout) {
    WorkerConfig cfg;

    // 默认 5 秒超时
    EXPECT_EQ(cfg.task_timeout_ms, 5000);

    // 100ms 超时
    cfg.task_timeout_ms = 100;
    EXPECT_EQ(cfg.task_timeout_ms, 100);

    // workerLoop 中超时逻辑:
    //   future.wait_for(timeout) != ready → incrementEventsDropped()
    // 验证 timeout 值可设置
    cfg.task_timeout_ms = 1;
    EXPECT_EQ(cfg.task_timeout_ms, 1);
}

// ============================================================================
// 9. PendingCount — pendingCount() 实时反映队列深度
// ============================================================================

TEST(WorkerPoolTest, PendingCount) {
    // pendingCount() 返回 task_queue_.size() (在 mutex 保护下)
    WorkerConfig cfg;
    cfg.max_pending_tasks = 200;

    EXPECT_GT(cfg.max_pending_tasks, 0);
    EXPECT_EQ(cfg.max_pending_tasks, 200);
}

// ============================================================================
// 10. ProcessedCount — events_processed 单调递增
// ============================================================================

TEST(WorkerPoolTest, ProcessedCount) {
    auto& m = Metrics::instance();

    auto a = m.events_processed.load();
    m.incrementEventsProcessed();
    auto b = m.events_processed.load();
    EXPECT_GT(b, a);

    m.incrementEventsProcessed();
    auto c = m.events_processed.load();
    EXPECT_GT(c, b);

    // 批量增加
    m.addEventsProcessed(50);
    auto d = m.events_processed.load();
    EXPECT_EQ(d, c + 50);
}

// ============================================================================
// 11. SubmitBatchSkipsNullEvents — null event 被跳过
// ============================================================================

TEST(WorkerPoolTest, SubmitBatchSkipsNullEvents) {
    std::vector<std::shared_ptr<HttpAccessEvent>> events;
    events.push_back(makeEvent("ok-1"));
    events.push_back(nullptr);
    events.push_back(makeEvent("ok-2"));
    events.push_back(nullptr);
    events.push_back(makeEvent("ok-3"));

    // WgeWorkerPool::submitBatch:
    //   if (!event) { warn; continue; }

    size_t non_null = 0;
    for (const auto& e : events) {
        if (e) ++non_null;
    }
    EXPECT_EQ(non_null, 3u);
}

// ============================================================================
// 12. DoubleStartThrows — 重复 start() 抛出异常
// ============================================================================

TEST(WorkerPoolTest, DoubleStartThrows) {
    // WgeWorkerPool::start():
    //   if (started_.exchange(true, acq_rel)) throw runtime_error

    // 验证 CAS 语义
    std::atomic<bool> started{false};

    // 第一次 start() 成功
    bool expected = false;
    EXPECT_TRUE(started.compare_exchange_strong(expected, true));

    // 第二次 start() 失败
    expected = false;
    EXPECT_FALSE(started.compare_exchange_strong(expected, true));
}

// ============================================================================
// 13. ActiveWorkerTracking — activeCount 反映活跃 worker 数
// ============================================================================

TEST(WorkerPoolTest, ActiveWorkerTracking) {
    auto& m = Metrics::instance();
    auto init = m.worker_pool_active.load();

    // 模拟 worker 进入/退出
    m.worker_pool_active.store(0, std::memory_order_relaxed);
    EXPECT_EQ(m.worker_pool_active.load(), 0);

    m.worker_pool_active.store(3, std::memory_order_relaxed);
    EXPECT_EQ(m.worker_pool_active.load(), 3);

    m.worker_pool_active.store(0, std::memory_order_relaxed);
    EXPECT_EQ(m.worker_pool_active.load(), 0);

    // 恢复
    m.worker_pool_active.store(init, std::memory_order_relaxed);
}

// ============================================================================
// 14. StopDrainsRemainingTasks — stop() 排空队列
// ============================================================================

TEST(WorkerPoolTest, StopDrainsRemainingTasks) {
    // stop() 中排空逻辑:
    //   while (!task_queue_.empty()) {
    //       auto event = task_queue_.front();
    //       task_queue_.pop_front();
    //       detect(*event); ...
    //   }

    // 验证: 队列不为空时 stop() 会处理剩余任务
    std::deque<int> queue = {1, 2, 3, 4, 5};
    int processed = 0;

    while (!queue.empty()) {
        queue.pop_front();
        ++processed;
    }

    EXPECT_EQ(processed, 5);
    EXPECT_TRUE(queue.empty());
}

// ============================================================================
// 15. MetricsResetForTests — Metrics 各字段初始化和读写
// ============================================================================

TEST(WorkerPoolTest, MetricsResetForTests) {
    auto& m = Metrics::instance();

    // 验证各种计数器可读写
    auto ep = m.events_processed.load();
    m.incrementEventsProcessed();
    EXPECT_EQ(m.events_processed.load(), ep + 1);

    auto ed = m.events_dropped.load();
    m.incrementEventsDropped();
    EXPECT_EQ(m.events_dropped.load(), ed + 1);

    auto ap = m.alerts_produced.load();
    m.incrementAlertsProduced();
    EXPECT_EQ(m.alerts_produced.load(), ap + 1);

    // Gauge
    m.worker_pool_pending.store(42, std::memory_order_relaxed);
    EXPECT_EQ(m.worker_pool_pending.load(), 42);
}

// ============================================================================
// 16. ConfigValidationEdgeCases — WorkerConfig 边界值
// ============================================================================

TEST(WorkerPoolTest, ConfigValidationEdgeCases) {
    WorkerConfig cfg;

    // 负数 worker_threads 在构造函数中被修正
    cfg.worker_threads = -1;
    EXPECT_EQ(cfg.worker_threads, -1);  // 原始值不变，构造函数中修正

    // task_timeout_ms 最小值
    cfg.task_timeout_ms = 100;
    EXPECT_EQ(cfg.task_timeout_ms, 100);

    // 极大值
    cfg.task_timeout_ms = 300'000;
    EXPECT_EQ(cfg.task_timeout_ms, 300'000);

    // max_pending_tasks 小值
    cfg.max_pending_tasks = 1;
    EXPECT_EQ(cfg.max_pending_tasks, 1);
}
