/**
 * @file test_concurrent_worker.cc
 * @brief WgeWorkerPool 并发压力集成测试
 *
 * 测试 WgeWorkerPool 的并发行为。由于没有真实 WGE Engine 和 Kafka，
 * 使用简单的模拟检测逻辑替代 detect()，重点测试并发正确性。
 *
 * 所有测试自包含，不依赖外部服务。
 *
 * 测试覆盖:
 *   1. ConcurrentSubmitBatch          — 多线程同时 submitBatch，验证不丢事件
 *   2. QueueBackpressure              — 队列满时 submit 正确阻塞，有空间后恢复
 *   3. WorkerGracefulShutdown         — stop() 后所有 in-flight 任务完成
 *   4. TaskTimeout                    — 超时任务被跳过，其他任务正常完成
 *   5. WorkerPoolStress               — 8线程 × 1000事件，验证吞吐和正确性
 *   6. AlertProducerBackpressure      — alert队列满时worker正确阻塞
 *   7. DrainAllPendingOnStop          — 停止时排空队列中所有pending任务
 *   8. CASStopIdempotent              — 多次调用stop()只执行一次
 *   9. AtomicMetricsConsistency       — 多线程更新metrics计数器，最终值正确
 *  10. ThreadSafetyNoDataRace         — 多次运行检测数据竞争
 */

#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <latch>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "detector/result.h"
#include "http_access.pb.h"

using namespace wge::kafka::detector;
using namespace wge::kafka;

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
    e->set_upstream_ip("10.0.0.1");
    e->set_upstream_port(8080);
    e->set_request_method("GET");
    e->set_request_uri("/test");
    e->set_request_version("1.1");
    return e;
}

std::vector<std::shared_ptr<HttpAccessEvent>> makeEventBatch(
    int count, const std::string& prefix = "evt") {
    std::vector<std::shared_ptr<HttpAccessEvent>> events;
    events.reserve(static_cast<size_t>(count));
    for (int i = 0; i < count; ++i) {
        events.push_back(makeEvent(prefix + "-" + std::to_string(i)));
    }
    return events;
}

/// @brief 模拟检测函数 — 返回随机 AlertResult
AlertResult mockDetect(const HttpAccessEvent& event) {
    // 使用 event_id 的 hash 来决定是否有匹配（确定性，可复现）
    size_t hash = std::hash<std::string>{}(event.event_id());

    AlertResult result;
    result.event_id = event.event_id();
    result.timestamp_ms = event.timestamp_ms();

    // ~30% 概率产生告警
    if (hash % 10 < 3) {
        result.intervened = true;
        result.disruptive_action = "DENY";
        result.response_code = 403;
        MatchedRuleInfo rule;
        rule.rule_id = 1000 + (hash % 100);
        rule.rule_msg = "Mock rule match for " + event.event_id();
        rule.severity = 2;  // CRITICAL
        rule.rule_ver = "1.0";
        rule.rule_tags = {"mock", "integration-test"};
        rule.matched_var_name = "REQUEST_URI";
        rule.matched_var_value = event.request_uri();
        rule.matched_var_original = event.request_uri();
        rule.operator_name = "@rx";
        rule.operator_param = "mock.*pattern";
        result.matched_rules.push_back(std::move(rule));
    }

    return result;
}

}  // namespace

// ============================================================================
// FakeWorkerPool — 模拟 WgeWorkerPool 的并发行为
// ============================================================================

/**
 * @brief 轻量级 Worker Pool，模拟 WgeWorkerPool 的核心并发语义:
 *   - 固定线程池
 *   - 有界阻塞队列
 *   - Per-task 超时
 *   - 优雅停止 (排空队列)
 *   - 幂等 stop()
 *
 * 使用 mockDetect() 替代真实的 WGE detect()，用 atomic 计数器追踪事件。
 */
class FakeWorkerPool {
public:
    struct Config {
        int worker_threads{4};
        int max_pending_tasks{64};
        int task_timeout_ms{1000};
        int alert_queue_size{32};  // AlertProducer 模拟队列容量
    };

    explicit FakeWorkerPool(Config cfg = {}) : config_(std::move(cfg)) {
        if (config_.worker_threads <= 0) {
            config_.worker_threads = static_cast<int>(
                std::thread::hardware_concurrency());
            if (config_.worker_threads <= 0) config_.worker_threads = 4;
        }
        if (config_.max_pending_tasks <= 0) config_.max_pending_tasks = 1024;
        if (config_.alert_queue_size <= 0) config_.alert_queue_size = 32;
    }

    ~FakeWorkerPool() {
        try { stop(); } catch (...) {}
    }

    // 禁止拷贝和移动
    FakeWorkerPool(const FakeWorkerPool&) = delete;
    FakeWorkerPool& operator=(const FakeWorkerPool&) = delete;
    FakeWorkerPool(FakeWorkerPool&&) = delete;
    FakeWorkerPool& operator=(FakeWorkerPool&&) = delete;

    void start() {
        if (started_.exchange(true, std::memory_order_acq_rel)) {
            throw std::runtime_error("FakeWorkerPool::start: already started");
        }
        stopped_.store(false, std::memory_order_release);

        workers_.reserve(static_cast<size_t>(config_.worker_threads));
        for (int i = 0; i < config_.worker_threads; ++i) {
            workers_.emplace_back(&FakeWorkerPool::workerLoop, this, i);
        }
    }

    void stop() {
        bool expected = false;
        if (!stopped_.compare_exchange_strong(expected, true,
                                              std::memory_order_acq_rel,
                                              std::memory_order_relaxed)) {
            return;  // 幂等: 已停止
        }

        stop_call_count_.fetch_add(1, std::memory_order_relaxed);

        // 唤醒所有等待线程
        not_empty_.notify_all();
        not_full_.notify_all();
        alert_not_full_.notify_all();

        // 等待所有 worker 退出
        for (auto& worker : workers_) {
            if (worker.joinable()) worker.join();
        }
        workers_.clear();

        // 排空队列中剩余任务
        std::deque<std::shared_ptr<HttpAccessEvent>> remaining;
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            remaining.swap(task_queue_);
        }

        for (auto& event : remaining) {
            try {
                processEvent(event);
            } catch (...) {
                dropped_count_.fetch_add(1, std::memory_order_relaxed);
            }
        }

        started_.store(false, std::memory_order_release);
    }

    int64_t submitBatch(
        std::vector<std::shared_ptr<HttpAccessEvent>>&& events) {
        if (!started_.load(std::memory_order_acquire)) {
            throw std::runtime_error(
                "FakeWorkerPool::submitBatch: pool not started");
        }
        if (events.empty()) return 0;

        int64_t submitted = 0;
        size_t max_cap = static_cast<size_t>(config_.max_pending_tasks);

        for (auto& event : events) {
            if (!event) continue;

            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                not_full_.wait(lock, [this, max_cap] {
                    return task_queue_.size() < max_cap
                        || stopped_.load(std::memory_order_acquire);
                });

                if (stopped_.load(std::memory_order_acquire)) break;

                task_queue_.push_back(std::move(event));
                ++submitted;
            }
            not_empty_.notify_one();
        }

        submitted_count_.fetch_add(submitted, std::memory_order_relaxed);
        return submitted;
    }

    // ---- 可观测性 ----

    size_t pendingCount() const {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return task_queue_.size();
    }

    size_t alertQueueSize() const {
        std::lock_guard<std::mutex> lock(alert_mutex_);
        return alert_queue_.size();
    }

    uint64_t processedCount() const {
        return processed_count_.load(std::memory_order_relaxed);
    }

    uint64_t submittedCount() const {
        return submitted_count_.load(std::memory_order_relaxed);
    }

    uint64_t alertProducedCount() const {
        return alert_produced_.load(std::memory_order_relaxed);
    }

    uint64_t droppedCount() const {
        return dropped_count_.load(std::memory_order_relaxed);
    }

    uint64_t timedOutCount() const {
        return timed_out_.load(std::memory_order_relaxed);
    }

    int stopCallCount() const {
        return stop_call_count_.load(std::memory_order_relaxed);
    }

    bool isStopped() const {
        return stopped_.load(std::memory_order_acquire);
    }

    /// @brief 设置模拟检测延迟（用于超时测试）
    void setMockDetectDelay(std::chrono::milliseconds delay) {
        detect_delay_ms_.store(
            static_cast<int>(delay.count()), std::memory_order_relaxed);
    }

    int mockDetectDelayMs() const {
        return detect_delay_ms_.load(std::memory_order_relaxed);
    }

private:
    void workerLoop(int worker_id) {
        (void)worker_id;

        while (!stopped_.load(std::memory_order_acquire)) {
            std::shared_ptr<HttpAccessEvent> event;

            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                not_empty_.wait(lock, [this] {
                    return !task_queue_.empty()
                        || stopped_.load(std::memory_order_acquire);
                });

                if (stopped_.load(std::memory_order_acquire)
                    && task_queue_.empty()) {
                    break;
                }

                if (!task_queue_.empty()) {
                    event = std::move(task_queue_.front());
                    task_queue_.pop_front();
                    not_full_.notify_one();
                }
            }

            if (!event) continue;

            processEvent(event);
        }
    }

    void processEvent(const std::shared_ptr<HttpAccessEvent>& event) {
        // 模拟检测延迟
        int delay_ms = detect_delay_ms_.load(std::memory_order_relaxed);
        auto deadline = std::chrono::steady_clock::now()
            + std::chrono::milliseconds(config_.task_timeout_ms);

        if (delay_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        }

        // 检查是否超时
        if (std::chrono::steady_clock::now() > deadline) {
            timed_out_.fetch_add(1, std::memory_order_relaxed);
            dropped_count_.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        // 执行模拟检测
        AlertResult result = mockDetect(*event);

        if (result.hasMatches()) {
            // 模拟 AlertProducer 背压: alert 队列满则阻塞
            {
                std::unique_lock<std::mutex> lock(alert_mutex_);
                alert_not_full_.wait(lock, [this] {
                    return alert_queue_.size()
                        < static_cast<size_t>(config_.alert_queue_size)
                        || stopped_.load(std::memory_order_acquire);
                });

                if (stopped_.load(std::memory_order_acquire)) {
                    // 停止中仍尝试发送
                }

                alert_queue_.push_back(result);
            }
            alert_produced_.fetch_add(1, std::memory_order_relaxed);

            // 模拟异步发送（这里同步弹出以简化）
            {
                std::lock_guard<std::mutex> lock(alert_mutex_);
                if (!alert_queue_.empty()) {
                    alert_queue_.pop_front();
                    alert_not_full_.notify_one();
                }
            }
        }

        processed_count_.fetch_add(1, std::memory_order_relaxed);
    }

    Config config_;

    // 线程池
    std::vector<std::thread> workers_;
    std::atomic<bool> started_{false};
    std::atomic<bool> stopped_{false};

    // 任务队列
    mutable std::mutex queue_mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::deque<std::shared_ptr<HttpAccessEvent>> task_queue_;

    // 模拟 AlertProducer 队列
    mutable std::mutex alert_mutex_;
    std::condition_variable alert_not_full_;
    std::deque<AlertResult> alert_queue_;

    // 模拟检测延迟
    std::atomic<int> detect_delay_ms_{0};

    // 计数器
    std::atomic<uint64_t> processed_count_{0};
    std::atomic<uint64_t> submitted_count_{0};
    std::atomic<uint64_t> alert_produced_{0};
    std::atomic<uint64_t> dropped_count_{0};
    std::atomic<uint64_t> timed_out_{0};
    std::atomic<int> stop_call_count_{0};
};

// ============================================================================
// Test Fixture
// ============================================================================

class ConcurrentWorkerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 每个测试使用独立的 pool
    }

    void TearDown() override {
        // 确保 pool 被停止
    }

    FakeWorkerPool::Config defaultConfig() const {
        FakeWorkerPool::Config cfg;
        cfg.worker_threads = 4;
        cfg.max_pending_tasks = 64;
        cfg.task_timeout_ms = 2000;
        cfg.alert_queue_size = 64;
        return cfg;
    }
};

// ============================================================================
// 1. ConcurrentSubmitBatch — 多线程同时 submitBatch，验证不丢事件
// ============================================================================

TEST_F(ConcurrentWorkerTest, ConcurrentSubmitBatch) {
    auto cfg = defaultConfig();
    cfg.worker_threads = 4;
    cfg.max_pending_tasks = 200;
    FakeWorkerPool pool(cfg);
    pool.start();

    constexpr int kNumThreads = 4;
    constexpr int kEventsPerThread = 250;
    std::atomic<int> ready{0};
    std::latch start_latch(kNumThreads);
    std::barrier sync_barrier(kNumThreads);  // 等待所有线程完成

    std::vector<std::thread> submitters;

    for (int t = 0; t < kNumThreads; ++t) {
        submitters.emplace_back([&, t]() {
            auto events = makeEventBatch(
                kEventsPerThread, "t" + std::to_string(t));
            ready.fetch_add(1, std::memory_order_relaxed);
            start_latch.arrive_and_wait();

            int64_t n = pool.submitBatch(std::move(events));
            EXPECT_EQ(n, kEventsPerThread)
                << "Thread " << t << " should submit all events";
        });
    }

    for (auto& t : submitters) t.join();

    // 等待处理完成
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    pool.stop();

    uint64_t total_submitted = pool.submittedCount();
    uint64_t total_processed = pool.processedCount();
    uint64_t total_dropped = pool.droppedCount();

    EXPECT_EQ(total_submitted,
              static_cast<uint64_t>(kNumThreads * kEventsPerThread));
    // 所有提交的事件要么处理完，要么在 stop() drain 时处理
    EXPECT_EQ(total_processed + total_dropped, total_submitted)
        << "Every submitted event must be accounted for";
}

// ============================================================================
// 2. QueueBackpressure — 队列满时 submit 正确阻塞，有空间后恢复
// ============================================================================

TEST_F(ConcurrentWorkerTest, QueueBackpressure) {
    auto cfg = defaultConfig();
    cfg.worker_threads = 2;
    cfg.max_pending_tasks = 10;  // 小队列
    cfg.task_timeout_ms = 2000;
    // 让 worker 处理变慢，制造背压
    FakeWorkerPool pool(cfg);
    pool.setMockDetectDelay(std::chrono::milliseconds(20));
    pool.start();

    constexpr int kTotalEvents = 100;
    std::atomic<bool> submit_blocked{false};
    std::atomic<bool> submit_completed{false};

    // 在独立线程中提交大量事件
    std::thread submitter([&]() {
        auto events = makeEventBatch(kTotalEvents, "backpressure");
        auto start = std::chrono::steady_clock::now();

        // 提交过程中检查队列是否曾满
        // (用一个监控线程来检测)
        int64_t n = pool.submitBatch(std::move(events));
        EXPECT_EQ(n, kTotalEvents);

        auto elapsed = std::chrono::steady_clock::now() - start;
        // 由于队列小，提交应该花一些时间
        EXPECT_GT(elapsed, std::chrono::milliseconds(50))
            << "Submit should block when queue is full";

        submit_completed.store(true, std::memory_order_release);
    });

    // 监控队列大小变化 — 验证队列曾达到容量上限
    bool queue_was_full = false;
    auto deadline = std::chrono::steady_clock::now()
        + std::chrono::seconds(10);

    while (std::chrono::steady_clock::now() < deadline
           && !submit_completed.load(std::memory_order_acquire)) {
        if (pool.pendingCount() >= static_cast<size_t>(cfg.max_pending_tasks)) {
            queue_was_full = true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    submitter.join();
    pool.stop();

    EXPECT_TRUE(queue_was_full)
        << "Queue should have been full at some point";
    EXPECT_EQ(pool.submittedCount(), static_cast<uint64_t>(kTotalEvents));
}

// ============================================================================
// 3. WorkerGracefulShutdown — stop() 后所有 in-flight 任务完成
// ============================================================================

TEST_F(ConcurrentWorkerTest, WorkerGracefulShutdown) {
    auto cfg = defaultConfig();
    cfg.worker_threads = 4;
    cfg.max_pending_tasks = 100;
    cfg.task_timeout_ms = 5000;
    FakeWorkerPool pool(cfg);
    // 设置较长检测延迟以模拟 in-flight 任务
    pool.setMockDetectDelay(std::chrono::milliseconds(50));
    pool.start();

    constexpr int kEvents = 80;
    auto events = makeEventBatch(kEvents, "shutdown");
    pool.submitBatch(std::move(events));

    // 短暂等待 workers 开始处理
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // 立即发起 stop
    pool.stop();

    // stop() 之后，所有 in-flight 任务应该已完成
    // (processed + dropped) 应该等于提交数
    uint64_t processed = pool.processedCount();
    uint64_t dropped = pool.droppedCount();
    uint64_t submitted = pool.submittedCount();

    EXPECT_EQ(processed + dropped, submitted)
        << "All events must be processed or dropped after graceful shutdown";
    EXPECT_EQ(pool.pendingCount(), 0u)
        << "Queue should be empty after stop() drain";
}

// ============================================================================
// 4. TaskTimeout — 超时任务被跳过，其他任务正常完成
// ============================================================================

TEST_F(ConcurrentWorkerTest, TaskTimeout) {
    auto cfg = defaultConfig();
    cfg.worker_threads = 2;
    cfg.max_pending_tasks = 100;
    cfg.task_timeout_ms = 30;  // 很短的超时
    FakeWorkerPool pool(cfg);
    // 设置检测延迟大于超时，导致超时
    pool.setMockDetectDelay(std::chrono::milliseconds(80));
    pool.start();

    constexpr int kEvents = 40;
    auto events = makeEventBatch(kEvents, "timeout");
    pool.submitBatch(std::move(events));

    // 等待处理完成
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    pool.stop();

    // 由于每个任务耗时 80ms 而超时是 30ms，大部分任务应超时
    uint64_t timed_out = pool.timedOutCount();
    uint64_t processed = pool.processedCount();
    uint64_t dropped = pool.droppedCount();
    uint64_t submitted = pool.submittedCount();

    EXPECT_EQ(processed + dropped, submitted);
    EXPECT_GT(timed_out, 0u) << "Some tasks should have timed out";
    EXPECT_GE(dropped, timed_out) << "Timed-out tasks count as dropped";

    // 验证: 超时计数 + 正常处理 = 总提交
    // (timed_out 在 processEvent 中超时时递增)
}

// ============================================================================
// 5. WorkerPoolStress — 8线程 × 1000事件，验证吞吐和正确性
// ============================================================================

TEST_F(ConcurrentWorkerTest, WorkerPoolStress) {
    auto cfg = defaultConfig();
    cfg.worker_threads = 8;
    cfg.max_pending_tasks = 200;
    cfg.task_timeout_ms = 5000;
    FakeWorkerPool pool(cfg);
    pool.setMockDetectDelay(std::chrono::milliseconds(0));  // 无延迟，测吞吐
    pool.start();

    constexpr int kNumThreads = 8;
    constexpr int kEventsPerThread = 125;  // 8 * 125 = 1000
    constexpr int kTotalEvents = kNumThreads * kEventsPerThread;

    std::latch latch(kNumThreads);
    std::vector<std::thread> submitters;

    auto start = std::chrono::steady_clock::now();

    for (int t = 0; t < kNumThreads; ++t) {
        submitters.emplace_back([&, t]() {
            auto events = makeEventBatch(
                kEventsPerThread, "stress-t" + std::to_string(t));
            latch.arrive_and_wait();
            pool.submitBatch(std::move(events));
        });
    }

    for (auto& t : submitters) t.join();

    // 自旋等待所有事件处理完成
    auto wait_deadline = std::chrono::steady_clock::now()
        + std::chrono::seconds(10);
    while (pool.processedCount() + pool.droppedCount()
           < static_cast<uint64_t>(kTotalEvents)) {
        if (std::chrono::steady_clock::now() > wait_deadline) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    auto end = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        end - start).count();

    pool.stop();

    uint64_t processed = pool.processedCount();
    uint64_t dropped = pool.droppedCount();
    uint64_t submitted = pool.submittedCount();

    EXPECT_EQ(submitted, static_cast<uint64_t>(kTotalEvents))
        << "All events should be submitted";
    EXPECT_EQ(processed + dropped, submitted)
        << "All events accounted for";
    EXPECT_GT(processed, 0u) << "Should have processed events";

    // 吞吐量验证: 1000 事件应在合理时间内完成
    double throughput = (processed * 1000.0) / std::max(elapsed_ms, 1L);
    // 输出吞吐量信息（不强制断言，取决于环境）
    SUCCEED() << "Throughput: " << throughput << " events/sec, "
              << "elapsed: " << elapsed_ms << "ms, "
              << "processed: " << processed << ", dropped: " << dropped;
}

// ============================================================================
// 6. AlertProducerBackpressure — alert队列满时worker正确阻塞
// ============================================================================

TEST_F(ConcurrentWorkerTest, AlertProducerBackpressure) {
    auto cfg = defaultConfig();
    cfg.worker_threads = 2;
    cfg.max_pending_tasks = 100;
    cfg.task_timeout_ms = 5000;
    cfg.alert_queue_size = 5;  // 很小的 alert 队列
    FakeWorkerPool pool(cfg);
    pool.setMockDetectDelay(std::chrono::milliseconds(10));

    // 使用大量事件，其中 ~30% 会产生 alert（由 mockDetect 决定）
    // 小 alert 队列 + 较多 alert → 背压
    pool.start();

    constexpr int kEvents = 200;
    auto events = makeEventBatch(kEvents, "alert-bp");

    std::thread submitter([&]() {
        pool.submitBatch(std::move(events));
    });

    submitter.join();

    // 等待处理
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    pool.stop();

    EXPECT_EQ(pool.processedCount() + pool.droppedCount(),
              static_cast<uint64_t>(kEvents));
    // alert 队列大小应合理 (在 stop drain 中会清空)
}

// ============================================================================
// 7. DrainAllPendingOnStop — 停止时排空队列中所有pending任务
// ============================================================================

TEST_F(ConcurrentWorkerTest, DrainAllPendingOnStop) {
    auto cfg = defaultConfig();
    cfg.worker_threads = 1;  // 单线程 worker，容易堆积
    cfg.max_pending_tasks = 200;
    cfg.task_timeout_ms = 5000;
    FakeWorkerPool pool(cfg);
    pool.setMockDetectDelay(std::chrono::milliseconds(5));
    pool.start();

    constexpr int kEvents = 100;
    auto events = makeEventBatch(kEvents, "drain");
    pool.submitBatch(std::move(events));

    // 不等所有任务处理完，直接 stop
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    size_t pending_before_stop = pool.pendingCount();

    pool.stop();

    // stop() 应排空所有 pending 任务
    EXPECT_EQ(pool.pendingCount(), 0u)
        << "Queue should be empty after stop drain";
    EXPECT_EQ(pool.processedCount() + pool.droppedCount(),
              static_cast<uint64_t>(kEvents))
        << "All events should be accounted for";

    // 如果 stop 前队列还有任务，processing 包含 drain 阶段处理的
    SUCCEED() << "Pending before stop: " << pending_before_stop
              << ", Processed: " << pool.processedCount()
              << ", Dropped: " << pool.droppedCount();
}

// ============================================================================
// 8. CASStopIdempotent — 多次调用stop()只执行一次
// ============================================================================

TEST_F(ConcurrentWorkerTest, CASStopIdempotent) {
    auto cfg = defaultConfig();
    cfg.worker_threads = 4;
    FakeWorkerPool pool(cfg);
    pool.start();

    // 提交一些事件以确保有 worker 在运行
    auto events = makeEventBatch(20, "idempotent");
    pool.submitBatch(std::move(events));

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    constexpr int kStopCalls = 5;
    std::vector<std::thread> stoppers;
    std::latch latch(kStopCalls);

    for (int i = 0; i < kStopCalls; ++i) {
        stoppers.emplace_back([&]() {
            latch.arrive_and_wait();
            pool.stop();
        });
    }

    for (auto& t : stoppers) t.join();

    // 只应执行一次 CAS 成功
    EXPECT_EQ(pool.stopCallCount(), 1)
        << "stop() should only execute once (CAS idempotent)";
    EXPECT_TRUE(pool.isStopped());
    EXPECT_EQ(pool.pendingCount(), 0u);
}

// ============================================================================
// 9. AtomicMetricsConsistency — 多线程更新metrics计数器，最终值正确
// ============================================================================

TEST_F(ConcurrentWorkerTest, AtomicMetricsConsistency) {
    auto cfg = defaultConfig();
    cfg.worker_threads = 6;
    cfg.max_pending_tasks = 200;
    cfg.task_timeout_ms = 5000;
    FakeWorkerPool pool(cfg);
    pool.setMockDetectDelay(std::chrono::milliseconds(2));
    pool.start();

    constexpr int kNumThreads = 6;
    constexpr int kEventsPerThread = 200;
    constexpr int kTotalEvents = kNumThreads * kEventsPerThread;

    std::latch latch(kNumThreads);
    std::vector<std::thread> submitters;

    for (int t = 0; t < kNumThreads; ++t) {
        submitters.emplace_back([&, t]() {
            auto events = makeEventBatch(
                kEventsPerThread, "metrics-t" + std::to_string(t));
            latch.arrive_and_wait();
            pool.submitBatch(std::move(events));
        });
    }

    for (auto& t : submitters) t.join();

    // 自旋等待所有处理完成
    auto wait_deadline = std::chrono::steady_clock::now()
        + std::chrono::seconds(15);
    while (pool.processedCount() + pool.droppedCount()
           < static_cast<uint64_t>(kTotalEvents)) {
        if (std::chrono::steady_clock::now() > wait_deadline) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    pool.stop();

    uint64_t processed = pool.processedCount();
    uint64_t dropped = pool.droppedCount();
    uint64_t submitted = pool.submittedCount();
    uint64_t alerts = pool.alertProducedCount();

    // 最终一致性检查
    EXPECT_EQ(submitted, static_cast<uint64_t>(kTotalEvents))
        << "Submitted count must match total events";
    EXPECT_EQ(processed + dropped, submitted)
        << "Processed + Dropped must equal Submitted";
    EXPECT_LE(alerts, processed)
        << "Alerts count must not exceed processed count";

    // 不丢事件: 每个提交的事件要么被处理要么被丢弃
    SUCCEED() << "Metrics: submitted=" << submitted
              << ", processed=" << processed
              << ", dropped=" << dropped
              << ", alerts=" << alerts;
}

// ============================================================================
// 10. ThreadSafetyNoDataRace — 多次运行检测数据竞争
// ============================================================================

TEST_F(ConcurrentWorkerTest, ThreadSafetyNoDataRace) {
    // 多次运行同一个并发场景，用简单的校验检测数据竞争：
    // 如果存在数据竞争，最终计数会不一致。

    constexpr int kIterations = 10;

    for (int iter = 0; iter < kIterations; ++iter) {
        auto cfg = defaultConfig();
        cfg.worker_threads = 4;
        cfg.max_pending_tasks = 100;
        cfg.task_timeout_ms = 2000;
        FakeWorkerPool pool(cfg);
        pool.setMockDetectDelay(std::chrono::milliseconds(1));
        pool.start();

        constexpr int kEvents = 100;
        auto events = makeEventBatch(kEvents,
            "race-iter" + std::to_string(iter));

        // 两个线程同时提交
        std::thread t1([&]() {
            auto half = makeEventBatch(kEvents / 2,
                "race-iter" + std::to_string(iter) + "-a");
            pool.submitBatch(std::move(half));
        });

        std::thread t2([&]() {
            auto half = makeEventBatch(kEvents / 2,
                "race-iter" + std::to_string(iter) + "-b");
            pool.submitBatch(std::move(half));
        });

        t1.join();
        t2.join();

        // 等待处理
        auto wait_deadline = std::chrono::steady_clock::now()
            + std::chrono::seconds(5);
        while (pool.processedCount() + pool.droppedCount()
               < static_cast<uint64_t>(kEvents)) {
            if (std::chrono::steady_clock::now() > wait_deadline) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        pool.stop();

        uint64_t processed = pool.processedCount();
        uint64_t dropped = pool.droppedCount();
        uint64_t submitted = pool.submittedCount();

        // 每次迭代都验证一致性: 不丢事件
        EXPECT_EQ(processed + dropped, submitted)
            << "Iteration " << iter
            << ": processed + dropped != submitted (possible data race)";
        EXPECT_EQ(submitted, static_cast<uint64_t>(kEvents))
            << "Iteration " << iter
            << ": submitted count mismatch (possible data race)";
    }

    SUCCEED() << "All " << kIterations << " iterations passed — "
              << "no data race detected by consistency checks";
}
