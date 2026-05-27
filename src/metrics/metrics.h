#pragma once

/**
 * @file metrics.h
 * @brief 进程内 Metrics 汇总（Prometheus 兼容）
 *
 * Metrics 类提供进程内原子计数器、Gauge 和 Histogram 的汇总能力。
 * 不依赖外部 Prometheus 库，纯 atomic + 手动实现。
 * 输出 Prometheus 兼容的文本格式，可直接通过 HTTP /metrics 端点返回。
 *
 * 线程安全: 所有读写操作均使用 atomic，线程安全。
 *           使用 memory_order_relaxed 优化性能。
 *
 * 单例模式: Metrics::instance() 返回全局唯一实例。
 */

#include <array>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>

namespace wge::kafka::metrics {

// ============================================================================
// Metrics 单例
// ============================================================================

/**
 * @brief 进程内 Metrics 汇总器
 *
 * 使用 atomic 变量维护所有计数器、Gauge 和 Histogram。
 * Histogram 使用手动对数分桶实现。
 *
 * 使用示例:
 * @code
 *   Metrics::instance().incrementEventsConsumed();
 *   Metrics::instance().recordDetectionDuration(150);  // 150 us
 *   std::string prometheus_text = Metrics::instance().toPrometheusText();
 *   // 在 HTTP handler 中返回 prometheus_text
 * @endcode
 *
 * @note 所有变量均为 atomic，读取使用 memory_order_relaxed 以提升性能。
 * @note reset() 应仅用于测试。
 */
class Metrics {
public:
    // ---- 计数器 ----

    /// @brief 已消费的事件总数
    std::atomic<uint64_t> events_consumed{0};

    /// @brief 成功处理的事件总数
    std::atomic<uint64_t> events_processed{0};

    /// @brief 丢弃的事件总数（解析失败、超时等）
    std::atomic<uint64_t> events_dropped{0};

    /// @brief 已生产的告警总数
    std::atomic<uint64_t> alerts_produced{0};

    /// @brief Kafka produce 错误总数
    std::atomic<uint64_t> kafka_produce_errors{0};

    /// @brief 规则评估次数
    std::atomic<uint64_t> rule_evaluations{0};

    /// @brief 规则匹配次数
    std::atomic<uint64_t> rule_matches{0};

    // ---- Gauge ----

    /// @brief Consumer lag（当前未消费的消息数）
    std::atomic<int64_t> consumer_lag{0};

    /// @brief Worker pool 待处理任务数
    std::atomic<int64_t> worker_pool_pending{0};

    /// @brief Worker pool 活跃线程数
    std::atomic<int64_t> worker_pool_active{0};

    /// @brief 已加载的规则数
    std::atomic<int64_t> rules_loaded{0};

    /// @brief WAL 待写入消息数
    std::atomic<int64_t> wal_pending{0};

    // ---- Histogram: detection_duration_us ----

    /**
     * @brief 对数分桶边界 (微秒)
     *
     * 桶: 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000 us
     * 共 9 个桶。
     */
    static constexpr size_t kNumBuckets = 9;
    static constexpr std::array<uint64_t, kNumBuckets> kBucketBoundaries = {
        10, 50, 100, 500, 1'000, 5'000, 10'000, 50'000, 100'000
    };

    /**
     * @brief 获取单例实例
     *
     * @return Metrics& 全局唯一 Metrics 实例
     * @note 线程安全: C++11+ 保证静态局部变量初始化线程安全。
     */
    static Metrics& instance() noexcept;

    /**
     * @brief 记录检测耗时
     *
     * 遍历所有桶边界，对 <= duration_us 的桶计数+1，
     * 累计总次数 sum 和总耗时 sum_us。
     *
     * @param duration_us 检测耗时（微秒）
     * @note 线程安全
     */
    void recordDetectionDuration(uint64_t duration_us);

    /**
     * @brief 获取特定桶的计数
     *
     * @param bucket_index 桶索引 (0..kNumBuckets-1)
     * @return uint64_t 该桶的累计计数
     */
    [[nodiscard]] uint64_t histogramBucketCount(size_t bucket_index) const;

    /**
     * @brief 获取 histogram 累计次数
     *
     * @return uint64_t 累计记录次数
     */
    [[nodiscard]] uint64_t histogramCount() const;

    /**
     * @brief 获取 histogram 累计总耗时 (us)
     *
     * @return uint64_t 总耗时
     */
    [[nodiscard]] uint64_t histogramSum() const;

    // ---- 便捷方法 ----

    /// @brief events_consumed += 1
    void incrementEventsConsumed() noexcept {
        events_consumed.fetch_add(1, std::memory_order_relaxed);
    }

    /// @brief events_processed += 1
    void incrementEventsProcessed() noexcept {
        events_processed.fetch_add(1, std::memory_order_relaxed);
    }

    /// @brief events_dropped += 1
    void incrementEventsDropped() noexcept {
        events_dropped.fetch_add(1, std::memory_order_relaxed);
    }

    /// @brief alerts_produced += 1
    void incrementAlertsProduced() noexcept {
        alerts_produced.fetch_add(1, std::memory_order_relaxed);
    }

    /// @brief kafka_produce_errors += 1
    void incrementKafkaProduceErrors() noexcept {
        kafka_produce_errors.fetch_add(1, std::memory_order_relaxed);
    }

    /// @brief rule_evaluations += 1
    void incrementRuleEvaluations() noexcept {
        rule_evaluations.fetch_add(1, std::memory_order_relaxed);
    }

    /// @brief rule_matches += 1
    void incrementRuleMatches() noexcept {
        rule_matches.fetch_add(1, std::memory_order_relaxed);
    }

    /**
     * @brief 批量增加 events_consumed
     *
     * @param count 增加量
     */
    void addEventsConsumed(uint64_t count) noexcept {
        events_consumed.fetch_add(count, std::memory_order_relaxed);
    }

    /**
     * @brief 批量增加 events_processed
     */
    void addEventsProcessed(uint64_t count) noexcept {
        events_processed.fetch_add(count, std::memory_order_relaxed);
    }

    /**
     * @brief 批量增加 events_dropped
     */
    void addEventsDropped(uint64_t count) noexcept {
        events_dropped.fetch_add(count, std::memory_order_relaxed);
    }

    /**
     * @brief 批量增加 alerts_produced
     */
    void addAlertsProduced(uint64_t count) noexcept {
        alerts_produced.fetch_add(count, std::memory_order_relaxed);
    }

    /**
     * @brief 批量增加 rule_evaluations
     */
    void addRuleEvaluations(uint64_t count) noexcept {
        rule_evaluations.fetch_add(count, std::memory_order_relaxed);
    }

    /**
     * @brief 批量增加 rule_matches
     */
    void addRuleMatches(uint64_t count) noexcept {
        rule_matches.fetch_add(count, std::memory_order_relaxed);
    }

    // ---- Prometheus 输出 ----

    /**
     * @brief 输出 Prometheus 兼容的文本格式
     *
     * 生成标准 Prometheus exposition format 文本，
     * 包含所有计数器、Gauge、Histogram 以及类型注释。
     *
     * @return std::string Prometheus 文本格式的 metrics 数据
     * @note 线程安全: 所有读取均为 atomic relaxed load。
     *
     * 示例输出:
     * @code
     * # HELP wge_events_consumed Total events consumed from Kafka
     * # TYPE wge_events_consumed counter
     * wge_events_consumed 12345
     * ...
     * @endcode
     */
    [[nodiscard]] std::string toPrometheusText() const;

    /**
     * @brief 重置所有 metrics（仅用于测试）
     *
     * 将所有计数器、Gauge 和 Histogram 桶置零。
     *
     * @note 非原子操作: 重置期间其他线程可能读到中间状态。
     *       仅应在测试环境的单线程上下文中调用。
     */
    void reset();

private:
    /**
     * @brief 私有构造函数（单例）
     */
    Metrics() = default;

    /**
     * @brief 析构函数
     */
    ~Metrics() = default;

    // 禁止拷贝和移动
    Metrics(const Metrics&) = delete;
    Metrics& operator=(const Metrics&) = delete;
    Metrics(Metrics&&) = delete;
    Metrics& operator=(Metrics&&) = delete;

    // ---- Histogram 存储 ----

    /// @brief 各桶累计计数
    std::array<std::atomic<uint64_t>, kNumBuckets> histogram_buckets_{};

    /// @brief Histogram 累计总次数
    std::atomic<uint64_t> histogram_count_{0};

    /// @brief Histogram 累计总耗时 (微秒)
    std::atomic<uint64_t> histogram_sum_us_{0};
};

}  // namespace wge::kafka::metrics
