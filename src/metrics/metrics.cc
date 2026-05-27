/**
 * @file metrics.cc
 * @brief Metrics 单例实现
 */

#include "metrics/metrics.h"

#include <sstream>
#include <string>

namespace wge::kafka::metrics {

// ============================================================================
// 单例
// ============================================================================

Metrics& Metrics::instance() noexcept {
    static Metrics instance;
    return instance;
}

// ============================================================================
// Histogram
// ============================================================================

void Metrics::recordDetectionDuration(uint64_t duration_us) {
    // 对每个桶: 若 duration_us <= 桶边界, 该桶计数+1
    for (size_t i = 0; i < kNumBuckets; ++i) {
        if (duration_us <= kBucketBoundaries[i]) {
            histogram_buckets_[i].fetch_add(1, std::memory_order_relaxed);
        }
    }
    histogram_count_.fetch_add(1, std::memory_order_relaxed);
    histogram_sum_us_.fetch_add(duration_us, std::memory_order_relaxed);
}

uint64_t Metrics::histogramBucketCount(size_t bucket_index) const {
    if (bucket_index >= kNumBuckets) {
        return 0;
    }
    return histogram_buckets_[bucket_index].load(std::memory_order_relaxed);
}

uint64_t Metrics::histogramCount() const {
    return histogram_count_.load(std::memory_order_relaxed);
}

uint64_t Metrics::histogramSum() const {
    return histogram_sum_us_.load(std::memory_order_relaxed);
}

// ============================================================================
// Prometheus 文本格式输出
// ============================================================================

std::string Metrics::toPrometheusText() const {
    std::ostringstream os;

    // ---- 辅助宏: 输出 counter ----
    auto writeCounter = [&os](const char* name, const char* help,
                               uint64_t value) {
        os << "# HELP " << name << " " << help << "\n";
        os << "# TYPE " << name << " counter\n";
        os << name << " " << value << "\n\n";
    };

    // ---- 辅助宏: 输出 gauge ----
    auto writeGauge = [&os](const char* name, const char* help,
                             int64_t value) {
        os << "# HELP " << name << " " << help << "\n";
        os << "# TYPE " << name << " gauge\n";
        os << name << " " << value << "\n\n";
    };

    // ---- 计数器 ----

    writeCounter("wge_events_consumed_total",
                 "Total events consumed from Kafka.",
                 events_consumed.load(std::memory_order_relaxed));

    writeCounter("wge_events_processed_total",
                 "Total events successfully processed.",
                 events_processed.load(std::memory_order_relaxed));

    writeCounter("wge_events_dropped_total",
                 "Total events dropped (parse failure, timeout, etc.).",
                 events_dropped.load(std::memory_order_relaxed));

    writeCounter("wge_alerts_produced_total",
                 "Total alert events produced to Kafka.",
                 alerts_produced.load(std::memory_order_relaxed));

    writeCounter("wge_kafka_produce_errors_total",
                 "Total Kafka produce errors.",
                 kafka_produce_errors.load(std::memory_order_relaxed));

    writeCounter("wge_rule_evaluations_total",
                 "Total rule evaluations performed.",
                 rule_evaluations.load(std::memory_order_relaxed));

    writeCounter("wge_rule_matches_total",
                 "Total rule matches (positive detections).",
                 rule_matches.load(std::memory_order_relaxed));

    // ---- Gauge ----

    writeGauge("wge_consumer_lag",
               "Current consumer lag (unprocessed messages).",
               consumer_lag.load(std::memory_order_relaxed));

    writeGauge("wge_worker_pool_pending",
               "Pending tasks in worker pool.",
               worker_pool_pending.load(std::memory_order_relaxed));

    writeGauge("wge_worker_pool_active",
               "Active worker threads in pool.",
               worker_pool_active.load(std::memory_order_relaxed));

    writeGauge("wge_rules_loaded",
               "Number of currently loaded WGE rules.",
               rules_loaded.load(std::memory_order_relaxed));

    writeGauge("wge_wal_pending",
               "Pending write-ahead log entries.",
               wal_pending.load(std::memory_order_relaxed));

    // ---- Histogram: detection_duration_us ----

    const char* hist_name = "wge_detection_duration_us";
    const char* hist_help = "Detection duration distribution in microseconds.";

    os << "# HELP " << hist_name << " " << hist_help << "\n";
    os << "# TYPE " << hist_name << " histogram\n";

    uint64_t cumulative = 0;
    for (size_t i = 0; i < kNumBuckets; ++i) {
        cumulative += histogram_buckets_[i].load(std::memory_order_relaxed);
        double bound = static_cast<double>(kBucketBoundaries[i]);
        os << hist_name << "_bucket{le=\"" << std::fixed << bound << "\"} "
           << cumulative << "\n";
    }
    // +Inf bucket
    os << hist_name << "_bucket{le=\"+Inf\"} "
       << histogram_count_.load(std::memory_order_relaxed) << "\n";

    // Sum 和 Count
    os << hist_name << "_sum "
       << histogram_sum_us_.load(std::memory_order_relaxed) << "\n";
    os << hist_name << "_count "
       << histogram_count_.load(std::memory_order_relaxed) << "\n";

    return os.str();
}

// ============================================================================
// Reset
// ============================================================================

void Metrics::reset() {
    // 计数器
    events_consumed.store(0, std::memory_order_relaxed);
    events_processed.store(0, std::memory_order_relaxed);
    events_dropped.store(0, std::memory_order_relaxed);
    alerts_produced.store(0, std::memory_order_relaxed);
    kafka_produce_errors.store(0, std::memory_order_relaxed);
    rule_evaluations.store(0, std::memory_order_relaxed);
    rule_matches.store(0, std::memory_order_relaxed);

    // Gauge
    consumer_lag.store(0, std::memory_order_relaxed);
    worker_pool_pending.store(0, std::memory_order_relaxed);
    worker_pool_active.store(0, std::memory_order_relaxed);
    rules_loaded.store(0, std::memory_order_relaxed);
    wal_pending.store(0, std::memory_order_relaxed);

    // Histogram
    for (size_t i = 0; i < kNumBuckets; ++i) {
        histogram_buckets_[i].store(0, std::memory_order_relaxed);
    }
    histogram_count_.store(0, std::memory_order_relaxed);
    histogram_sum_us_.store(0, std::memory_order_relaxed);
}

}  // namespace wge::kafka::metrics
