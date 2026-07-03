#pragma once

/**
 * @file akto_metrics.h
 * @brief Akto Adapter Prometheus 指标定义
 *
 * 设计报告第 9.6 节要求暴露 4 个 Prometheus 指标:
 *   - wge_adapter_alerts_consumed       (Adapter 消费的告警总数)
 *   - wge_adapter_akto_events_produced (成功注入 Akto 的事件总数)
 *   - wge_adapter_rate_limited_drops    (IP 限流丢弃数)
 *   - wge_adapter_collection_id_zero_drops (collection_id=0 丢弃数)
 *
 * 当 prometheus-cpp 不可用时，指标退化为原子计数器，仅通过日志输出。
 */

#include <atomic>
#include <cstdint>

#ifdef WGE_DETECTOR_HAS_PROMETHEUS
#include <prometheus/counter.h>
#include <prometheus/registry.h>
#include <prometheus/exposer.h>
#endif

namespace wge::akto {

/**
 * @brief Akto Adapter 指标单例
 *
 * 线程安全: 所有计数器使用 std::atomic，Prometheus Counter 内部也是线程安全的。
 */
class AktoMetrics {
public:
    /// @brief 获取单例 (C++11 Meyers Singleton, 线程安全)
    static AktoMetrics& instance() {
        static AktoMetrics inst;
        return inst;
    }

    // ── 指标递增接口 (线程安全) ──

    /// @brief 告警消费计数 +1
    void incAlertsConsumed() {
        alerts_consumed_.fetch_add(1, std::memory_order_relaxed);
#ifdef WGE_DETECTOR_HAS_PROMETHEUS
        if (prom_alerts_consumed_) prom_alerts_consumed_->Increment();
#endif
    }

    /// @brief Akto 事件产出计数 +1
    void incAktoEventsProduced() {
        akto_events_produced_.fetch_add(1, std::memory_order_relaxed);
#ifdef WGE_DETECTOR_HAS_PROMETHEUS
        if (prom_akto_events_produced_) prom_akto_events_produced_->Increment();
#endif
    }

    /// @brief IP 限流丢弃计数 +1
    void incRateLimitedDrops() {
        rate_limited_drops_.fetch_add(1, std::memory_order_relaxed);
#ifdef WGE_DETECTOR_HAS_PROMETHEUS
        if (prom_rate_limited_drops_) prom_rate_limited_drops_->Increment();
#endif
    }

    /// @brief collection_id=0 丢弃计数 +1
    void incCollectionIdZeroDrops() {
        collection_id_zero_drops_.fetch_add(1, std::memory_order_relaxed);
#ifdef WGE_DETECTOR_HAS_PROMETHEUS
        if (prom_collection_id_zero_drops_) prom_collection_id_zero_drops_->Increment();
#endif
    }

    // ── 指标快照 (用于日志输出) ──

    uint64_t alertsConsumed() const { return alerts_consumed_.load(std::memory_order_relaxed); }
    uint64_t aktoEventsProduced() const { return akto_events_produced_.load(std::memory_order_relaxed); }
    uint64_t rateLimitedDrops() const { return rate_limited_drops_.load(std::memory_order_relaxed); }
    uint64_t collectionIdZeroDrops() const { return collection_id_zero_drops_.load(std::memory_order_relaxed); }

private:
    AktoMetrics() = default;
    ~AktoMetrics() = default;
    AktoMetrics(const AktoMetrics&) = delete;
    AktoMetrics& operator=(const AktoMetrics&) = delete;

    // 原子计数器 (始终可用，即使无 prometheus-cpp)
    std::atomic<uint64_t> alerts_consumed_{0};
    std::atomic<uint64_t> akto_events_produced_{0};
    std::atomic<uint64_t> rate_limited_drops_{0};
    std::atomic<uint64_t> collection_id_zero_drops_{0};

#ifdef WGE_DETECTOR_HAS_PROMETHEUS
    // Prometheus Counter 指针 (仅当 prometheus-cpp 可用时)
    prometheus::Counter* prom_alerts_consumed_{nullptr};
    prometheus::Counter* prom_akto_events_produced_{nullptr};
    prometheus::Counter* prom_rate_limited_drops_{nullptr};
    prometheus::Counter* prom_collection_id_zero_drops_{nullptr};
#endif
};

}  // namespace wge::akto
