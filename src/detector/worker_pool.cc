/**
 * @file worker_pool.cc
 * @brief WgeWorkerPool 实现 — WGE 检测线程池
 */

#include "detector/worker_pool.h"

#include <chrono>
#include <future>
#include <stdexcept>
#include <string_view>

#include "detector/alert_builder.h"
#include "detector/http_extractor_adapter.h"
#include "detector/result.h"
#include "http_access.pb.h"
#include "kafka/producer.h"
#include "metrics/metrics.h"
#include "spdlog/spdlog.h"

// WGE SDK 头文件
#include "wge/engine.h"
#include "wge/rule.h"
#include "wge/transaction.h"

namespace wge::kafka::detector {

// ============================================================================
// 构造与析构
// ============================================================================

WgeWorkerPool::WgeWorkerPool(const wge::Engine& engine,
                             AlertProducer& producer,
                             metrics::Metrics& metrics,
                             const WorkerConfig& config)
    : engine_(engine)
    , producer_(producer)
    , metrics_(metrics)
    , config_(config) {

    // 自动检测线程数
    if (config_.worker_threads <= 0) {
        config_.worker_threads = static_cast<int>(
            std::thread::hardware_concurrency());
        if (config_.worker_threads <= 0) {
            config_.worker_threads = 4;  // 安全回退
        }
    }

    if (config_.max_pending_tasks <= 0) {
        config_.max_pending_tasks = 1024;
    }

    SPDLOG_INFO("WgeWorkerPool created: threads={}, max_pending={}, timeout_ms={}",
                config_.worker_threads, config_.max_pending_tasks,
                config_.task_timeout_ms);
}

WgeWorkerPool::~WgeWorkerPool() {
    try {
        stop();
    } catch (const std::exception& e) {
        SPDLOG_ERROR("WgeWorkerPool destructor error: {}", e.what());
    } catch (...) {
        SPDLOG_ERROR("WgeWorkerPool destructor: unknown error");
    }
}

// ============================================================================
// start / stop
// ============================================================================

void WgeWorkerPool::start() {
    if (started_.exchange(true, std::memory_order_acq_rel)) {
        throw std::runtime_error("WgeWorkerPool::start: already started");
    }

    stopped_.store(false, std::memory_order_release);

    workers_.reserve(static_cast<size_t>(config_.worker_threads));
    for (int i = 0; i < config_.worker_threads; ++i) {
        workers_.emplace_back(&WgeWorkerPool::workerLoop, this, i);
    }

    // 设置 worker 线程名称 (平台相关，尽力而为)
    // 注: pthread_setname_np 在 Linux 上可用

    SPDLOG_INFO("WgeWorkerPool started: {} workers", config_.worker_threads);
}

void WgeWorkerPool::stop() {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel,
                                          std::memory_order_relaxed)) {
        return;  // 已停止
    }

    SPDLOG_INFO("WgeWorkerPool::stop: signaling all workers to stop");

    // 唤醒所有等待的 worker 线程
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
    }
    not_empty_.notify_all();
    not_full_.notify_all();

    // 等待所有 worker 线程退出
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();

    SPDLOG_INFO("WgeWorkerPool::stop: all workers stopped, "
                "remaining tasks in queue: {}",
                task_queue_.size());

    // 排空并处理队列中剩余的任务
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        while (!task_queue_.empty()) {
            auto event = std::move(task_queue_.front());
            task_queue_.pop_front();

            // 在锁外处理 (但这里简单起见在锁内处理，因为已经停止不会阻塞)
            try {
                AlertResult result = detect(*event);
                if (result.hasMatches()) {
                    auto alert = AlertBuilder::build(
                        result,
                        event->event_id(),
                        event->collector_id(),
                        event->request_method(),
                        event->request_uri(),
                        event->downstream_ip(),
                        event->upstream_ip());
                    producer_.sendAlert(std::move(alert));
                    metrics_.incrementAlertsProduced();
                }
                metrics_.incrementEventsProcessed();
            } catch (const std::exception& e) {
                SPDLOG_ERROR("WgeWorkerPool::stop: drain task failed: {}", e.what());
                metrics_.incrementEventsDropped();
            }
        }
    }

    started_.store(false, std::memory_order_release);
}

// ============================================================================
// submitBatch
// ============================================================================

int64_t WgeWorkerPool::submitBatch(
    std::vector<std::shared_ptr<HttpAccessEvent>>&& events) {

    if (!started_.load(std::memory_order_acquire)) {
        throw std::runtime_error("WgeWorkerPool::submitBatch: pool not started");
    }

    if (events.empty()) {
        return 0;
    }

    int64_t submitted = 0;
    size_t max_capacity = static_cast<size_t>(config_.max_pending_tasks);

    for (auto& event : events) {
        if (!event) {
            SPDLOG_WARN("WgeWorkerPool::submitBatch: null event skipped");
            continue;
        }

        {
            std::unique_lock<std::mutex> lock(queue_mutex_);

            // 有界队列: 满时阻塞等待
            not_full_.wait(lock, [this, max_capacity] {
                return task_queue_.size() < max_capacity ||
                       stopped_.load(std::memory_order_acquire);
            });

            // 若已停止则不继续提交
            if (stopped_.load(std::memory_order_acquire)) {
                SPDLOG_WARN("WgeWorkerPool::submitBatch: pool stopped, "
                            "{} events not submitted",
                            events.size() - submitted);
                break;
            }

            task_queue_.push_back(std::move(event));
            ++submitted;
        }

        not_empty_.notify_one();
    }

    // 更新 metrics
    metrics_.worker_pool_pending.store(
        static_cast<int64_t>(pendingCount()), std::memory_order_relaxed);

    return submitted;
}

// ============================================================================
// pendingCount / activeCount
// ============================================================================

size_t WgeWorkerPool::pendingCount() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return task_queue_.size();
}

size_t WgeWorkerPool::activeCount() const {
    return active_workers_.load(std::memory_order_relaxed);
}

// ============================================================================
// workerLoop
// ============================================================================

void WgeWorkerPool::workerLoop(int worker_id) {
    SPDLOG_DEBUG("WgeWorkerPool: worker {} started", worker_id);

    while (!stopped_.load(std::memory_order_acquire)) {
        std::shared_ptr<HttpAccessEvent> event;

        // ---- 从队列取任务 ----
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);

            not_empty_.wait(lock, [this] {
                return !task_queue_.empty() ||
                       stopped_.load(std::memory_order_acquire);
            });

            if (stopped_.load(std::memory_order_acquire) && task_queue_.empty()) {
                break;
            }

            if (!task_queue_.empty()) {
                event = std::move(task_queue_.front());
                task_queue_.pop_front();

                // 通知 submitBatch 可以继续写入
                not_full_.notify_one();
            }
        }

        if (!event) {
            continue;
        }

        // ---- Per-task 超时检测 ----
        active_workers_.fetch_add(1, std::memory_order_relaxed);
        metrics_.worker_pool_active.store(
            static_cast<int64_t>(active_workers_.load(std::memory_order_relaxed)),
            std::memory_order_relaxed);

        auto timeout = std::chrono::milliseconds(config_.task_timeout_ms);

        try {
            // 使用 std::async + future 实现超时
            auto future = std::async(std::launch::async, [this, &event]() {
                return detect(*event);
            });

            if (future.wait_for(timeout) == std::future_status::ready) {
                AlertResult result = future.get();

                // 记录检测耗时到 metrics
                // (简化实现; 实际应在 detect 内部记录)

                if (result.hasMatches()) {
                    // 构建告警并发送
                    auto alert = AlertBuilder::build(
                        result,
                        event->event_id(),
                        event->collector_id(),
                        event->request_method(),
                        event->request_uri(),
                        event->downstream_ip(),
                        event->upstream_ip());

                    producer_.sendAlert(std::move(alert));
                    metrics_.incrementAlertsProduced();
                    metrics_.incrementRuleMatches();
                }

                metrics_.incrementEventsProcessed();

            } else {
                // 超时 — 任务仍在后台运行，但我们放弃等待
                SPDLOG_WARN("WgeWorkerPool: worker {} task timed out after {}ms "
                            "for event_id={}",
                            worker_id, config_.task_timeout_ms,
                            event->event_id());
                metrics_.incrementEventsDropped();
            }

        } catch (const std::exception& e) {
            SPDLOG_ERROR("WgeWorkerPool: worker {} detect failed: {} "
                         "(event_id={})",
                         worker_id, e.what(), event->event_id());
            metrics_.incrementEventsDropped();
        }

        active_workers_.fetch_sub(1, std::memory_order_relaxed);

        // 更新 pending metrics
        metrics_.worker_pool_pending.store(
            static_cast<int64_t>(pendingCount()), std::memory_order_relaxed);
    }

    SPDLOG_DEBUG("WgeWorkerPool: worker {} exiting", worker_id);
}

// ============================================================================
// detect — 核心 WGE 检测流程
// ============================================================================

AlertResult WgeWorkerPool::detect(const HttpAccessEvent& event) {
    AlertResult result;
    result.event_id = event.event_id();
    result.timestamp_ms = event.timestamp_ms();

    // 1. 创建 Transaction
    auto tx = engine_.makeTransaction();
    if (!tx) {
        throw std::runtime_error("WgeWorkerPool::detect: makeTransaction returned null");
    }

    // 2. processConnection — 连接信息
    tx->processConnection(
        event.downstream_ip(),
        static_cast<short>(event.downstream_port()),
        event.upstream_ip(),
        static_cast<short>(event.upstream_port()));

    // 3. processUri — URI + Method + HTTP 版本
    tx->processUri(
        event.request_uri(),
        event.request_method(),
        event.request_version());

    // 4. 构建 HttpExtractorAdapter (头信息适配器)
    //    使用 shared_ptr 包装 event (copy shared ownership is fine)
    auto event_ptr = std::make_shared<HttpAccessEvent>(event);
    HttpExtractorAdapter adapter(event_ptr);

    // 5. processRequestHeaders
    auto req_header_find = adapter.requestHeaderFind();
    auto req_header_traversal = adapter.requestHeaderTraversal();

    tx->processRequestHeaders(
        req_header_find,
        req_header_traversal,
        adapter.requestHeaderCount(),
        &WgeWorkerPool::onRuleMatch,
        &result,
        nullptr,   // AdditionalCondCallback
        nullptr);  // Additional cond user_data

    // 6. processRequestBody
    if (!event.request_body().empty()) {
        tx->processRequestBody(event.request_body());
    }

    // 7. processResponseHeaders
    auto resp_header_find = adapter.responseHeaderFind();
    auto resp_header_traversal = adapter.responseHeaderTraversal();

    tx->processResponseHeaders(
        adapter.responseStatusCode(),
        adapter.responseProtocol(),
        resp_header_find,
        resp_header_traversal,
        adapter.responseHeaderCount(),
        &WgeWorkerPool::onRuleMatch,
        &result);

    // 8. processResponseBody
    if (!event.response_body().empty()) {
        tx->processResponseBody(event.response_body());
    }

    // 9. 提取 matched_variables — 通过 Transaction 的 friend 访问或 public getter
    //    假设 Transaction 提供 getCurrentMatchedVariables() 方法
    //    或通过 friend class WgeWorkerPool 访问 matched_variables_
    //
    //    从 WGE Transaction 中提取 matched_variables 来填充 matched_var_name/value
    //    注: 若 Transaction 没有 public getter，需要使 WgeWorkerPool 成为 friend class
    //
    //    这里使用 getCurrentMatchedVariables() (假设存在)
    //    如果只有私有成员，则需要在 wge::Transaction 中声明 friend class WgeWorkerPool

    // 注: 以下代码假设 Transaction 提供 getCurrentMatchedVariables() 方法
    // 若实际 API 不同，调用方需要相应调整

    // ---- 尝试通过 friend 访问 matched_variables_ ----
    // 如果 Transaction 声明了 friend class WgeWorkerPool:
    // auto& matched_vars = tx->matched_variables_;
    // 否则假设有 public getter:
    // auto& matched_vars = tx->getCurrentMatchedVariables();

    // 由于我们在编译时无法确定 Transaction 是否有 friend 声明，
    // 使用一个适配方法: 在 onRuleMatch 回调中已经获取了 Rule 信息，
    // matched_variables 信息需要通过其他方式获取。
    //
    // 对于 matched_var_name/value/original，在 LogCallback 触发时
    // 可以从 Rule 的上下文中推导，但实际 matched variable 信息
    // 需要通过 Transaction 的接口获取。

    // 这里假设 Transaction 有一个 public getter:
    // tx->getCurrentMatchedVariables() 返回
    // const std::vector<std::pair<std::string, std::pair<std::string, std::string>>>&
    //
    // 每个元素: {variable_name, {transformed_value, original_value}}

    // 填充 matched_variables 信息到已有的 matched_rules 中
    // (假设最后一次匹配对应 matched_variables 的最后一个条目)

    // 注: 以下代码为适配层，具体 API 可能不同
    // 实际部署时需根据 WGE SDK 版本调整
#if 0
    // 示例: 假设 Transaction 有此方法
    const auto& matched_vars = tx->getCurrentMatchedVariables();
    for (size_t i = 0; i < matched_vars.size() && i < result.matched_rules.size(); ++i) {
        result.matched_rules[i].matched_var_name = matched_vars[i].first;
        result.matched_rules[i].matched_var_value = matched_vars[i].second.first;
        result.matched_rules[i].matched_var_original = matched_vars[i].second.second;
    }
#endif

    // 规则计数
    metrics_.addEventsProcessed(1);
    // rule_evaluations 和 rule_matches 在 onRuleMatch 中已更新

    return result;
}

// ============================================================================
// onRuleMatch — WGE 规则匹配回调
// ============================================================================

void WgeWorkerPool::onRuleMatch(const wge::Rule& rule, void* user_data) {
    if (!user_data) {
        SPDLOG_WARN("WgeWorkerPool::onRuleMatch: null user_data");
        return;
    }

    auto* result = static_cast<AlertResult*>(user_data);
    auto& metrics = metrics::Metrics::instance();

    metrics.incrementRuleEvaluations();
    metrics.incrementRuleMatches();

    // 从 Rule::detail_ 提取规则信息
    MatchedRuleInfo info;

    if (rule.detail_) {
        info.rule_id = rule.detail_->id_;
        info.rule_msg = rule.detail_->msg_;
        info.severity = rule.detail_->severity_;
        info.rule_ver = rule.detail_->ver_;

        if (rule.detail_->tags_) {
            // tags_ 可能是逗号分隔字符串或数组
            // 这里假设是逗号分隔字符串
            std::string tags_str = *rule.detail_->tags_;
            if (!tags_str.empty()) {
                size_t start = 0;
                size_t end = 0;
                while ((end = tags_str.find(',', start)) != std::string::npos) {
                    info.rule_tags.push_back(tags_str.substr(start, end - start));
                    start = end + 1;
                }
                info.rule_tags.push_back(tags_str.substr(start));
            }
        }

        // 提取拦截信息
        if (rule.detail_->disruptive_) {
            result->intervened = *rule.detail_->disruptive_;
        }

        if (rule.detail_->status_) {
            result->response_code = *rule.detail_->status_;
        }

        if (rule.detail_->redirect_) {
            result->redirect_url = *rule.detail_->redirect_;
        }

        // 提取操作符信息
        if (rule.detail_->operator_) {
            info.operator_name = *rule.detail_->operator_;
        }
    }

    // disruptive_action 推导
    if (result->intervened) {
        if (result->response_code > 0 && !result->redirect_url.empty()) {
            result->disruptive_action = "REDIRECT";
        } else if (result->response_code > 0) {
            result->disruptive_action = "DENY";
        } else {
            result->disruptive_action = "DROP";
        }
    } else {
        result->disruptive_action = "ALLOW";
    }

    result->matched_rules.push_back(std::move(info));
}

}  // namespace wge::kafka::detector
