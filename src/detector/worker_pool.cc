/**
 * @file worker_pool.cc
 * @brief WgeWorkerPool 实现 — WGE 检测线程池
 *
 * ## 模块职责
 * 实现 WGE 安全检测的核心调度逻辑：
 * - 管理固定数量的 worker 线程，从有界阻塞队列消费检测任务
 * - 对每个 HttpAccessEvent 执行 WGE 全流程检测（connection → URI → headers → body）
 * - 通过 AlertProducer 将检测结果（告警）发送到 Kafka
 * - 支持协作式超时、优雅停止和队列排空
 *
 * ## 关键设计决策
 * - **协作式超时**: 在 detect() 各步骤间插入 timed_out() 检查，
 *   而非使用 std::future + wait_for（后者需要额外的线程/复杂度）
 * - **有界队列**: 使用两个 condition_variable（not_empty / not_full）
 *   实现背压，防止生产者速度远超消费者导致 OOM
 * - **swap-then-process**: stop() 排空时先 swap 到局部变量释放锁，
 *   再逐个处理，避免持锁调用可能阻塞的 detect/sendAlert
 */

#include "detector/worker_pool.h"

#include <cctype>
#include <chrono>
#include <stdexcept>
#include <string_view>

#include "detector/alert_builder.h"
#include "detector/http_extractor_adapter.h"
#include "detector/result.h"
#include "http_access.pb.h"
#include "kafka/producer.h"
#include "metrics/metrics.h"
#include "spdlog/spdlog.h"
#include "wge_alert.pb.h"  // WgeAlertEvent (for shouldSendAlert)

// ============================================================================
// 辅助函数: 从 HttpAccessEvent.request_headers 提取 Host header
// ============================================================================

namespace {

/// @brief 从 protobuf repeated Header 中提取 Host header 值 (大小写不敏感)
std::string extractHostFromHeaders(
    const google::protobuf::RepeatedPtrField<wge::kafka::Header>& headers) {
    for (const auto& h : headers) {
        const std::string& key = h.key();
        // 大小写不敏感比较 (使用 static_cast<unsigned char> 避免 char 符号问题)
        if (key.size() == 4 &&
            std::tolower(static_cast<unsigned char>(key[0])) == 'h' &&
            std::tolower(static_cast<unsigned char>(key[1])) == 'o' &&
            std::tolower(static_cast<unsigned char>(key[2])) == 's' &&
            std::tolower(static_cast<unsigned char>(key[3])) == 't') {
            return h.value();
        }
    }
    return "";
}

} // anonymous namespace

// WGE SDK 头文件 (真实 SDK)
#include "engine.h"
#include "rule.h"
#include "transaction.h"

namespace wge::kafka::detector {

// ============================================================================
// 告警保护逻辑 (从 AktoAdapter::convert() 迁移)
// ============================================================================

// Host → CollectionID 兜底映射 (与 AktoAdapter 一致)
const std::unordered_map<std::string, int32_t> WgeWorkerPool::HOST_COLLECTION_FALLBACK_ = {
    {"api.example.com", 1},
    {"admin.example.com", 2},
};

// IpRateLimiter::allow — IP 级限流 (≤5条/分钟/IP+Account+Category)
bool WgeWorkerPool::IpRateLimiter::allow(
    const std::string& ip, const std::string& account_id,
    const std::string& category, int max_per_minute) {
    std::lock_guard<std::mutex> lock(mutex_);
    Key key{ip, account_id, category};
    auto& window = windows_[key];

    auto now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // 清理 1 分钟前的时间戳
    while (!window.empty() && window.front() < now - 60) {
        window.pop_front();
    }

    if (static_cast<int>(window.size()) >= max_per_minute) {
        return false;  // 限流
    }
    window.push_back(now);

    // 定期清理空窗口，防止 windows_ 无限增长
    // 每 1024 次调用清理一次 (避免每次调用都遍历)
    static uint64_t call_counter = 0;
    if ((++call_counter & 0x3FF) == 0) {  // & 1023 == 0
        for (auto it = windows_.begin(); it != windows_.end(); ) {
            auto& w = it->second;
            while (!w.empty() && w.front() < now - 60) {
                w.pop_front();
            }
            if (w.empty()) {
                it = windows_.erase(it);
            } else {
                ++it;
            }
        }
    }

    return true;
}

// shouldSendAlert — 告警分级过滤 + IP 限流 + collection_id 兜底
bool WgeWorkerPool::shouldSendAlert(WgeAlertEvent& alert) {
    // ── 功能1: 告警分级过滤 ──
    // 丢弃 RateLimit/LOW 级别，防止 Akto CloudflareWafSyncCron 误封禁
    if (alert.attack_type() == "RateLimit" || alert.severity() == "LOW") {
        metrics_.incrementAlertsFiltered();
        SPDLOG_DEBUG("[worker_pool] Dropping low-severity/RateLimit alert: {}",
                     alert.alert_id());
        return false;
    }

    // ── 功能2: collection_id=0 兜底 ──
    if (alert.akto_collection_id() == 0) {
        auto it = HOST_COLLECTION_FALLBACK_.find(alert.request_host());
        if (it != HOST_COLLECTION_FALLBACK_.end()) {
            alert.set_akto_collection_id(it->second);
            SPDLOG_DEBUG("[worker_pool] Collection ID fallback: host={} → id={}",
                        alert.request_host(), it->second);
        } else {
            metrics_.incrementAlertsCollectionIdZero();
            SPDLOG_WARN("[worker_pool] Dropping alert: collection_id=0 and no host fallback for {}",
                        alert.request_host());
            return false;
        }
    }

    // ── 功能3: IP 级限流 (≤5条/分钟/IP+Account+Category) ──
    if (!rate_limiter_.allow(alert.downstream_ip(),
                             alert.akto_account_id(),
                             alert.attack_type())) {
        metrics_.incrementAlertsRateLimited();
        SPDLOG_DEBUG("[worker_pool] Rate limited: ip={} category={}",
                     alert.downstream_ip(), alert.attack_type());
        return false;
    }

    return true;
}

// ============================================================================
// 构造与析构
// ============================================================================

WgeWorkerPool::WgeWorkerPool(const Wge::Engine& engine,
                             AlertProducer& producer,
                             metrics::Metrics& metrics,
                             const WorkerConfig& config)
    : engine_(engine)       // WGE 引擎引用，外部保证生命周期
    , producer_(producer)   // 告警生产者引用
    , metrics_(metrics)     // Metrics 单例引用
    , config_(config) {     // 拷贝配置（后续可能修改 worker_threads 等）

    // 自动检测线程数：0 表示使用硬件并发数
    if (config_.worker_threads <= 0) {
        config_.worker_threads = static_cast<int>(
            std::thread::hardware_concurrency());
        if (config_.worker_threads <= 0) {
            config_.worker_threads = 4;  // 检测失败的安全回退值
        }
    }

    // 补全默认队列容量
    if (config_.max_pending_tasks <= 0) {
        config_.max_pending_tasks = 1024;
    }

    SPDLOG_INFO("WgeWorkerPool created: threads={}, max_pending={}, timeout_ms={}",
                config_.worker_threads, config_.max_pending_tasks,
                config_.task_timeout_ms);
}

WgeWorkerPool::~WgeWorkerPool() {
    try {
        stop();  // 确保线程安全退出，排空剩余任务
    } catch (const std::exception& e) {
        // 析构函数中不抛出异常，记录日志即可
        SPDLOG_ERROR("WgeWorkerPool destructor error: {}", e.what());
    } catch (...) {
        SPDLOG_ERROR("WgeWorkerPool destructor: unknown error");
    }
}

// ============================================================================
// start / stop
// ============================================================================

void WgeWorkerPool::start() {
    // CAS 防护：确保只启动一次，重复调用抛异常
    if (started_.exchange(true, std::memory_order_acq_rel)) {
        throw std::runtime_error("WgeWorkerPool::start: already started");
    }

    // 清除停止标志（支持 stop() 后重新 start() 的场景）
    stopped_.store(false, std::memory_order_release);

    // 预分配 vector 容量，避免创建线程时重新分配导致引用失效
    workers_.reserve(static_cast<size_t>(config_.worker_threads));
    for (int i = 0; i < config_.worker_threads; ++i) {
        // emplace_back: 直接在 vector 中构造 thread，参数为成员函数指针 + this + worker_id
        workers_.emplace_back(&WgeWorkerPool::workerLoop, this, i);
    }

    // 设置 worker 线程名称 (平台相关，尽力而为)
    // 注: pthread_setname_np 在 Linux 上可用

    SPDLOG_INFO("WgeWorkerPool started: {} workers", config_.worker_threads);
}

void WgeWorkerPool::stop() {
    // CAS 实现幂等：只有第一个调用者执行停止逻辑，后续调用直接返回
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel,
                                          std::memory_order_relaxed)) {
        return;  // 已停止或正在停止，直接返回
    }

    SPDLOG_INFO("WgeWorkerPool::stop: signaling all workers to stop");

    // 唤醒所有等待中的 worker 线程和 submitBatch 调用者
    // 必须先设置 stopped_ 再 notify，否则存在 TOCTOU 竞态
    not_empty_.notify_all();
    not_full_.notify_all();

    // 等待所有 worker 线程退出（join 阻塞直到线程函数返回）
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();

    // ===== 排空队列中剩余的任务 =====
    // 关键设计：先 swap 到局部变量释放锁，再逐个处理，
    // 避免持锁调用 detect()/sendAlert()（可能阻塞或死锁）
    std::deque<std::shared_ptr<HttpAccessEvent>> remaining;
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        remaining.swap(task_queue_);  // O(1) 交换，不拷贝元素
        SPDLOG_INFO("WgeWorkerPool::stop: all workers stopped, "
                    "remaining tasks in queue: {}",
                    remaining.size());
    }

    // drain 超时: task_timeout_ms * 2，防止挂起的 detect() 永久阻塞 shutdown
    // 溢出保护: task_timeout_ms * 2 可能溢出 int32_t
    int64_t drain_timeout_ms = static_cast<int64_t>(config_.task_timeout_ms) * 2;
    if (drain_timeout_ms > INT32_MAX) drain_timeout_ms = INT32_MAX;
    if (drain_timeout_ms < 1) drain_timeout_ms = 1;
    auto drain_deadline = std::chrono::steady_clock::now()
        + std::chrono::milliseconds(drain_timeout_ms);

    for (auto& event : remaining) {
        try {
            AlertResult result = detect(*event, drain_deadline);
            if (result.hasMatches()) {  // 有匹配才生成告警
                auto alert = AlertBuilder::build(
                    result,
                    event->event_id(),
                    event->collector_id(),
                    event->request_method(),
                    event->request_uri(),
                    event->downstream_ip(),
                    event->upstream_ip(),
                    // Akto 透传字段
                    event->akto_account_id(),
                    event->akto_collection_id(),
                    event->request_body(),
                    event->response_status(),
                    extractHostFromHeaders(event->request_headers()));
                // 告警保护: 分级过滤 + IP 限流 + collection_id 兜底
                if (shouldSendAlert(*alert)) {
                    producer_.sendAlert(std::move(alert));
                    metrics_.incrementAlertsProduced();
                }
            }
            metrics_.incrementEventsProcessed();
        } catch (const std::exception& e) {
            SPDLOG_ERROR("WgeWorkerPool::stop: drain task failed: {}", e.what());
            metrics_.incrementEventsDropped();  // 异常视为丢弃
        }
    }

    started_.store(false, std::memory_order_release);
}

// ============================================================================
// submitBatch
// ============================================================================

int64_t WgeWorkerPool::submitBatch(
    std::vector<std::shared_ptr<HttpAccessEvent>>&& events) {

    // 前置检查：线程池必须已启动
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
            continue;  // 跳过空指针，不中断批量提交
        }

        {
            std::unique_lock<std::mutex> lock(queue_mutex_);

            // 有界队列满时阻塞等待：wait 会在 predicate 为 true 或 spurious wakeup 时返回
            // predicate 条件：队列未满 或 已请求停止
            not_full_.wait(lock, [this, max_capacity] {
                return task_queue_.size() < max_capacity ||
                       stopped_.load(std::memory_order_acquire);
            });

            // 检查停止标志：若已停止则放弃剩余 events，避免无限阻塞
            if (stopped_.load(std::memory_order_acquire)) {
                SPDLOG_WARN("WgeWorkerPool::submitBatch: pool stopped, "
                            "{} events not submitted",
                            events.size() - submitted);
                break;
            }

            task_queue_.push_back(std::move(event));
            ++submitted;
        }
        // 锁外通知：减少锁争用（notify_one 不需要持锁）

        not_empty_.notify_one();  // 通知一个等待的 worker 有新任务
    }

    // 更新 metrics（非关键路径，relaxed 即可）
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

    // 主循环：在收到停止信号前持续从队列取任务处理
    while (!stopped_.load(std::memory_order_acquire)) {
        std::shared_ptr<HttpAccessEvent> event;

        // ---- 从有界队列取任务 (消费者端) ----
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);

            // 队列为空时阻塞等待，直到有新任务或收到停止信号
            not_empty_.wait(lock, [this] {
                return !task_queue_.empty() ||
                       stopped_.load(std::memory_order_acquire);
            });

            // 停止 + 空队列 = 安全退出
            if (stopped_.load(std::memory_order_acquire) && task_queue_.empty()) {
                break;
            }

            if (!task_queue_.empty()) {
                event = std::move(task_queue_.front());
                task_queue_.pop_front();

                // 通知 submitBatch：队列有空位了（生产者可继续写入）
                not_full_.notify_one();
            }
        }

        if (!event) {
            continue;  // 空指针防护，理论上不应出现
        }

        // ---- Per-task 超时检测 (cooperative / 协作式) ----
        // 增加活跃计数，用于 metrics 和监控
        active_workers_.fetch_add(1, std::memory_order_relaxed);
        metrics_.worker_pool_active.store(
            static_cast<int64_t>(active_workers_.load(std::memory_order_relaxed)),
            std::memory_order_relaxed);

        // 计算本次任务的绝对截止时间
        auto timeout = std::chrono::milliseconds(config_.task_timeout_ms);
        auto deadline = std::chrono::steady_clock::now() + timeout;

        try {
            AlertResult result = detect(*event, deadline);

            // 检测是否因超时而提前返回（结果为空且无匹配）
            if (std::chrono::steady_clock::now() > deadline) {
                SPDLOG_WARN("WgeWorkerPool: worker {} task timed out after {}ms "
                            "for event_id={}",
                            worker_id, config_.task_timeout_ms,
                            event->event_id());
                metrics_.incrementEventsDropped();
            } else if (result.dropped) {
                // detect() 内部处理失败（processConnection/Uri/Headers 等返回 false）
                // 已在 detect() 中设置 dropped 标志，此处统一计数
                metrics_.incrementEventsDropped();
            } else if (result.hasMatches()) {
                // 有规则匹配 → 构建告警并发送
                auto alert = AlertBuilder::build(
                    result,
                    event->event_id(),
                    event->collector_id(),
                    event->request_method(),
                    event->request_uri(),
                    event->downstream_ip(),
                    event->upstream_ip(),
                    // Akto 透传字段
                    event->akto_account_id(),
                    event->akto_collection_id(),
                    event->request_body(),
                    event->response_status(),
                    extractHostFromHeaders(event->request_headers()));

                // 告警保护: 分级过滤 + IP 限流 + collection_id 兜底
                if (shouldSendAlert(*alert)) {
                    producer_.sendAlert(std::move(alert));
                    metrics_.incrementAlertsProduced();
                }
                metrics_.incrementEventsProcessed();
            } else {
                // 正常检测无匹配
                metrics_.incrementEventsProcessed();
            }

        } catch (const std::exception& e) {
            // detect() 异常视为该事件检测失败，丢弃并继续处理下一条
            SPDLOG_ERROR("WgeWorkerPool: worker {} detect failed: {} "
                         "(event_id={})",
                         worker_id, e.what(), event->event_id());
            metrics_.incrementEventsDropped();
        }

        // 任务完成，减少活跃计数
        active_workers_.fetch_sub(1, std::memory_order_relaxed);

        // 更新队列待处理数 metrics
        metrics_.worker_pool_pending.store(
            static_cast<int64_t>(pendingCount()), std::memory_order_relaxed);
    }

    SPDLOG_DEBUG("WgeWorkerPool: worker {} exiting", worker_id);
}

// ============================================================================
// detect — 核心 WGE 检测流程
// ============================================================================

AlertResult WgeWorkerPool::detect(const HttpAccessEvent& event,
                                   std::chrono::steady_clock::time_point deadline) {
    AlertResult result;
    result.event_id = event.event_id();        // 关联原始事件 ID
    result.timestamp_ms = event.timestamp_ms(); // 保留原始时间戳

    // 辅助 Lambda：检查当前时间是否超过 deadline（协作式超时）
    // deadline == max() 表示无超时限制（drain 时使用此值）
    auto timed_out = [&deadline]() -> bool {
        return deadline != std::chrono::steady_clock::time_point::max() &&
               std::chrono::steady_clock::now() > deadline;
    };

    // ===== 步骤 1: 创建 WGE Transaction (检测会话) =====
    // Transaction 是 WGE 引擎的核心抽象，封装一次完整的 HTTP 检测上下文
    auto tx = engine_.makeTransaction();
    if (!tx) {
        throw std::runtime_error("WgeWorkerPool::detect: makeTransaction returned null");
    }

    // ===== 步骤 2: processConnection — 设置连接信息 =====
    // 传入下游 IP/Port（客户端地址）和上游 IP/Port（源服务器地址）
    // WGE 引擎可用这些信息进行 IP 黑白名单等检测
    // 注: 真实 WGE SDK 中 processConnection 返回 void
    tx->processConnection(
            event.downstream_ip(),
            static_cast<short>(event.downstream_port()),
            event.upstream_ip(),
            static_cast<short>(event.upstream_port()));
    if (timed_out()) { result.dropped = true; return result; }

    // ===== 步骤 3: processUri — URI + Method + HTTP 版本 =====
    // 注: 真实 WGE SDK 中 processUri 返回 void
    tx->processUri(
            event.request_uri(),
            event.request_method(),
            event.request_version());
    if (timed_out()) { result.dropped = true; return result; }

    // ===== 步骤 4: 构建 HttpExtractorAdapter（延迟创建，避免不必要开销）=====
    // adapter 构建 header 索引（小写 key → values 映射），供 WGE 规则检索
    // 使用引用传递 event，避免 protobuf 深拷贝
    HttpExtractorAdapter adapter(event);

    // ===== 步骤 5: processRequestHeaders =====
    // 获取 header 查找和遍历的 Lambda（由 adapter 生成）
    auto req_header_find = adapter.requestHeaderFind();       // O(1) 按 key 查找
    auto req_header_traversal = adapter.requestHeaderTraversal(); // 遍历所有 headers

    if (!tx->processRequestHeaders(
            req_header_find,          // HeaderFind: 按名称查找 header 值
            req_header_traversal,     // HeaderTraversal: 遍历所有 headers
            adapter.requestHeaderCount(), // header 总数
            &WgeWorkerPool::onRuleMatch,  // 规则匹配回调（静态函数）
            &result,                      // user_data 传入 result 指针
            nullptr,   // AdditionalCondCallback: WGE 扩展条件回调（未使用）
            nullptr))  // Additional cond user_data
    {
        SPDLOG_WARN("WgeWorkerPool::detect: processRequestHeaders failed for event_id={}",
                     event.event_id());
        result.dropped = true;
        return result;
    }
    if (timed_out()) { result.dropped = true; return result; }

    // ===== 步骤 6: processRequestBody =====
    // 仅当请求体非空时才调用（避免不必要的处理）
    if (!event.request_body().empty()) {
        if (!tx->processRequestBody(event.request_body())) {
            SPDLOG_WARN("WgeWorkerPool::detect: processRequestBody failed for event_id={}",
                         event.event_id());
            result.dropped = true;
            return result;
        }
    }
    if (timed_out()) { result.dropped = true; return result; }

    // ===== 步骤 7: processResponseHeaders =====
    auto resp_header_find = adapter.responseHeaderFind();
    auto resp_header_traversal = adapter.responseHeaderTraversal();

    if (!tx->processResponseHeaders(
            adapter.responseStatusCode(),   // 如 "200"
            adapter.responseProtocol(),     // 如 "HTTP/1.1"
            resp_header_find,
            resp_header_traversal,
            adapter.responseHeaderCount(),
            &WgeWorkerPool::onRuleMatch,    // 同上，规则匹配回调
            &result))
    {
        SPDLOG_WARN("WgeWorkerPool::detect: processResponseHeaders failed for event_id={}",
                     event.event_id());
        result.dropped = true;
        return result;
    }
    if (timed_out()) { result.dropped = true; return result; }

    // ===== 步骤 8: processResponseBody =====
    if (!event.response_body().empty()) {
        if (!tx->processResponseBody(event.response_body())) {
            SPDLOG_WARN("WgeWorkerPool::detect: processResponseBody failed for event_id={}",
                         event.event_id());
            result.dropped = true;
            return result;
        }
    }

    // ===== 步骤 9: matched_variables 提取 =====
    // 真实 WGE SDK 中 matched_variables 在 onRuleMatch 回调中已处理
    // 此处无需额外提取

    // 注意: events_processed / events_dropped 计数在 workerLoop() 中统一处理，
    // rule_evaluations 和 rule_matches 在 onRuleMatch 中已更新。
    // 此处不再重复计数 events_processed，避免双重计数。

    return result;
}

// ============================================================================
// onRuleMatch — WGE 规则匹配回调
// ============================================================================

void WgeWorkerPool::onRuleMatch(const Wge::Rule& rule, void* user_data) {
    // user_data 是 detect() 中传入的 AlertResult 指针
    if (!user_data) {
        SPDLOG_WARN("WgeWorkerPool::onRuleMatch: null user_data");
        return;
    }

    auto* result = static_cast<AlertResult*>(user_data);
    auto& metrics = metrics::Metrics::instance();

    // 更新 metrics：每次规则匹配都计数
    metrics.incrementRuleEvaluations();
    metrics.incrementRuleMatches();

    // ===== 从 Rule 提取规则元信息 (使用 getter 方法) =====
    MatchedRuleInfo info;

    info.rule_id = rule.id();
    info.rule_msg = std::string(rule.msg());
    info.severity = static_cast<int>(rule.severity());
    info.rule_ver = std::string(rule.ver());

    // 解析 tags (unordered_set<string_view> → vector<string>)
    for (const auto& tag : rule.tags()) {
        info.rule_tags.emplace_back(tag);
    }

    // ---- 提取拦截/阻断信息 ----
    // disruptive() 返回 Disruptive 枚举，判断是否为阻断型规则
    auto disruptive = rule.disruptive();
    bool is_disruptive = (disruptive == Wge::Rule::Disruptive::DENY ||
                          disruptive == Wge::Rule::Disruptive::DROP ||
                          disruptive == Wge::Rule::Disruptive::REDIRECT ||
                          disruptive == Wge::Rule::Disruptive::BLOCK);
    result->intervened = is_disruptive;

    // status() 返回 string_view，尝试转为 int
    auto status_sv = rule.status();
    if (!status_sv.empty()) {
        try {
            result->response_code = std::stoi(std::string(status_sv));
        } catch (...) {
            result->response_code = 0;
        }
    }

    // redirect() 返回 string_view (非 const 方法，需 const_cast)
    result->redirect_url = std::string(const_cast<Wge::Rule&>(rule).redirect());

    // ===== 推导 disruptive_action（阻断动作类型）=====
    // 根据 intervened + response_code + redirect_url 的组合推导
    if (result->intervened) {
        if (result->response_code > 0 && !result->redirect_url.empty()) {
            result->disruptive_action = "REDIRECT";  // 阻断 + 重定向
        } else if (result->response_code > 0) {
            result->disruptive_action = "DENY";      // 阻断 + 返回状态码
        } else {
            result->disruptive_action = "DROP";       // 阻断 + 丢弃连接
        }
    } else {
        result->disruptive_action = "ALLOW";          // 仅检测，不阻断
    }

    // 将匹配的规则信息追加到结果列表
    result->matched_rules.push_back(std::move(info));
}

}  // namespace wge::kafka::detector
