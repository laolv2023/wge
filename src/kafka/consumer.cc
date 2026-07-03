/**
 * @file consumer.cc
 * @brief KafkaConsumer 实现 — Kafka 消费者
 *
 * ## 模块职责
 * KafkaConsumer 负责从 Kafka topic 消费原始 HTTP 访问日志消息。
 *
 * ## 核心设计
 * - **单线程 poll 模型**: 一个专用 poll 线程持续调用 consumer->consume()，
 *   将消息聚合成批次后通过 on_batch_ 回调交给下游处理
 * - **批次聚合**: 在 poll 线程内部累积消息，达到 max_poll_records 或
 *   遇到超时/分区事件时触发回调，减少回调次数
 * - **Rebalance 处理**: 通过 rdkafka 的 cooperative-sticky 协议响应
 *   ERR__REVOKE_PARTITIONS 和 ERR__ASSIGN_PARTITIONS 事件
 * - **锁拆分**: query_mutex_（轻量，用于 offset 查询）与 poll 线程的
 *   consume() 调用分离，避免持锁进行阻塞 I/O
 * - **优雅停止**: close() consumer 中断阻塞的 consume() 调用，
 *   join poll 线程，清理资源
 *
 * ## 线程模型
 * - Poll 线程: 运行 pollLoop()，调用 consume() + 批处理回调
 * - 外部查询线程（main）: 调用 consumerLag() / committedOffset()
 *   使用 query_mutex_ 保护 assignment() 调用
 */

#include "kafka/consumer.h"

#include <chrono>
#include <sstream>
#include <stdexcept>

#include "librdkafka/rdkafkacpp.h"
#include "spdlog/spdlog.h"

namespace wge::kafka {

// ============================================================================
// 辅助: 配置设置宏和函数
// ============================================================================

namespace {

/**
 * @brief 设置 RdKafka Conf 字符串属性，失败时抛出异常
 *
 * @param conf RdKafka Conf 对象
 * @param key 属性键
 * @param value 属性值
 * @throws std::runtime_error 若设置失败
 */
void setConfOrThrow(RdKafka::Conf* conf, const std::string& key,
                    const std::string& value) {
    std::string err_str;
    RdKafka::Conf::ConfResult result =
        conf->set(key, value, err_str);
    if (result != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error(
            "Failed to set Kafka config '" + key + "' = '" + value +
            "': " + err_str);
    }
}

/**
 * @brief 设置 RdKafka Conf 整型属性，失败时抛出异常
 */
void setConfOrThrow(RdKafka::Conf* conf, const std::string& key,
                    int32_t value) {
    setConfOrThrow(conf, key, std::to_string(value));
}

/**
 * @brief 设置 RdKafka Conf 整型属性 (int64_t)
 */
void setConfOrThrow(RdKafka::Conf* conf, const std::string& key,
                    int64_t value) {
    setConfOrThrow(conf, key, std::to_string(value));
}

/**
 * @brief 条件性设置 RdKafka Conf 字符串属性
 *
 * 仅当 value 非空时设置。
 */
void setConfIfNotEmpty(RdKafka::Conf* conf, const std::string& key,
                       const std::string& value) {
    if (!value.empty()) {
        setConfOrThrow(conf, key, value);
    }
}

}  // namespace

// ============================================================================
// KafkaConsumer 实现
// ============================================================================

KafkaConsumer::KafkaConsumer(const ConsumerConfig& config)
    : config_(config)
    , global_conf_(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL))
    , topic_conf_(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC)) {

    if (!global_conf_ || !topic_conf_) {
        throw std::runtime_error("Failed to create RdKafka Conf objects");
    }

    // ---- 全局配置 ----
    setConfOrThrow(global_conf_.get(), "bootstrap.servers",
                   config_.bootstrap_servers);
    setConfOrThrow(global_conf_.get(), "group.id", config_.group_id);
    setConfOrThrow(global_conf_.get(), "session.timeout.ms",
                   config_.session_timeout_ms);
    setConfOrThrow(global_conf_.get(), "enable.auto.commit",
                   config_.enable_auto_commit ? "true" : "false");
    // max.poll.interval.ms: 设置为 session_timeout * 3，
    // 给下游处理留足时间，防止因处理慢被踢出 group
    // 溢出保护: session_timeout_ms * 3 可能溢出 int32_t
    int64_t max_poll_interval = static_cast<int64_t>(config_.session_timeout_ms) * 3;
    if (max_poll_interval > INT32_MAX) max_poll_interval = INT32_MAX;
    if (max_poll_interval < 1) max_poll_interval = 1;
    setConfOrThrow(global_conf_.get(), "max.poll.interval.ms",
                   std::to_string(max_poll_interval));

    // 安全协议（SASL/SSL）
    applySaslConfig(global_conf_.get());

    // rdkafka 内部日志级别: 6 = LOG_INFO（生产环境建议 4 = LOG_WARNING）
    setConfOrThrow(global_conf_.get(), "log_level", "6");  // LOG_INFO

    // ---- Topic 配置 ----
    setConfOrThrow(topic_conf_.get(), "auto.offset.reset", "latest");

    // ---- 创建 Consumer ----
    std::string err_str;
    consumer_ = RdKafka::KafkaConsumer::create(global_conf_.get(), err_str);
    if (!consumer_) {
        throw std::runtime_error("Failed to create KafkaConsumer: " + err_str);
    }

    // ---- 订阅 ----
    std::vector<std::string> topics = {config_.topic};
    RdKafka::ErrorCode err = consumer_->subscribe(topics);
    if (err != RdKafka::ERR_NO_ERROR) {
        std::string msg = "Failed to subscribe to topic '" +
                          config_.topic + "': " + errorString(err);
        consumer_->close();
        delete consumer_;
        consumer_ = nullptr;
        throw std::runtime_error(msg);
    }

    SPDLOG_INFO("KafkaConsumer created: brokers={}, group={}, topic={}",
                config_.bootstrap_servers, config_.group_id, config_.topic);
}

KafkaConsumer::~KafkaConsumer() {
    try {
        stop();
    } catch (const std::exception& e) {
        SPDLOG_ERROR("KafkaConsumer destructor error: {}", e.what());
    } catch (...) {
        SPDLOG_ERROR("KafkaConsumer destructor: unknown error");
    }
}

void KafkaConsumer::applySaslConfig(RdKafka::Conf* conf) const {
    setConfIfNotEmpty(conf, "security.protocol", config_.security_protocol);
    setConfIfNotEmpty(conf, "sasl.mechanism", config_.sasl_mechanism);
    setConfIfNotEmpty(conf, "sasl.username", config_.sasl_username);
    setConfIfNotEmpty(conf, "sasl.password", config_.sasl_password);
}

void KafkaConsumer::start(
    std::function<void(std::vector<std::unique_ptr<RdKafka::Message>>)> on_batch) {

    if (running_.exchange(true, std::memory_order_acq_rel)) {
        throw std::runtime_error("KafkaConsumer::start: poll thread already running");
    }

    on_batch_ = std::move(on_batch);
    stopped_.store(false, std::memory_order_release);

    poll_thread_ = std::thread(&KafkaConsumer::pollLoop, this);

    SPDLOG_INFO("KafkaConsumer poll thread started: topic={}, max_poll_records={}, interval_ms={}",
                config_.topic, config_.max_poll_records, config_.poll_interval_ms);
}

void KafkaConsumer::stop() {
    // 幂等: 已停止则直接返回
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel,
                                          std::memory_order_relaxed)) {
        return;  // 已经在停止中
    }

    SPDLOG_INFO("KafkaConsumer::stop: signaling poll thread to stop");

    // 立即关闭 consumer 以中断 poll 线程中阻塞的 consume() 调用
    if (consumer_) {
        consumer_->close();
    }

    // 等待 poll 线程退出 (poll 使用 timeout，自动检查 stopped_ flag)
    if (poll_thread_.joinable()) {
        poll_thread_.join();
        SPDLOG_INFO("KafkaConsumer::stop: poll thread joined");
    }

    // 清理 consumer
    if (consumer_) {
        delete consumer_;
        consumer_ = nullptr;
        SPDLOG_INFO("KafkaConsumer::stop: consumer deleted");
    }

    running_.store(false, std::memory_order_release);
}

RdKafka::ConsumerGroupMetadata* KafkaConsumer::groupMetadata() const {
    if (!consumer_) {
        return nullptr;
    }
    return consumer_->groupMetadata();
}

int64_t KafkaConsumer::consumerLag() const {
    if (!consumer_) {
        return -1;
    }

    // ===== 锁拆分模式：持锁收集信息 → 释放锁 → 调用阻塞 I/O =====
    // 持锁状态下调用 assignment()（轻量操作，获取当前分配的分区列表）
    // 然后将分区信息拷贝到本地，释放锁后再调用可能阻塞的 API
    std::vector<RdKafka::TopicPartition*> partitions;
    {
        std::lock_guard<std::mutex> lock(query_mutex_);
        RdKafka::ErrorCode err = consumer_->assignment(partitions);
        if (err != RdKafka::ERR_NO_ERROR) {
            SPDLOG_WARN("Failed to get assignment for lag query: {}",
                        errorString(err));
            for (auto* tp : partitions) {
                delete tp;
            }
            return -1;
        }
    }
    // 锁外：逐个分区查询 lag（committed + get_watermark_offsets 都是阻塞 I/O）
    // 使用 RAII 确保 partitions 中的所有 TopicPartition* 都被释放，
    // 即使中途抛出异常也不会泄漏。
    struct TopicPartitionDeleter {
        std::vector<RdKafka::TopicPartition*>& vec;
        ~TopicPartitionDeleter() {
            for (auto* tp : vec) delete tp;
            vec.clear();
        }
    } deleter{partitions};

    int64_t total_lag = 0;
    for (auto* tp : partitions) {
        // 1. 获取已提交 offset (committed)
        int64_t committed = -1;
        {
            std::vector<RdKafka::TopicPartition*> tp_vec = {tp};
            RdKafka::ErrorCode committed_err =
                consumer_->committed(tp_vec, 5000);  // 5s 超时
            if (committed_err == RdKafka::ERR_NO_ERROR) {
                committed = tp_vec[0]->offset();
            }
        }

        // 2. 获取高水位 (log end offset)
        int64_t low = 0;      // 低水位（最早可用 offset）
        int64_t high = -1;    // 高水位（最新 offset + 1）
        RdKafka::ErrorCode watermark_err =
            consumer_->get_watermark_offsets(
                tp->topic(), tp->partition(), &low, &high, 5000);

        // 3. 计算 lag = 高水位 - 已提交 offset
        if (watermark_err == RdKafka::ERR_NO_ERROR && high >= 0) {
            if (committed >= 0) {
                total_lag += (high - committed);
            } else {
                total_lag += high;  // 无 committed offset，将全部消息视为 lag
            }
        }
        // 注意: 不在此处 delete tp，由 RAII deleter 统一释放
    }

    return total_lag;
}

int64_t KafkaConsumer::committedOffset() const {
    if (!consumer_) {
        return -1;
    }

    // 持锁收集分区信息到本地变量，释放锁后再调用阻塞 I/O
    std::vector<RdKafka::TopicPartition*> partitions;
    {
        std::lock_guard<std::mutex> lock(query_mutex_);
        RdKafka::ErrorCode err = consumer_->assignment(partitions);
        if (err != RdKafka::ERR_NO_ERROR) {
            for (auto* tp : partitions) {
                delete tp;
            }
            return -1;
        }
    }

    // 释放锁后逐个调用阻塞 API
    // 使用 RAII 确保 partitions 中的所有 TopicPartition* 都被释放
    struct TopicPartitionDeleter {
        std::vector<RdKafka::TopicPartition*>& vec;
        ~TopicPartitionDeleter() {
            for (auto* tp : vec) delete tp;
            vec.clear();
        }
    } deleter{partitions};

    int64_t total_committed = 0;
    for (auto* tp : partitions) {
        std::vector<RdKafka::TopicPartition*> tp_vec = {tp};
        RdKafka::ErrorCode committed_err =
            consumer_->committed(tp_vec, 5000);
        if (committed_err == RdKafka::ERR_NO_ERROR) {
            total_committed += tp_vec[0]->offset();
        }
        // 注意: 不在此处 delete tp，由 RAII deleter 统一释放
    }

    return total_committed;
}

std::string KafkaConsumer::errorString(RdKafka::ErrorCode err) {
    return RdKafka::err2str(err);
}

// ============================================================================
// Poll 循环
// ============================================================================

void KafkaConsumer::pollLoop() {
    SPDLOG_INFO("KafkaConsumer poll loop started");

    // 预分配批次缓冲区，避免频繁重新分配
    std::vector<std::unique_ptr<RdKafka::Message>> batch;
    batch.reserve(static_cast<size_t>(config_.max_poll_records));

    while (!stopped_.load(std::memory_order_acquire)) {
        // 单次 poll: consume() 阻塞等待，超时时间为 poll_interval_ms
        std::unique_ptr<RdKafka::Message> msg(
            consumer_->consume(config_.poll_interval_ms));

        if (!msg) {
            continue;  // 空消息（极少发生），继续循环
        }

        // 根据消息 err 码进行状态机处理
        switch (msg->err()) {
        case RdKafka::ERR_NO_ERROR:
            // 正常消息: 加入批次缓冲区
            batch.push_back(std::move(msg));

            // 批次满时触发回调，然后清空缓冲区
            if (static_cast<int32_t>(batch.size()) >= config_.max_poll_records) {
                try {
                    if (on_batch_) {
                        on_batch_(std::move(batch));  // 转移所有权
                    }
                } catch (const std::exception& e) {
                    SPDLOG_ERROR("KafkaConsumer on_batch exception: {}", e.what());
                }
                batch.clear();
                batch.reserve(static_cast<size_t>(config_.max_poll_records));
            }
            break;

        case RdKafka::ERR__TIMED_OUT:
        case RdKafka::ERR__PARTITION_EOF:
            // poll 超时或分区已读完（EOF）: 投递当前批次
            // 超时是正常现象，表示当前没有新消息
            if (!batch.empty()) {
                try {
                    if (on_batch_) {
                        on_batch_(std::move(batch));
                    }
                } catch (const std::exception& e) {
                    SPDLOG_ERROR("KafkaConsumer on_batch exception: {}", e.what());
                }
                batch.clear();
                batch.reserve(static_cast<size_t>(config_.max_poll_records));
            }
            break;

        case RdKafka::ERR__REVOKE_PARTITIONS:
            // Rebalance: 分区被撤销
            // 1. 先投递当前批次中的消息（在失去分区所有权前），避免数据丢失
            // 2. 调用 assign(空) 显式确认撤销
            SPDLOG_INFO("KafkaConsumer: partitions revoked, flushing current batch "
                        "({} messages) before clearing", batch.size());
            // 先投递当前批次，避免已 poll 但未处理的消息丢失
            if (!batch.empty() && on_batch_) {
                try {
                    on_batch_(std::move(batch));
                } catch (const std::exception& e) {
                    SPDLOG_ERROR("KafkaConsumer: on_batch exception during revoke: {}",
                                 e.what());
                }
            }
            {
                std::vector<RdKafka::TopicPartition*> empty;
                consumer_->assign(empty);  // 空列表 = 放弃所有分区
            }
            batch.clear();
            batch.reserve(static_cast<size_t>(config_.max_poll_records));
            break;

        case RdKafka::ERR__ASSIGN_PARTITIONS:
            // Rebalance: 新分区分配
            // 记录分配的分区信息，投递当前批次
            SPDLOG_INFO("KafkaConsumer: partitions assigned/re-assigned");
            {
                std::vector<RdKafka::TopicPartition*> assigned;
                // RAII: 确保 assigned 中的所有 TopicPartition* 都被释放
                struct TpDeleter {
                    std::vector<RdKafka::TopicPartition*>& v;
                    ~TpDeleter() {
                        for (auto* tp : v) delete tp;
                        v.clear();
                    }
                } tp_deleter{assigned};

                RdKafka::ErrorCode assign_err = consumer_->assignment(assigned);
                if (assign_err == RdKafka::ERR_NO_ERROR) {
                    for (auto* tp : assigned) {
                        SPDLOG_INFO("KafkaConsumer: assigned partition {} [{}]",
                                    tp->topic(), tp->partition());
                    }
                } else {
                    SPDLOG_WARN("KafkaConsumer: failed to get assignment: {}",
                                errorString(assign_err));
                }
            }
            // Rebalance 后投递当前批次（避免消息丢失）
            if (!batch.empty()) {
                try {
                    if (on_batch_) {
                        on_batch_(std::move(batch));
                    }
                } catch (const std::exception& e) {
                    SPDLOG_ERROR("KafkaConsumer on_batch exception: {}", e.what());
                }
                batch.clear();
                batch.reserve(static_cast<size_t>(config_.max_poll_records));
            }
            break;

        default:
            // 其他错误（如网络错误、broker 不可用等）
            SPDLOG_ERROR("KafkaConsumer poll error: {} ({})",
                         msg->errstr(), static_cast<int>(msg->err()));

            // 对于可恢复错误，尽力投递当前批次
            if (!batch.empty()) {
                try {
                    if (on_batch_) {
                        on_batch_(std::move(batch));
                    }
                } catch (const std::exception& e) {
                    SPDLOG_ERROR("KafkaConsumer on_batch exception: {}", e.what());
                }
                batch.clear();
                batch.reserve(static_cast<size_t>(config_.max_poll_records));
            }
            break;
        }
    }

    // 停止前投递缓冲区剩余消息（不丢失已 poll 但未回调的消息）
    if (!batch.empty()) {
        try {
            if (on_batch_) {
                on_batch_(std::move(batch));
            }
        } catch (const std::exception& e) {
            SPDLOG_ERROR("KafkaConsumer on_batch exception on shutdown: {}",
                         e.what());
        }
    }

    SPDLOG_INFO("KafkaConsumer poll loop exited");
}

}  // namespace wge::kafka
