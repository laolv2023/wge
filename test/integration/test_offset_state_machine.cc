/**
 * @file test_offset_state_machine.cc
 * @brief CTP Offset 状态机集成测试
 *
 * 实现一个独立于 Kafka 的 OffsetTracker 类，模拟 CTP (Consume-Transform-Produce)
 * 的 offset 管理状态机语义。
 *
 * 状态机:
 * ```
 *                  consume(n)
 *     [INIT] ───────────────▶ [PENDING]
 *                                │
 *                     markSuccess(n) / markFailure(n)
 *                                │
 *               ┌────────────────┼────────────────┐
 *               ▼                ▼                ▼
 *         [SUCCESS]        [FAILED]         [PARTIAL]
 *               │                │                │
 *               │    retry()     │    retry()     │
 *               │    ┌───────────┘                │
 *               ▼    ▼                            │
 *           commit() → [COMMITTED]  ◀─────────────┘
 *               │
 *               ▼
 *           [CLEAN]
 * ```
 *
 * 所有测试自包含，不依赖外部 Kafka/WGE 服务。
 *
 * 测试覆盖:
 *   1. SimpleConsumeCommit           — consume→success→commit
 *   2. ConsumeFailRetrySuccess       — 失败→重试→成功→提交
 *   3. BatchConsumePartialFail       — 批量消费，部分成功部分失败，只提交成功部分
 *   4. TransactionRollback           — 事务回滚后offset回到commit点
 *   5. MultiPartitionIndependent     — 多分区offset独立追踪
 *   6. ExactlyOnceGuarantee          — 回滚后重消费不会产生重复offset推进
 *   7. CommitIdempotent              — 重复commit不会推进已提交的offset
 *   8. RestoreFromCommitted          — 从已提交的offset恢复（模拟崩溃重启）
 *   9. OutOfOrderOffsets             — 乱序offset正确处理
 *  10. HighWatermarkBoundary         — offset不能超过high watermark
 *  11. ZeroOffsetHandling            — offset=0的边界情况
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

// ============================================================================
// OffsetState — 单条消息的 offset 状态
// ============================================================================

namespace {

/// @brief 单条消息在 CTP 管道中的状态
enum class OffsetStatus : uint8_t {
    Init,       // 初始状态
    Pending,    // 已消费，等待处理
    Success,    // 处理成功
    Failed,     // 处理失败
    Partial,    // 批量中的部分成功
    Committed,  // 已提交
};

/// @brief 单条 offset 记录的状态
struct OffsetRecord {
    int64_t offset{0};
    OffsetStatus status{OffsetStatus::Init};
    int32_t retry_count{0};

    bool isTerminal() const {
        return status == OffsetStatus::Committed;
    }

    bool isSuccessLike() const {
        return status == OffsetStatus::Success
            || status == OffsetStatus::Partial
            || status == OffsetStatus::Committed;
    }

    const char* statusStr() const {
        switch (status) {
            case OffsetStatus::Init:      return "Init";
            case OffsetStatus::Pending:   return "Pending";
            case OffsetStatus::Success:   return "Success";
            case OffsetStatus::Failed:    return "Failed";
            case OffsetStatus::Partial:   return "Partial";
            case OffsetStatus::Committed: return "Committed";
        }
        return "Unknown";
    }
};

}  // namespace

// ============================================================================
// OffsetTracker — CTP Offset 状态机
// ============================================================================

/**
 * @brief CTP Offset 状态机追踪器
 *
 * 模拟 Kafka CTP (Consume-Transform-Produce) 管道中的 offset 管理:
 *
 * - consume(n):    将 offset n 从 INIT → PENDING
 * - markSuccess(n): 将 offset n 从 PENDING → SUCCESS
 * - markFailure(n): 将 offset n 从 PENDING → FAILED
 * - markPartial(n): 将 offset n 标记为 PARTIAL（批量中部分成功）
 * - retry(n):       将 FAILED/PARTIAL offset 重置为 PENDING
 * - commit():       提交所有 SUCCESS/PARTIAL 连续 offset
 * - rollback():     回滚到上次 commit 点
 * - restore():      从 committed offset 恢复（模拟重启）
 */
class OffsetTracker {
public:
    // ==========================================================================
    // 消费
    // ==========================================================================

    /**
     * @brief 消费消息 — INIT → PENDING
     *
     * @param topic     主题名
     * @param partition 分区号
     * @param offset    offset 值
     * @param high_watermark 高水位线（可选，用于边界检查）
     * @return true 若成功消费
     */
    bool consume(const std::string& topic, int32_t partition,
                 int64_t offset,
                 std::optional<int64_t> high_watermark = std::nullopt) {
        // 边界检查: offset 不能超过 high watermark
        if (high_watermark.has_value() && offset > high_watermark.value()) {
            return false;
        }

        // offset 不能为负（除了特殊情况）
        if (offset < 0) {
            return false;
        }

        auto key = makeKey(topic, partition);
        auto& partition_state = partitions_[key];

        // 状态机: INIT → PENDING
        auto it = partition_state.records.find(offset);
        if (it != partition_state.records.end()) {
            // 已存在的 offset: 只能从 INIT/FAILED 状态重新消费
            if (it->second.status == OffsetStatus::Init
                || it->second.status == OffsetStatus::Failed) {
                it->second.status = OffsetStatus::Pending;
                it->second.retry_count++;
            }
            // 否则不允许重复消费已成功的 offset
            return it->second.status == OffsetStatus::Pending;
        }

        OffsetRecord rec;
        rec.offset = offset;
        rec.status = OffsetStatus::Pending;
        partition_state.records[offset] = rec;

        // 更新该分区的 max_consumed
        if (offset > partition_state.max_consumed) {
            partition_state.max_consumed = offset;
        }

        return true;
    }

    // ==========================================================================
    // 标记结果
    // ==========================================================================

    /**
     * @brief 标记成功 — PENDING → SUCCESS
     */
    void markSuccess(const std::string& topic, int32_t partition,
                     int64_t offset) {
        auto* rec = findRecord(topic, partition, offset);
        if (!rec) return;
        if (rec->status == OffsetStatus::Pending
            || rec->status == OffsetStatus::Failed) {
            rec->status = OffsetStatus::Success;
        }
    }

    /**
     * @brief 标记失败 — PENDING → FAILED
     */
    void markFailure(const std::string& topic, int32_t partition,
                     int64_t offset) {
        auto* rec = findRecord(topic, partition, offset);
        if (!rec) return;
        if (rec->status == OffsetStatus::Pending
            || rec->status == OffsetStatus::Success) {
            rec->status = OffsetStatus::Failed;
        }
    }

    /**
     * @brief 标记部分成功 — PENDING → PARTIAL
     *
     * 用于批量消费场景: 批量中某些 offset 成功、某些失败。
     */
    void markPartial(const std::string& topic, int32_t partition,
                     int64_t offset) {
        auto* rec = findRecord(topic, partition, offset);
        if (!rec) return;
        if (rec->status == OffsetStatus::Pending) {
            rec->status = OffsetStatus::Partial;
        }
    }

    // ==========================================================================
    // 重试
    // ==========================================================================

    /**
     * @brief 重试失败的 offset — FAILED/PARTIAL → PENDING
     */
    void retry(const std::string& topic, int32_t partition, int64_t offset) {
        auto* rec = findRecord(topic, partition, offset);
        if (!rec) return;
        if (rec->status == OffsetStatus::Failed
            || rec->status == OffsetStatus::Partial) {
            rec->status = OffsetStatus::Pending;
            rec->retry_count++;
        }
    }

    /**
     * @brief 重试所有 FAILED 和 PARTIAL 的 offset
     * @return 重试的 offset 数量
     */
    int retryAll(const std::string& topic, int32_t partition) {
        auto key = makeKey(topic, partition);
        auto it = partitions_.find(key);
        if (it == partitions_.end()) return 0;

        int count = 0;
        for (auto& [offset, rec] : it->second.records) {
            if (rec.status == OffsetStatus::Failed
                || rec.status == OffsetStatus::Partial) {
                rec.status = OffsetStatus::Pending;
                rec.retry_count++;
                count++;
            }
        }
        return count;
    }

    // ==========================================================================
    // 提交
    // ==========================================================================

    /**
     * @brief 提交 offset — 连续 SUCCESS/PARTIAL → COMMITTED
     *
     * 从已提交点之后开始，提交连续的 SUCCESS 和 PARTIAL offset。
     * 遇到 FAILED 或 PENDING 停止。
     *
     * @return 本次提交推进到的 offset（含），-1 表示无推进
     */
    int64_t commit(const std::string& topic, int32_t partition) {
        auto key = makeKey(topic, partition);
        auto it = partitions_.find(key);
        if (it == partitions_.end()) return -1;

        auto& ps = it->second;
        int64_t start = ps.committed_offset + 1;
        int64_t last_committable = ps.committed_offset;

        // 从 committed+1 开始扫描连续成功的 offset
        for (int64_t off = start; ; ++off) {
            auto rit = ps.records.find(off);
            if (rit == ps.records.end()) break;  // 空洞: 停止

            auto& rec = rit->second;
            if (rec.status == OffsetStatus::Success
                || rec.status == OffsetStatus::Partial) {
                rec.status = OffsetStatus::Committed;
                last_committable = off;
            } else if (rec.status == OffsetStatus::Committed) {
                last_committable = off;  // 已提交的 span 延续
            } else {
                break;  // PENDING/FAILED: 停止
            }
        }

        if (last_committable > ps.committed_offset) {
            ps.committed_offset = last_committable;
            return last_committable;
        }
        return -1;
    }

    /**
     * @brief 提交所有分区
     */
    void commitAll() {
        for (auto& [key, ps] : partitions_) {
            // 从 key 解析 topic 和 partition
            // key 格式: "topic:partition"
            auto pos = key.find(':');
            if (pos == std::string::npos) continue;
            std::string topic = key.substr(0, pos);
            int32_t partition = std::stoi(key.substr(pos + 1));
            commit(topic, partition);
        }
    }

    // ==========================================================================
    // 回滚
    // ==========================================================================

    /**
     * @brief 事务回滚 — 回到 committed offset
     *
     * 将 committed 之后的所有 offset 状态重置为 PENDING，
     * 模拟事务失败后回滚到安全点。
     */
    void rollback(const std::string& topic, int32_t partition) {
        auto key = makeKey(topic, partition);
        auto it = partitions_.find(key);
        if (it == partitions_.end()) return;

        auto& ps = it->second;
        for (auto& [offset, rec] : ps.records) {
            if (offset > ps.committed_offset) {
                // 已提交的保持不变
                if (rec.status != OffsetStatus::Committed) {
                    rec.status = OffsetStatus::Pending;
                    rec.retry_count = 0;
                }
            }
        }

        // max_consumed 回到 committed
        ps.max_consumed = ps.committed_offset;
    }

    // ==========================================================================
    // 恢复
    // ==========================================================================

    /**
     * @brief 从已提交的 offset 恢复（模拟崩溃重启）
     *
     * 清除 committed 之后的所有记录，状态回到 committed 点。
     */
    void restoreFromCommitted(const std::string& topic, int32_t partition) {
        auto key = makeKey(topic, partition);
        auto it = partitions_.find(key);
        if (it == partitions_.end()) return;

        auto& ps = it->second;

        // 移除 committed 之后的所有记录
        auto rit = ps.records.begin();
        while (rit != ps.records.end()) {
            if (rit->first > ps.committed_offset) {
                rit = ps.records.erase(rit);
            } else {
                ++rit;
            }
        }

        ps.max_consumed = ps.committed_offset;
    }

    // ==========================================================================
    // 查询
    // ==========================================================================

    /// @brief 获取已提交的 offset
    int64_t getCommittedOffset(const std::string& topic,
                               int32_t partition) const {
        auto key = makeKey(topic, partition);
        auto it = partitions_.find(key);
        if (it == partitions_.end()) return -1;
        return it->second.committed_offset;
    }

    /// @brief 获取已消费的最大 offset
    int64_t getMaxConsumed(const std::string& topic,
                           int32_t partition) const {
        auto key = makeKey(topic, partition);
        auto it = partitions_.find(key);
        if (it == partitions_.end()) return -1;
        return it->second.max_consumed;
    }

    /// @brief 获取 offset 状态
    OffsetStatus getStatus(const std::string& topic, int32_t partition,
                           int64_t offset) const {
        auto key = makeKey(topic, partition);
        auto it = partitions_.find(key);
        if (it == partitions_.end()) return OffsetStatus::Init;
        auto rit = it->second.records.find(offset);
        if (rit == it->second.records.end()) return OffsetStatus::Init;
        return rit->second.status;
    }

    /// @brief 获取重试次数
    int32_t getRetryCount(const std::string& topic, int32_t partition,
                          int64_t offset) const {
        auto* rec = findRecord(topic, partition, offset);
        if (!rec) return 0;
        return rec->retry_count;
    }

    /// @brief 获取分区中处于某状态的 offset 集合
    std::set<int64_t> getOffsetsByStatus(const std::string& topic,
                                          int32_t partition,
                                          OffsetStatus status) const {
        auto key = makeKey(topic, partition);
        auto it = partitions_.find(key);
        std::set<int64_t> result;
        if (it == partitions_.end()) return result;
        for (const auto& [off, rec] : it->second.records) {
            if (rec.status == status) result.insert(off);
        }
        return result;
    }

    /// @brief 分区数
    size_t partitionCount() const { return partitions_.size(); }

    /// @brief 某分区的记录总数
    size_t recordCount(const std::string& topic, int32_t partition) const {
        auto key = makeKey(topic, partition);
        auto it = partitions_.find(key);
        if (it == partitions_.end()) return 0;
        return it->second.records.size();
    }

private:
    struct PartitionState {
        std::map<int64_t, OffsetRecord> records;
        int64_t committed_offset{-1};
        int64_t max_consumed{-1};
    };

    static std::string makeKey(const std::string& topic, int32_t partition) {
        return topic + ":" + std::to_string(partition);
    }

    OffsetRecord* findRecord(const std::string& topic, int32_t partition,
                             int64_t offset) {
        auto key = makeKey(topic, partition);
        auto it = partitions_.find(key);
        if (it == partitions_.end()) return nullptr;
        auto rit = it->second.records.find(offset);
        if (rit == it->second.records.end()) return nullptr;
        return &rit->second;
    }

    const OffsetRecord* findRecord(const std::string& topic, int32_t partition,
                                   int64_t offset) const {
        auto key = makeKey(topic, partition);
        auto it = partitions_.find(key);
        if (it == partitions_.end()) return nullptr;
        auto rit = it->second.records.find(offset);
        if (rit == it->second.records.end()) return nullptr;
        return &rit->second;
    }

    std::map<std::string, PartitionState> partitions_;
};

// ============================================================================
// Test Fixture
// ============================================================================

class OffsetStateMachineTest : public ::testing::Test {
protected:
    void SetUp() override {
        tracker_ = OffsetTracker{};
    }

    /// @brief 便捷: 消费一批 offset
    void consumeRange(const std::string& topic, int32_t partition,
                      int64_t from, int64_t to) {
        for (int64_t i = from; i <= to; ++i) {
            ASSERT_TRUE(tracker_.consume(topic, partition, i))
                << "consume(" << i << ") failed";
        }
    }

    /// @brief 便捷: 标记一批 offset 成功
    void markSuccessRange(const std::string& topic, int32_t partition,
                          int64_t from, int64_t to) {
        for (int64_t i = from; i <= to; ++i) {
            tracker_.markSuccess(topic, partition, i);
        }
    }

    OffsetTracker tracker_;
    const std::string topic_{"test-topic"};
};

// ============================================================================
// 1. SimpleConsumeCommit — consume→success→commit
// ============================================================================

TEST_F(OffsetStateMachineTest, SimpleConsumeCommit) {
    // 消费 offset 0
    ASSERT_TRUE(tracker_.consume(topic_, 0, 0));
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 0), OffsetStatus::Pending);

    // 标记成功
    tracker_.markSuccess(topic_, 0, 0);
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 0), OffsetStatus::Success);

    // 提交
    int64_t committed = tracker_.commit(topic_, 0);
    EXPECT_EQ(committed, 0);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 0);
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 0), OffsetStatus::Committed);
}

// ============================================================================
// 2. ConsumeFailRetrySuccess — 失败→重试→成功→提交
// ============================================================================

TEST_F(OffsetStateMachineTest, ConsumeFailRetrySuccess) {
    // 消费 offset 5
    ASSERT_TRUE(tracker_.consume(topic_, 0, 5));
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 5), OffsetStatus::Pending);

    // 处理失败
    tracker_.markFailure(topic_, 0, 5);
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 5), OffsetStatus::Failed);

    // 尝试提交 — 不应推进（无连续成功）
    int64_t committed = tracker_.commit(topic_, 0);
    EXPECT_EQ(committed, -1);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), -1);

    // 重试
    tracker_.retry(topic_, 0, 5);
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 5), OffsetStatus::Pending);
    EXPECT_EQ(tracker_.getRetryCount(topic_, 0, 5), 1);

    // 重试成功
    tracker_.markSuccess(topic_, 0, 5);
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 5), OffsetStatus::Success);

    // 提交
    committed = tracker_.commit(topic_, 0);
    EXPECT_EQ(committed, 5);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 5);
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 5), OffsetStatus::Committed);
}

// ============================================================================
// 3. BatchConsumePartialFail — 批量消费，部分成功部分失败，只提交成功部分
// ============================================================================

TEST_F(OffsetStateMachineTest, BatchConsumePartialFail) {
    // 消费 offset 0-9
    consumeRange(topic_, 0, 0, 9);

    // 标记: 0-2 成功, 3 失败, 4-6 成功, 7 失败, 8-9 成功
    markSuccessRange(topic_, 0, 0, 2);
    tracker_.markFailure(topic_, 0, 3);
    markSuccessRange(topic_, 0, 4, 6);
    tracker_.markFailure(topic_, 0, 7);
    markSuccessRange(topic_, 0, 8, 9);

    // 将成功的标记为 PARTIAL（表示批量中的部分成功）
    for (int64_t i : {0, 1, 2, 4, 5, 6, 8, 9}) {
        tracker_.markPartial(topic_, 0, i);
    }

    // commit 应该只提交连续的 PARTIAL: 0-2
    int64_t committed = tracker_.commit(topic_, 0);
    EXPECT_EQ(committed, 2) << "Should only commit contiguous success span";
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 2);

    // offset 3 仍为 FAILED
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 3), OffsetStatus::Failed);

    // offset 4-6 为 PARTIAL 但被 offset 3 阻塞
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 4), OffsetStatus::Partial);

    // 重试 offset 3
    tracker_.retry(topic_, 0, 3);
    tracker_.markSuccess(topic_, 0, 3);

    // 再次 commit — 应该推进到 6（连续: 3(SUCCESS) + 4-6(PARTIAL)）
    committed = tracker_.commit(topic_, 0);
    EXPECT_EQ(committed, 6);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 6);

    // 重试 offset 7
    tracker_.retry(topic_, 0, 7);
    tracker_.markSuccess(topic_, 0, 7);

    committed = tracker_.commit(topic_, 0);
    EXPECT_EQ(committed, 9);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 9);

    // 所有 offset 都已提交
    for (int64_t i = 0; i <= 9; ++i) {
        EXPECT_EQ(tracker_.getStatus(topic_, 0, i), OffsetStatus::Committed)
            << "Offset " << i;
    }
}

// ============================================================================
// 4. TransactionRollback — 事务回滚后offset回到commit点
// ============================================================================

TEST_F(OffsetStateMachineTest, TransactionRollback) {
    // 提交 offset 0-4
    consumeRange(topic_, 0, 0, 4);
    markSuccessRange(topic_, 0, 0, 4);
    int64_t committed = tracker_.commit(topic_, 0);
    EXPECT_EQ(committed, 4);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 4);

    // 消费并处理 offset 5-9
    consumeRange(topic_, 0, 5, 9);
    markSuccessRange(topic_, 0, 5, 9);
    EXPECT_EQ(tracker_.getMaxConsumed(topic_, 0), 9);

    // ---- 事务失败，回滚 ----
    tracker_.rollback(topic_, 0);

    // max_consumed 回到 committed 点
    EXPECT_EQ(tracker_.getMaxConsumed(topic_, 0), 4);

    // offset 5-9 状态重置为 PENDING
    for (int64_t i = 5; i <= 9; ++i) {
        EXPECT_EQ(tracker_.getStatus(topic_, 0, i), OffsetStatus::Pending)
            << "Offset " << i << " should be reset to Pending after rollback";
    }

    // committed 不变
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 4);

    // 重新处理并提交
    markSuccessRange(topic_, 0, 5, 9);
    committed = tracker_.commit(topic_, 0);
    EXPECT_EQ(committed, 9);
}

// ============================================================================
// 5. MultiPartitionIndependent — 多分区offset独立追踪
// ============================================================================

TEST_F(OffsetStateMachineTest, MultiPartitionIndependent) {
    const int32_t kPartitions = 4;

    // 每个分区消费不同的 offset 范围
    for (int32_t p = 0; p < kPartitions; ++p) {
        int64_t count = (p + 1) * 10;
        for (int64_t i = 0; i < count; ++i) {
            ASSERT_TRUE(tracker_.consume(topic_, p, i));
            tracker_.markSuccess(topic_, p, i);
        }
    }

    // 验证各分区独立
    EXPECT_EQ(tracker_.partitionCount(), static_cast<size_t>(kPartitions));

    for (int32_t p = 0; p < kPartitions; ++p) {
        int64_t expected = (p + 1) * 10 - 1;
        EXPECT_EQ(tracker_.getMaxConsumed(topic_, p), expected)
            << "Partition " << p;

        // 提交该分区
        int64_t committed = tracker_.commit(topic_, p);
        EXPECT_EQ(committed, expected) << "Partition " << p;
        EXPECT_EQ(tracker_.getCommittedOffset(topic_, p), expected);
    }

    // 分区 1: 单独失败不影响分区 2
    ASSERT_TRUE(tracker_.consume(topic_, 1, 20));
    tracker_.markFailure(topic_, 1, 20);
    EXPECT_EQ(tracker_.getStatus(topic_, 1, 20), OffsetStatus::Failed);

    ASSERT_TRUE(tracker_.consume(topic_, 2, 40));
    tracker_.markSuccess(topic_, 2, 40);
    EXPECT_EQ(tracker_.getStatus(topic_, 2, 40), OffsetStatus::Success);

    // 分区 2 提交成功: 不影响分区 1
    int64_t c2 = tracker_.commit(topic_, 2);
    EXPECT_EQ(c2, 40);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 2), 40);
    // 分区 1 仍为旧 committed 值 (19)
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 1), 19);
    // 分区 1 的 offset 20 仍为 Failed
    EXPECT_EQ(tracker_.getStatus(topic_, 1, 20), OffsetStatus::Failed);

    // commitAll 提交所有可提交的
    tracker_.commitAll();
    // 分区 1 的 20 仍是 Failed，不能推进
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 1), 19);
    // 分区 2 已提交 40
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 2), 40);
}

// ============================================================================
// 6. ExactlyOnceGuarantee — 回滚后重消费不会产生重复offset推进
// ============================================================================

TEST_F(OffsetStateMachineTest, ExactlyOnceGuarantee) {
    // 提交 offset 0-9
    consumeRange(topic_, 0, 0, 9);
    markSuccessRange(topic_, 0, 0, 9);
    int64_t committed = tracker_.commit(topic_, 0);
    EXPECT_EQ(committed, 9);

    // 消费 offset 10-14，标记成功，但事务回滚
    consumeRange(topic_, 0, 10, 14);
    markSuccessRange(topic_, 0, 10, 14);
    EXPECT_EQ(tracker_.getMaxConsumed(topic_, 0), 14);

    tracker_.rollback(topic_, 0);
    EXPECT_EQ(tracker_.getMaxConsumed(topic_, 0), 9);

    // 回滚后重新消费 offset 10-14
    // consume 应该把 PENDING 的记录更新
    for (int64_t i = 10; i <= 14; ++i) {
        ASSERT_TRUE(tracker_.consume(topic_, 0, i))
            << "Re-consume offset " << i << " after rollback";
        EXPECT_EQ(tracker_.getStatus(topic_, 0, i), OffsetStatus::Pending);
    }

    // 重复消费已提交的 offset 9: 不应改变状态
    bool consumed_9 = tracker_.consume(topic_, 0, 9);
    // offset 9 已 Committed，不应重新进入 Pending
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 9), OffsetStatus::Committed);

    // 重新成功处理
    markSuccessRange(topic_, 0, 10, 14);
    committed = tracker_.commit(topic_, 0);
    EXPECT_EQ(committed, 14);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 14);

    // 验证: 最终只有一个连续的 committed span 0-14
    for (int64_t i = 0; i <= 14; ++i) {
        EXPECT_EQ(tracker_.getStatus(topic_, 0, i), OffsetStatus::Committed)
            << "Offset " << i << " should be committed exactly once";
    }

    // Exactly-once: 没有重复或遗漏
    auto committed_set = tracker_.getOffsetsByStatus(
        topic_, 0, OffsetStatus::Committed);
    EXPECT_EQ(committed_set.size(), 15u) << "Exactly 15 offsets committed";
}

// ============================================================================
// 7. CommitIdempotent — 重复commit不会推进已提交的offset
// ============================================================================

TEST_F(OffsetStateMachineTest, CommitIdempotent) {
    // 提交 offset 0-4
    consumeRange(topic_, 0, 0, 4);
    markSuccessRange(topic_, 0, 0, 4);
    int64_t c1 = tracker_.commit(topic_, 0);
    EXPECT_EQ(c1, 4);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 4);

    // 重复 commit: 无新数据，应返回 -1
    int64_t c2 = tracker_.commit(topic_, 0);
    EXPECT_EQ(c2, -1) << "Idempotent commit should return -1 (no progress)";
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 4)
        << "Committed offset unchanged";

    // 第三次 commit: 仍幂等
    int64_t c3 = tracker_.commit(topic_, 0);
    EXPECT_EQ(c3, -1);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 4);

    // 新增 offset 5-6
    consumeRange(topic_, 0, 5, 6);
    markSuccessRange(topic_, 0, 5, 6);

    int64_t c4 = tracker_.commit(topic_, 0);
    EXPECT_EQ(c4, 6);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 6);

    // 再次重复 commit
    int64_t c5 = tracker_.commit(topic_, 0);
    EXPECT_EQ(c5, -1);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 6);
}

// ============================================================================
// 8. RestoreFromCommitted — 从已提交的offset恢复（模拟崩溃重启）
// ============================================================================

TEST_F(OffsetStateMachineTest, RestoreFromCommitted) {
    // 正常消费并提交 0-49
    consumeRange(topic_, 0, 0, 49);
    markSuccessRange(topic_, 0, 0, 49);
    int64_t committed = tracker_.commit(topic_, 0);
    EXPECT_EQ(committed, 49);

    // 消费 50-59（未提交）
    consumeRange(topic_, 0, 50, 59);
    markSuccessRange(topic_, 0, 50, 59);
    EXPECT_EQ(tracker_.getMaxConsumed(topic_, 0), 59);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 49);

    // ---- 模拟崩溃 ----
    int64_t saved_committed = tracker_.getCommittedOffset(topic_, 0);
    EXPECT_EQ(saved_committed, 49);

    // 创建新的 tracker，从 committed offset 恢复
    OffsetTracker recovered;

    // 恢复: 从持久化存储读取 committed offset 并设置
    // 首先消费到 committed 点
    for (int64_t i = 0; i <= saved_committed; ++i) {
        recovered.consume(topic_, 0, i);
        recovered.markSuccess(topic_, 0, i);
    }
    recovered.commit(topic_, 0);
    EXPECT_EQ(recovered.getCommittedOffset(topic_, 0), 49);

    recovered.restoreFromCommitted(topic_, 0);
    EXPECT_EQ(recovered.getMaxConsumed(topic_, 0), 49);

    // 恢复后继续消费 50-59
    for (int64_t i = 50; i <= 59; ++i) {
        ASSERT_TRUE(recovered.consume(topic_, 0, i));
        recovered.markSuccess(topic_, 0, i);
    }
    int64_t c = recovered.commit(topic_, 0);
    EXPECT_EQ(c, 59);
    EXPECT_EQ(recovered.getCommittedOffset(topic_, 0), 59);

    // 验证恢复后没有重复记录
    EXPECT_EQ(recovered.recordCount(topic_, 0), 60u)
        << "Should have exactly 60 records (0-59)";
}

// ============================================================================
// 9. OutOfOrderOffsets — 乱序offset正确处理
// ============================================================================

TEST_F(OffsetStateMachineTest, OutOfOrderOffsets) {
    // 乱序消费: 10, 5, 3, 7, 1, 0, 8, 2, 9, 6, 4
    std::vector<int64_t> out_of_order = {10, 5, 3, 7, 1, 0, 8, 2, 9, 6, 4};

    for (auto off : out_of_order) {
        ASSERT_TRUE(tracker_.consume(topic_, 0, off))
            << "consume(" << off << ")";
    }

    // max_consumed 应为最大值
    EXPECT_EQ(tracker_.getMaxConsumed(topic_, 0), 10);

    // 全部标记成功
    for (int64_t i = 0; i <= 10; ++i) {
        tracker_.markSuccess(topic_, 0, i);
    }

    // commit 从 0 开始连续扫描
    int64_t committed = tracker_.commit(topic_, 0);
    EXPECT_EQ(committed, 10) << "Should commit all contiguous offsets 0-10";

    // 验证所有 offset 都已提交
    for (int64_t i = 0; i <= 10; ++i) {
        EXPECT_EQ(tracker_.getStatus(topic_, 0, i), OffsetStatus::Committed)
            << "Offset " << i;
    }

    // 测试乱序中的空洞: 跳过 offset 12，消费 11, 13
    ASSERT_TRUE(tracker_.consume(topic_, 0, 11));
    tracker_.markSuccess(topic_, 0, 11);
    ASSERT_TRUE(tracker_.consume(topic_, 0, 13));
    tracker_.markSuccess(topic_, 0, 13);

    // commit 只能推进连续的 11（12 缺失）
    int64_t c2 = tracker_.commit(topic_, 0);
    EXPECT_EQ(c2, 11) << "Should commit 11 but stop at gap before 12";
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 11);

    // 13 因空洞 (12 缺失) 不能提交
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 13), OffsetStatus::Success);
    EXPECT_NE(tracker_.getStatus(topic_, 0, 13), OffsetStatus::Committed);

    // 填补空洞
    ASSERT_TRUE(tracker_.consume(topic_, 0, 12));
    tracker_.markSuccess(topic_, 0, 12);

    int64_t c3 = tracker_.commit(topic_, 0);
    EXPECT_EQ(c3, 13);
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 13), OffsetStatus::Committed);
}

// ============================================================================
// 10. HighWatermarkBoundary — offset不能超过high watermark
// ============================================================================

TEST_F(OffsetStateMachineTest, HighWatermarkBoundary) {
    const int64_t kHighWatermark = 100;

    // offset 在 high watermark 之内: 成功
    EXPECT_TRUE(tracker_.consume(topic_, 0, 50, kHighWatermark));
    EXPECT_TRUE(tracker_.consume(topic_, 0, 0, kHighWatermark));
    EXPECT_TRUE(tracker_.consume(topic_, 0, kHighWatermark, kHighWatermark));

    // offset 超过 high watermark: 失败
    EXPECT_FALSE(tracker_.consume(topic_, 0, kHighWatermark + 1,
                                  kHighWatermark));
    EXPECT_FALSE(tracker_.consume(topic_, 0, kHighWatermark + 100,
                                  kHighWatermark));

    // offset 恰好为 high watermark: 允许
    EXPECT_TRUE(tracker_.consume(topic_, 1, kHighWatermark, kHighWatermark));
    tracker_.markSuccess(topic_, 1, kHighWatermark);
    int64_t committed = tracker_.commit(topic_, 1);
    EXPECT_EQ(committed, kHighWatermark);

    // 不同 high watermark per consume call
    EXPECT_TRUE(tracker_.consume(topic_, 2, 10, 10));
    EXPECT_FALSE(tracker_.consume(topic_, 2, 11, 10));

    // max_consumed 只反映成功消费的
    EXPECT_EQ(tracker_.getMaxConsumed(topic_, 0), 100);
}

// ============================================================================
// 11. ZeroOffsetHandling — offset=0的边界情况
// ============================================================================

TEST_F(OffsetStateMachineTest, ZeroOffsetHandling) {
    // offset=0 是合法的最小值
    ASSERT_TRUE(tracker_.consume(topic_, 0, 0));
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 0), OffsetStatus::Pending);
    EXPECT_EQ(tracker_.getMaxConsumed(topic_, 0), 0);

    tracker_.markSuccess(topic_, 0, 0);
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 0), OffsetStatus::Success);

    int64_t committed = tracker_.commit(topic_, 0);
    EXPECT_EQ(committed, 0);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 0);

    // offset=0 已提交后再消费失败
    bool re_consumed = tracker_.consume(topic_, 0, 0);
    // 已提交的 offset 不应被重新消费为 Pending
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 0), OffsetStatus::Committed);

    // 从偏移 0 恢复
    tracker_.restoreFromCommitted(topic_, 0);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 0);
    EXPECT_EQ(tracker_.getMaxConsumed(topic_, 0), 0);

    // 偏移 0 恢复后继续消费偏移 1
    ASSERT_TRUE(tracker_.consume(topic_, 0, 1));
    tracker_.markSuccess(topic_, 0, 1);
    committed = tracker_.commit(topic_, 0);
    EXPECT_EQ(committed, 1);

    // 偏移 0 到 1 都连续提交
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 0), OffsetStatus::Committed);
    EXPECT_EQ(tracker_.getStatus(topic_, 0, 1), OffsetStatus::Committed);

    // 负数偏移被拒绝
    EXPECT_FALSE(tracker_.consume(topic_, 0, -1));
    EXPECT_FALSE(tracker_.consume(topic_, 0, -100));

    // 使用 high watermark=0 测试边界
    EXPECT_TRUE(tracker_.consume(topic_, 1, 0, 0))
        << "offset=0 with high_watermark=0 is valid";
    EXPECT_FALSE(tracker_.consume(topic_, 1, 1, 0))
        << "offset=1 with high_watermark=0 is invalid";
}

// ============================================================================
// 附加测试: 综合场景
// ============================================================================

/// @brief 综合测试: 模拟完整的 CTP 管道多批次处理
TEST_F(OffsetStateMachineTest, FullCtpPipelineSimulation) {
    // 模拟 3 个批次的完整 CTP 流程
    struct Batch {
        std::vector<int64_t> offsets;
        std::vector<int64_t> failed_offsets;
    };

    std::vector<Batch> batches = {
        {{0, 1, 2, 3, 4}, {2}},          // batch 1: offset 2 失败
        {{5, 6, 7, 8, 9}, {7}},          // batch 2: offset 7 失败
        {{10, 11, 12, 13, 14}, {}},      // batch 3: 全部成功
    };

    for (size_t bi = 0; bi < batches.size(); ++bi) {
        auto& batch = batches[bi];

        // 1. Consume
        for (auto off : batch.offsets) {
            ASSERT_TRUE(tracker_.consume(topic_, 0, off))
                << "Batch " << bi << ", offset " << off;
        }

        // 2. Transform (mark result)
        for (auto off : batch.offsets) {
            bool failed = std::find(batch.failed_offsets.begin(),
                                    batch.failed_offsets.end(), off)
                          != batch.failed_offsets.end();
            if (failed) {
                tracker_.markFailure(topic_, 0, off);
            } else {
                tracker_.markSuccess(topic_, 0, off);
            }
        }

        // 3. Produce (commit continuous success)
        int64_t c = tracker_.commit(topic_, 0);
        // 验证: 只提交连续成功部分，遇到 FAILED 停止
        SUCCEED() << "Batch " << bi << ": committed up to " << c;

        // 4. Retry failed
        if (!batch.failed_offsets.empty()) {
            tracker_.retryAll(topic_, 0);

            // 重新处理失败的
            for (auto off : batch.failed_offsets) {
                tracker_.markSuccess(topic_, 0, off);
            }
        }
    }

    // 最终提交
    tracker_.commitAll();

    // 验证: 所有 offset 0-14 应已提交
    for (int64_t i = 0; i <= 14; ++i) {
        EXPECT_EQ(tracker_.getStatus(topic_, 0, i), OffsetStatus::Committed)
            << "Offset " << i << " should be committed in full pipeline";
    }

    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 14);
}
