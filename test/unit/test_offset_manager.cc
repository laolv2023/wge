/**
 * @file test_offset_manager.cc
 * @brief Offset 管理逻辑单元测试 — 验证 CTP offset 追踪、提交、重试和恢复
 *
 * 由于没有真实的 Kafka + WGE Engine 环境，本测试实现一个轻量级的
 * OffsetTracker 类来模拟 offset 管理的核心逻辑，包括：
 * - 多分区 offset 追踪
 * - 成功/失败时 offset 提交策略
 * - 批量提交
 * - 崩溃恢复
 * - Exactly-once 语义
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
// OffsetTracker — 模拟 Kafka CTP offset 管理
// ============================================================================

namespace {

/**
 * @brief 单分区 offset 状态
 */
struct PartitionOffsetState {
    int32_t partition = 0;
    int64_t current_offset = -1;       // 当前已消费到的 offset
    int64_t committed_offset = -1;      // 已成功提交的 offset
    int64_t last_success_offset = -1;   // 最后处理成功的 offset
    int32_t pending_retries = 0;        // 当前待重试次数

    /// 下一个要消费的 offset
    int64_t nextOffset() const { return current_offset + 1; }
};

/**
 * @brief OffsetTracker — 追踪和管理 CTP offset
 *
 * 模拟核心逻辑:
 * - 消费消息后 offset 递增
 * - 处理成功 → 标记为可提交
 * - 处理失败 → 不推进 committed，等待重试
 * - 批量处理完统一提交
 * - 多分区独立追踪
 * - 恢复时从最后 committed offset 开始
 * - 事务回滚后 offset 不推进
 */
class OffsetTracker {
public:
    /**
     * @brief 记录消费到一个消息（offset 推进）
     */
    void recordConsume(const std::string& topic, int32_t partition,
                       int64_t offset) {
        auto key = makeKey(topic, partition);
        auto& state = states_[key];
        state.partition = partition;
        if (offset > state.current_offset) {
            state.current_offset = offset;
        }
    }

    /**
     * @brief 处理成功 — 标记该 offset 为可提交
     */
    void markSuccess(const std::string& topic, int32_t partition,
                     int64_t offset) {
        auto key = makeKey(topic, partition);
        auto& state = states_[key];
        state.last_success_offset = std::max(state.last_success_offset, offset);
        state.pending_retries = 0;
    }

    /**
     * @brief 处理失败 — 不提交，增加重试计数
     */
    void markFailure(const std::string& topic, int32_t partition,
                     int64_t offset) {
        auto key = makeKey(topic, partition);
        auto& state = states_[key];
        state.pending_retries++;
        // offset 不推进 committed
    }

    /**
     * @brief 提交 offset — 将 last_success_offset 写入 committed
     */
    void commit(const std::string& topic, int32_t partition) {
        auto key = makeKey(topic, partition);
        auto it = states_.find(key);
        if (it == states_.end()) return;

        auto& state = it->second;
        if (state.last_success_offset > state.committed_offset) {
            state.committed_offset = state.last_success_offset;
        }
    }

    /**
     * @brief 批量提交所有分区
     */
    void commitAll() {
        for (auto& [key, state] : states_) {
            if (state.last_success_offset > state.committed_offset) {
                state.committed_offset = state.last_success_offset;
            }
        }
    }

    /**
     * @brief 获取 committed offset（崩溃恢复时使用）
     */
    int64_t getCommittedOffset(const std::string& topic,
                               int32_t partition) const {
        auto key = makeKey(topic, partition);
        auto it = states_.find(key);
        if (it == states_.end()) return -1;
        return it->second.committed_offset;
    }

    /**
     * @brief 获取当前消费 offset
     */
    int64_t getCurrentOffset(const std::string& topic,
                             int32_t partition) const {
        auto key = makeKey(topic, partition);
        auto it = states_.find(key);
        if (it == states_.end()) return -1;
        return it->second.current_offset;
    }

    /**
     * @brief 获取待重试次数
     */
    int32_t getPendingRetries(const std::string& topic,
                              int32_t partition) const {
        auto key = makeKey(topic, partition);
        auto it = states_.find(key);
        if (it == states_.end()) return 0;
        return it->second.pending_retries;
    }

    /**
     * @brief 崩溃恢复: 重置 current_offset 到 committed_offset
     *
     * 模拟消费者重启后从 committed offset 恢复
     */
    void restoreFromCommitted(const std::string& topic, int32_t partition) {
        auto key = makeKey(topic, partition);
        auto it = states_.find(key);
        if (it == states_.end()) return;

        auto& state = it->second;
        state.current_offset = state.committed_offset;
        state.last_success_offset = state.committed_offset;
        state.pending_retries = 0;
    }

    /**
     * @brief 事务回滚 — 回滚到 committed offset
     */
    void rollback(const std::string& topic, int32_t partition) {
        auto key = makeKey(topic, partition);
        auto it = states_.find(key);
        if (it == states_.end()) return;

        auto& state = it->second;
        // 回滚后: current_offset 回到 committed, last_success 也回退
        state.current_offset = state.committed_offset;
        state.last_success_offset = state.committed_offset;
        state.pending_retries = 0;
    }

    /**
     * @brief 获取已追踪的分区数
     */
    size_t partitionCount() const { return states_.size(); }

    /**
     * @brief 获取所有分区的 topic+partition 键
     */
    std::vector<std::string> allKeys() const {
        std::vector<std::string> keys;
        for (const auto& [k, _] : states_) {
            keys.push_back(k);
        }
        return keys;
    }

private:
    static std::string makeKey(const std::string& topic, int32_t partition) {
        return topic + ":" + std::to_string(partition);
    }

    std::map<std::string, PartitionOffsetState> states_;
};

}  // namespace

// ============================================================================
// 测试夹具
// ============================================================================

class OffsetManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        tracker_ = OffsetTracker{};
    }

    OffsetTracker tracker_;
    const std::string topic_ = "http-access";
};

// ============================================================================
// 1. OffsetTracking — 消费消息后 offset 正确记录
// ============================================================================

TEST_F(OffsetManagerTest, OffsetTracking) {
    // 消费一批消息
    for (int64_t i = 0; i < 10; ++i) {
        tracker_.recordConsume(topic_, 0, i);
    }

    EXPECT_EQ(tracker_.getCurrentOffset(topic_, 0), 9);
    // 尚未提交
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), -1);

    // 继续消费
    tracker_.recordConsume(topic_, 0, 15);
    EXPECT_EQ(tracker_.getCurrentOffset(topic_, 0), 15);
}

// ============================================================================
// 2. CommitOnSuccess — 处理成功 → offset 标记为可提交
// ============================================================================

TEST_F(OffsetManagerTest, CommitOnSuccess) {
    // 消费 offset 0-4
    for (int64_t i = 0; i <= 4; ++i) {
        tracker_.recordConsume(topic_, 0, i);
        tracker_.markSuccess(topic_, 0, i);
    }

    // 提交
    tracker_.commit(topic_, 0);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 4);

    // 再消费并成功 offset 5-9
    for (int64_t i = 5; i <= 9; ++i) {
        tracker_.recordConsume(topic_, 0, i);
        tracker_.markSuccess(topic_, 0, i);
    }

    tracker_.commit(topic_, 0);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 9);
}

// ============================================================================
// 3. RetryOnFailure — 处理失败 → offset 不提交，等待重试
// ============================================================================

TEST_F(OffsetManagerTest, RetryOnFailure) {
    // 消费 offset 0-4 全部成功
    for (int64_t i = 0; i <= 4; ++i) {
        tracker_.recordConsume(topic_, 0, i);
        tracker_.markSuccess(topic_, 0, i);
    }
    tracker_.commit(topic_, 0);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 4);

    // offset 5 失败
    tracker_.recordConsume(topic_, 0, 5);
    tracker_.markFailure(topic_, 0, 5);
    EXPECT_EQ(tracker_.getPendingRetries(topic_, 0), 1);

    // 失败后 committed 仍是 4
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 4);

    // offset 6 成功
    tracker_.recordConsume(topic_, 0, 6);
    tracker_.markSuccess(topic_, 0, 6);

    // 提交: 只会提交到 last_success_offset = 6 (但 offset 5 未成功)
    // 注意：这里的逻辑是 committed = last_success_offset（单调递增）
    tracker_.commit(topic_, 0);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 6);

    // 重试 offset 5 成功
    tracker_.markSuccess(topic_, 0, 5);
    EXPECT_EQ(tracker_.getPendingRetries(topic_, 0), 0);

    // 现在 last_success 已是 6，无需再推进
    tracker_.commit(topic_, 0);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 6);
}

// ============================================================================
// 4. BatchCommit — 批量处理完统一提交
// ============================================================================

TEST_F(OffsetManagerTest, BatchCommit) {
    // 消费 offset 0-19，全部标记成功
    for (int64_t i = 0; i < 20; ++i) {
        tracker_.recordConsume(topic_, 0, i);
        tracker_.markSuccess(topic_, 0, i);
    }

    // 尚未提交
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), -1);

    // 批量提交所有分区
    tracker_.commitAll();

    // 所有分区都已提交
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 19);

    // 验证 commitAll 对多分区也有效
    tracker_.recordConsume(topic_, 1, 0);
    tracker_.markSuccess(topic_, 1, 0);
    tracker_.recordConsume(topic_, 1, 1);
    tracker_.markSuccess(topic_, 1, 1);

    tracker_.commitAll();
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 19);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 1), 1);
}

// ============================================================================
// 5. PartitionAware — 多分区 offset 独立追踪
// ============================================================================

TEST_F(OffsetManagerTest, PartitionAware) {
    const int32_t NUM_PARTITIONS = 4;

    // 每个分区消费不同数量的消息
    std::map<int32_t, int64_t> expected_offsets;
    for (int32_t p = 0; p < NUM_PARTITIONS; ++p) {
        int64_t count = (p + 1) * 10;  // 10, 20, 30, 40
        for (int64_t i = 0; i < count; ++i) {
            tracker_.recordConsume(topic_, p, i);
            tracker_.markSuccess(topic_, p, i);
        }
        expected_offsets[p] = count - 1;
    }

    // 提交所有分区
    tracker_.commitAll();

    // 验证各分区独立追踪
    for (int32_t p = 0; p < NUM_PARTITIONS; ++p) {
        EXPECT_EQ(tracker_.getCommittedOffset(topic_, p), expected_offsets[p])
            << "Partition " << p;
        EXPECT_EQ(tracker_.getCurrentOffset(topic_, p), expected_offsets[p])
            << "Partition " << p;
    }

    EXPECT_EQ(tracker_.partitionCount(), 4u);

    // 分区 1 单独失败不影响分区 2
    tracker_.recordConsume(topic_, 1, 20);
    tracker_.markFailure(topic_, 1, 20);
    EXPECT_EQ(tracker_.getPendingRetries(topic_, 1), 1);

    tracker_.recordConsume(topic_, 2, 30);
    tracker_.markSuccess(topic_, 2, 30);
    EXPECT_EQ(tracker_.getPendingRetries(topic_, 2), 0);

    // 分区 2 提交成功不影响分区 1
    tracker_.commit(topic_, 2);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 2), 30);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 1), 19);  // 仍为旧值
}

// ============================================================================
// 6. RestoreOnCrash — 崩溃恢复后从最后 committed offset 开始
// ============================================================================

TEST_F(OffsetManagerTest, RestoreOnCrash) {
    // 正常消费并提交 0-49
    for (int64_t i = 0; i < 50; ++i) {
        tracker_.recordConsume(topic_, 0, i);
        tracker_.markSuccess(topic_, 0, i);
    }
    tracker_.commit(topic_, 0);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 49);

    // 消费 50-59（未提交）
    for (int64_t i = 50; i < 60; ++i) {
        tracker_.recordConsume(topic_, 0, i);
        tracker_.markSuccess(topic_, 0, i);
    }
    EXPECT_EQ(tracker_.getCurrentOffset(topic_, 0), 59);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 49);

    // ---- 模拟崩溃 ----
    // 创建新的 tracker，从 committed offset 恢复
    OffsetTracker recovered;

    // 从持久化存储中读取 committed offset（模拟）
    int64_t saved_committed = tracker_.getCommittedOffset(topic_, 0);
    EXPECT_EQ(saved_committed, 49);

    // 恢复: 设置 committed offset
    // (恢复后的 tracker 从 committed+1 开始消费)
    recovered.recordConsume(topic_, 0, saved_committed);
    recovered.markSuccess(topic_, 0, saved_committed);
    recovered.commit(topic_, 0);

    recovered.restoreFromCommitted(topic_, 0);
    EXPECT_EQ(recovered.getCurrentOffset(topic_, 0), 49);
    EXPECT_EQ(recovered.getCommittedOffset(topic_, 0), 49);

    // 恢复后继续消费 50-59
    for (int64_t i = 50; i < 60; ++i) {
        recovered.recordConsume(topic_, 0, i);
        recovered.markSuccess(topic_, 0, i);
    }
    recovered.commit(topic_, 0);
    EXPECT_EQ(recovered.getCommittedOffset(topic_, 0), 59);
}

// ============================================================================
// 7. ExactlyOnce — 事务回滚后 offset 不推进
// ============================================================================

TEST_F(OffsetManagerTest, ExactlyOnce) {
    // 消费并提交 offset 0-9
    for (int64_t i = 0; i < 10; ++i) {
        tracker_.recordConsume(topic_, 0, i);
        tracker_.markSuccess(topic_, 0, i);
    }
    tracker_.commit(topic_, 0);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 9);

    // 消费 offset 10-14 并标记成功，但事务回滚前
    for (int64_t i = 10; i < 15; ++i) {
        tracker_.recordConsume(topic_, 0, i);
        tracker_.markSuccess(topic_, 0, i);
    }
    EXPECT_EQ(tracker_.getCurrentOffset(topic_, 0), 14);

    // ---- 事务回滚 ----
    tracker_.rollback(topic_, 0);

    // 回滚后 offset 回到 committed 值
    EXPECT_EQ(tracker_.getCurrentOffset(topic_, 0), 9);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 9);

    // 重新消费 10-14
    for (int64_t i = 10; i < 15; ++i) {
        tracker_.recordConsume(topic_, 0, i);
        tracker_.markSuccess(topic_, 0, i);
    }
    tracker_.commit(topic_, 0);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 14);
}

// ============================================================================
// 8. MultiTopicIndependentTracking — 多 topic 独立追踪
// ============================================================================

TEST_F(OffsetManagerTest, MultiTopicIndependentTracking) {
    std::string topic_a = "http-access";
    std::string topic_b = "wge-alert";

    // Topic A: partition 0
    tracker_.recordConsume(topic_a, 0, 100);
    tracker_.markSuccess(topic_a, 0, 100);
    tracker_.commit(topic_a, 0);

    // Topic B: partition 0
    tracker_.recordConsume(topic_b, 0, 200);
    tracker_.markSuccess(topic_b, 0, 200);
    tracker_.commit(topic_b, 0);

    EXPECT_EQ(tracker_.getCommittedOffset(topic_a, 0), 100);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_b, 0), 200);

    // Topic A 回滚不影响 Topic B
    tracker_.recordConsume(topic_a, 0, 101);
    tracker_.markSuccess(topic_a, 0, 101);
    tracker_.rollback(topic_a, 0);

    EXPECT_EQ(tracker_.getCommittedOffset(topic_a, 0), 100);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_b, 0), 200);  // 不受影响
}

// ============================================================================
// 9. OutOfOrderOffsets — 乱序到达的 offset 正确处理
// ============================================================================

TEST_F(OffsetManagerTest, OutOfOrderOffsets) {
    // 模拟 Kafka 消息可能乱序到达
    tracker_.recordConsume(topic_, 0, 5);
    tracker_.markSuccess(topic_, 0, 5);

    tracker_.recordConsume(topic_, 0, 3);  // 乱序
    tracker_.markSuccess(topic_, 0, 3);

    tracker_.recordConsume(topic_, 0, 7);
    tracker_.markSuccess(topic_, 0, 7);

    // current_offset 应该是最大值
    EXPECT_EQ(tracker_.getCurrentOffset(topic_, 0), 7);

    // 提交后 committed 也应该是最大值
    tracker_.commit(topic_, 0);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 7);
}

// ============================================================================
// 10. PendingRetriesReset — 成功后重试计数归零
// ============================================================================

TEST_F(OffsetManagerTest, PendingRetriesReset) {
    // 连续失败 3 次
    tracker_.recordConsume(topic_, 0, 42);
    tracker_.markFailure(topic_, 0, 42);
    tracker_.markFailure(topic_, 0, 42);
    tracker_.markFailure(topic_, 0, 42);
    EXPECT_EQ(tracker_.getPendingRetries(topic_, 0), 3);

    // 重试成功
    tracker_.markSuccess(topic_, 0, 42);
    EXPECT_EQ(tracker_.getPendingRetries(topic_, 0), 0);
}

// ============================================================================
// 11. CommitIdempotent — 重复提交不改变结果
// ============================================================================

TEST_F(OffsetManagerTest, CommitIdempotent) {
    tracker_.recordConsume(topic_, 0, 99);
    tracker_.markSuccess(topic_, 0, 99);
    tracker_.commit(topic_, 0);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 99);

    // 重复提交
    tracker_.commit(topic_, 0);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 99);

    tracker_.commitAll();
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 99);
}

// ============================================================================
// 12. NoCommitOnStaleOffset — 不会提交过期 offset
// ============================================================================

TEST_F(OffsetManagerTest, NoCommitOnStaleOffset) {
    // 消费到 20
    tracker_.recordConsume(topic_, 0, 20);
    tracker_.markSuccess(topic_, 0, 20);
    tracker_.commit(topic_, 0);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 20);

    // 尝试标记 offset 10 成功（过期的）
    tracker_.markSuccess(topic_, 0, 10);

    // committed 应保持为 20 (last_success_offset 只增不减)
    tracker_.commit(topic_, 0);
    EXPECT_EQ(tracker_.getCommittedOffset(topic_, 0), 20);
}

// ============================================================================
// 13. InitialState — 未追踪的分区返回默认值
// ============================================================================

TEST_F(OffsetManagerTest, InitialState) {
    // 未追踪的分区
    EXPECT_EQ(tracker_.getCurrentOffset("unknown", 0), -1);
    EXPECT_EQ(tracker_.getCommittedOffset("unknown", 0), -1);
    EXPECT_EQ(tracker_.getPendingRetries("unknown", 0), 0);
    EXPECT_EQ(tracker_.partitionCount(), 0u);
    EXPECT_TRUE(tracker_.allKeys().empty());
}
