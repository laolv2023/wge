#pragma once

/**
 * @file wal_writer.h
 * @brief WalWriter — Write-Ahead Log 写入器
 *
 * WalWriter 提供线程安全的告警事件预写日志 (WAL) 写入能力。
 * 每条告警在发送到 Kafka 之前先写入 WAL 文件，确保:
 * - Kafka 发送失败时可从 WAL 恢复 (由 WalRelay 负责)
 * - 进程崩溃时可从 WAL 重放
 *
 * WAL 格式:
 * - 文件位置: /var/lib/wge/wal/alert-YYYYMMDD-HH.log
 * - 每条告警一行 JSON，append-only
 * - 文件名按小时轮转
 *
 * 线程安全: 内部使用 mutex 保护文件写入，可从多线程调用 write()。
 */

#include <ctime>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

namespace wge::kafka {

// 前向声明
class WgeAlertEvent;

namespace wal {

// ============================================================================
// WalWriter
// ============================================================================

/**
 * @brief WAL 写入器
 *
 * 将告警事件以 JSON 格式写入 WAL 文件，一条一行，append-only。
 * 文件名按小时自动轮转 (alert-YYYYMMDD-HH.log)。
 *
 * 使用示例:
 * @code
 *   WalWriter writer("/var/lib/wge/wal");
 *   writer.write(alert_event);
 * @endcode
 */
class WalWriter {
public:
    /**
     * @brief 构造函数
     *
     * 创建 WAL 目录 (若不存在)。不打开文件 (write 时按需打开)。
     *
     * @param wal_dir WAL 文件目录路径
     * @throws std::runtime_error 若无法创建目录
     */
    explicit WalWriter(const std::string& wal_dir);

    /**
     * @brief 析构函数
     *
     * 关闭当前打开的 WAL 文件。
     * 异常内部捕获并记录。
     */
    ~WalWriter();

    // 禁止拷贝和移动
    WalWriter(const WalWriter&) = delete;
    WalWriter& operator=(const WalWriter&) = delete;
    WalWriter(WalWriter&&) = delete;
    WalWriter& operator=(WalWriter&&) = delete;

    /**
     * @brief 写入告警事件到 WAL
     *
     * 将 WgeAlertEvent 序列化为 JSON 并追加到当前小时的 WAL 文件。
     * 若小时变化则自动轮转文件。
     *
     * @param alert 告警事件 (shared_ptr)
     * @throws std::runtime_error 若写入失败 (磁盘满等)
     *
     * @note 线程安全: 可从多线程调用
     * @note 若 alert 为 nullptr，静默忽略
     */
    void write(const std::shared_ptr<WgeAlertEvent>& alert);

    /**
     * @brief 获取 WAL 目录路径
     */
    [[nodiscard]] const std::string& walDir() const noexcept {
        return wal_dir_;
    }

    /**
     * @brief 强制 flush 当前文件
     *
     * 将缓冲数据同步到磁盘。
     */
    void flush();

private:
    /**
     * @brief 获取当前小时对应的 WAL 文件名
     *
     * 格式: alert-YYYYMMDD-HH.log
     *
     * @param t 时间 (默认当前时间)
     * @return std::string 完整文件路径
     */
    [[nodiscard]] std::string currentWalFilePath(const std::tm* t = nullptr) const;

    /**
     * @brief 打开或轮转 WAL 文件
     *
     * 若当前文件路径与期望路径不一致 (小时变化) 则关闭旧文件、打开新文件。
     *
     * @param now 当前时间 (tm 结构)
     */
    void rotateIfNeeded(const std::tm& now);

    /**
     * @brief 将 WgeAlertEvent 序列化为单行 JSON
     *
     * 使用 protobuf 的 JSON 序列化 (Utf8DebugString 或 MessageToJsonString)。
     *
     * @param alert 告警事件
     * @return std::string 单行 JSON
     */
    [[nodiscard]] static std::string serializeToJsonLine(const WgeAlertEvent& alert);

    /// @brief WAL 目录路径
    std::string wal_dir_;

    /// @brief 当前打开的文件路径
    std::string current_file_path_;

    /// @brief 当前打开的文件句柄 (FILE*)
    FILE* current_file_{nullptr};

    /// @brief 线程安全互斥锁
    mutable std::mutex mutex_;
};

}  // namespace wal
}  // namespace wge::kafka
