#pragma once

/**
 * @file wal_relay.h
 * @brief WalRelay — WAL 补发中继器
 *
 * WalRelay 在后台线程中定期扫描 WAL 文件目录，查找:
 * - 已写入 WAL 但可能未成功发送到 Kafka 的告警
 * - 进程重启后历史 WAL 文件中未清理的告警
 *
 * 补发策略:
 * - 扫描当前及历史 hour 的 WAL 文件
 * - 逐行读取 JSON，反序列化并重发到 AlertProducer
 * - 补发成功后从 WAL 文件中删除对应条目 (或标记为已发送)
 *
 * 线程安全: 内部单线程运行，start/stop 使用 atomic flag 同步。
 */

#include <atomic>
#include <memory>
#include <string>
#include <thread>

namespace wge::kafka {

// 前向声明
class AlertProducer;

namespace wal {

/**
 * @brief WAL 补发中继器
 *
 * 后台线程定期扫描 WAL 目录，补发未成功投递的告警。
 *
 * 使用示例:
 * @code
 *   WalRelay relay("/var/lib/wge/wal", producer);
 *   relay.start();
 *   // ...
 *   relay.stop();
 * @endcode
 */
class WalRelay {
public:
    /**
     * @brief 构造函数
     *
     * @param wal_dir  WAL 文件目录路径
     * @param producer 告警生产者引用 (生命周期必须长于 WalRelay)
     */
    WalRelay(const std::string& wal_dir, AlertProducer& producer);

    /**
     * @brief 析构函数
     *
     * 若尚未调用 stop()，自动停止。
     * 异常内部捕获并记录。
     */
    ~WalRelay();

    // 禁止拷贝和移动
    WalRelay(const WalRelay&) = delete;
    WalRelay& operator=(const WalRelay&) = delete;
    WalRelay(WalRelay&&) = delete;
    WalRelay& operator=(WalRelay&&) = delete;

    /**
     * @brief 启动后台补发线程
     *
     * 创建独立线程，定期扫描 WAL 目录并补发告警。
     *
     * @param scan_interval_ms 扫描间隔 (ms)，默认 5000
     * @throws std::runtime_error 若已启动
     */
    void start(int64_t scan_interval_ms = 5000);

    /**
     * @brief 停止后台补发线程
     *
     * 设置停止标志，等待线程退出。
     *
     * @note 幂等: 多次调用安全
     * @note 阻塞直到线程退出
     */
    void stop();

    /**
     * @brief 检查是否在运行
     */
    [[nodiscard]] bool isRunning() const noexcept {
        return running_.load(std::memory_order_acquire);
    }

private:
    /**
     * @brief 后台补发循环
     *
     * 定期扫描 WAL 目录，对每个 WAL 文件:
     * 1. 逐行读取 JSON
     * 2. 反序列化为 WgeAlertEvent
     * 3. 调用 producer.sendAlert() 补发
     * 4. 若成功，记录行号
     * 5. 所有行处理完毕后，重写文件 (移除已补发条目)
     *
     * @param scan_interval_ms 扫描间隔
     */
    void relayLoop(int64_t scan_interval_ms);

    /**
     * @brief 处理单个 WAL 文件
     *
     * 读取所有行，逐行补发，成功后移除已补发行。
     *
     * @param file_path WAL 文件路径
     * @return int64_t 成功补发的告警数
     */
    int64_t processWalFile(const std::string& file_path);

    /// @brief WAL 目录路径
    std::string wal_dir_;

    /// @brief 告警生产者引用
    AlertProducer& producer_;

    /// @brief 后台线程
    std::thread relay_thread_;

    /// @brief 运行标志
    std::atomic<bool> running_{false};

    /// @brief 停止标志
    std::atomic<bool> stopped_{false};
};

}  // namespace wal
}  // namespace wge::kafka
