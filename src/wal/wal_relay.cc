/**
 * @file wal_relay.cc
 * @brief WalRelay 实现 — WAL 补发中继器
 *
 * ## 模块职责
 * WalRelay 定期扫描 WAL 目录，将之前因 Kafka 不可用而写入本地文件
 * 的告警重新发送（补发）到 Kafka。
 *
 * ## 工作流程
 * ```
 * 定时扫描 (scan_interval_ms)
 *   │
 *   ├─ 遍历 WAL 目录中的 alert-*.log 文件
 *   │
 *   ├─ 跳过最近 2 小时内创建的文件（正在由 WalWriter 写入）
 *   │
 *   ├─ 逐行反序列化 JSON → WgeAlertEvent
 *   │
 *   ├─ 通过 AlertProducer 补发
 *   │
 *   └─ 全部补发成功 → 删除 WAL 文件
 *       部分失败 → 重写文件，保留未补发条目
 *       rename 失败 → 将原始文件移到 .failed 后缀
 * ```
 *
 * ## 关键设计
 * - **安全跳过机制**: 跳过最近 2 小时的文件，防止与 WalWriter 冲突
 * - **部分失败处理**: 不整文件丢弃，而是重写只保留未成功补发的行
 * - **原子 rename**: 使用 .tmp → rename 实现安全的文件重写
 * - **失败标记**: rename 失败时将文件移为 .failed，防止下次扫描重复处理
 */

#include "wal/wal_relay.h"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <fstream>
#include <stdexcept>
#include <sys/stat.h>

#include "google/protobuf/util/json_util.h"
#include "kafka/producer.h"
#include "spdlog/spdlog.h"
#include "wge_alert.pb.h"

#if __has_include("google/protobuf/util/json_util.h")
#include "google/protobuf/util/json_util.h"
#define WGE_HAS_PROTOBUF_JSON_UTIL 1
#else
#define WGE_HAS_PROTOBUF_JSON_UTIL 0
#endif

namespace wge::kafka::wal {

// ============================================================================
// 简易 base64 解码 (与 WalWriter 的 base64 编码配对)
// 在 protobuf JSON util 不可用时，WalWriter 使用 base64(protobuf binary) 格式，
// WalRelay 需要通过 base64 解码还原 protobuf 消息
// ============================================================================

namespace {

/// @brief base64 字符 → 6-bit 值映射
int base64CharValue(char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';       // 0-25
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;  // 26-51
    if (c >= '0' && c <= '9') return c - '0' + 52;  // 52-61
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;  // 无效字符（包括 '='）
}

/// @brief base64 解码
/// 处理标准 base64（含 '=' 填充），忽略空白字符
[[maybe_unused]] std::string base64Decode(const std::string& input) {
    std::string output;
    output.reserve((input.size() * 3) / 4);  // 预估输出大小

    int val = 0;    // 累积的 6-bit 值缓冲区
    int valb = -8;  // 缓冲区中有效位数（负值表示缓冲区为空）
    for (char c : input) {
        if (c == '=') break;  // padding 后结束
        int v = base64CharValue(c);
        if (v < 0) continue;  // 跳过无效字符（如空白）
        val = (val << 6) + v; // 累积 6 bits
        valb += 6;
        if (valb >= 0) {      // 凑满 8 bits 时输出一个字节
            output.push_back(static_cast<char>((val >> valb) & 0xFF));
            valb -= 8;
        }
    }
    return output;
}

}  // namespace

// ============================================================================
// 构造与析构
// ============================================================================

WalRelay::WalRelay(const std::string& wal_dir, AlertProducer& producer)
    : wal_dir_(wal_dir)
    , producer_(producer) {

    if (wal_dir_.empty()) {
        throw std::runtime_error("WalRelay: wal_dir is empty");
    }

    SPDLOG_INFO("WalRelay created: wal_dir={}", wal_dir_);
}

WalRelay::~WalRelay() {
    try {
        stop();
    } catch (const std::exception& e) {
        SPDLOG_ERROR("WalRelay destructor error: {}", e.what());
    } catch (...) {
        SPDLOG_ERROR("WalRelay destructor: unknown error");
    }
}

// ============================================================================
// start / stop
// ============================================================================

void WalRelay::start(int64_t scan_interval_ms) {
    if (running_.exchange(true, std::memory_order_acq_rel)) {
        throw std::runtime_error("WalRelay::start: already running");
    }

    stopped_.store(false, std::memory_order_release);

    relay_thread_ = std::thread(&WalRelay::relayLoop, this, scan_interval_ms);

    SPDLOG_INFO("WalRelay started: scan_interval_ms={}", scan_interval_ms);
}

void WalRelay::stop() {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel,
                                          std::memory_order_relaxed)) {
        return;
    }

    SPDLOG_INFO("WalRelay::stop: signaling relay thread to stop");

    if (relay_thread_.joinable()) {
        relay_thread_.join();
        SPDLOG_INFO("WalRelay::stop: relay thread joined");
    }

    running_.store(false, std::memory_order_release);
}

// ============================================================================
// relayLoop
// ============================================================================

void WalRelay::relayLoop(int64_t scan_interval_ms) {
    SPDLOG_INFO("WalRelay relay loop started");

    while (!stopped_.load(std::memory_order_acquire)) {
        // ===== 等待扫描间隔（可被打断） =====
        // 使用 200ms 粒度检查停止标志，保证停止响应延迟 ≤ 200ms
        auto wait_start = std::chrono::steady_clock::now();
        while (!stopped_.load(std::memory_order_acquire) &&
               std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - wait_start)
                       .count() < scan_interval_ms) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }

        if (stopped_.load(std::memory_order_acquire)) {
            break;
        }

        // ===== 扫描 WAL 目录 =====
        DIR* dir = ::opendir(wal_dir_.c_str());
        if (!dir) {
            SPDLOG_WARN("WalRelay: failed to open WAL dir '{}': {}",
                        wal_dir_, std::strerror(errno));
            continue;  // 目录打开失败，等待下次扫描重试
        }
        // RAII: 确保 DIR* 在任何情况下（包括异常）都被关闭
        struct DirDeleter {
            void operator()(DIR* d) const { if (d) ::closedir(d); }
        };
        std::unique_ptr<DIR, DirDeleter> dir_guard(dir);

        int64_t total_relayed = 0;
        int64_t files_processed = 0;

        struct dirent* entry;
        while ((entry = ::readdir(dir)) != nullptr) {
            if (stopped_.load(std::memory_order_acquire)) {
                break;
            }

            // 匹配 WAL 文件命名: alert-YYYYMMDD-HH.log
            // 最小长度: "alert-" (6) + "YYYYMMDD-HH" (11) + ".log" (4) = 至少 17
            std::string name(entry->d_name);
            if (name.size() < 10 || name.substr(0, 6) != "alert-" ||
                name.substr(name.size() - 4) != ".log") {
                continue;
            }

            // 路径遍历防护: 拒绝包含 / 或 .. 的文件名
            // 防止恶意构造的文件名逃逸 wal_dir_ 目录
            if (name.find('/') != std::string::npos ||
                name.find("..") != std::string::npos) {
                SPDLOG_WARN("WalRelay: skipping suspicious filename '{}'", name);
                continue;
            }

            std::string file_path = wal_dir_;
            if (!file_path.empty() && file_path.back() != '/') {
                file_path += '/';
            }
            file_path += name;

            // 跳过当前小时的文件 (正在由 WalWriter 写入)
            // 通过文件修改时间判断：最近 2 小时内修改的文件跳过
            // 这避免了与 WalWriter 的写竞争

            struct stat file_stat;
            if (::stat(file_path.c_str(), &file_stat) == 0) {
                auto now = std::time(nullptr);
                auto age_sec = now - file_stat.st_mtime;
                constexpr int64_t kSkipAgeSec = 7200;  // 2 小时 = 7200 秒

                if (age_sec < kSkipAgeSec) {
                    SPDLOG_DEBUG("WalRelay: skipping recent file '{}' (age={}s)",
                                 name, age_sec);
                    continue;
                }
            }

            // 处理 WAL 文件
            try {
                int64_t relayed = processWalFile(file_path);
                if (relayed > 0) {
                    total_relayed += relayed;
                    ++files_processed;
                    SPDLOG_INFO("WalRelay: relayed {} alerts from '{}'",
                                relayed, name);
                }
            } catch (const std::exception& e) {
                SPDLOG_ERROR("WalRelay: failed to process WAL file '{}': {}",
                             file_path, e.what());
            }
        }
        // dir_guard 析构时自动调用 ::closedir(dir)，无需手动关闭

        if (total_relayed > 0) {
            SPDLOG_INFO("WalRelay: scan complete — relayed {} alerts from {} files",
                        total_relayed, files_processed);
        }
    }

    SPDLOG_INFO("WalRelay relay loop exiting");
}

// ============================================================================
// processWalFile
// ============================================================================

int64_t WalRelay::processWalFile(const std::string& file_path) {
    std::ifstream infile(file_path);
    if (!infile.is_open()) {
        SPDLOG_WARN("WalRelay: failed to open WAL file '{}' for reading", file_path);
        return 0;
    }

    // 读取所有行
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(infile, line)) {
        if (!line.empty()) {
            lines.push_back(line);
        }
    }
    infile.close();

    if (lines.empty()) {
        // 空文件，直接删除
        if (std::remove(file_path.c_str()) != 0) {
            SPDLOG_WARN("WalRelay: failed to remove empty WAL file '{}': {}",
                        file_path, std::strerror(errno));
        }
        return 0;
    }

    // 转为逐个标记模式 — 用 vector<bool> 记录每行补发状态
    std::vector<bool> relayed_flags(lines.size(), false);
    int64_t relayed = 0;

    // 逐行反序列化并补发
    for (size_t line_idx = 0; line_idx < lines.size(); ++line_idx) {
        if (stopped_.load(std::memory_order_acquire)) {
            break;
        }

        const auto& json_line = lines[line_idx];

        // 反序列化 JSON → WgeAlertEvent
        auto alert = std::make_shared<WgeAlertEvent>();

#if WGE_HAS_PROTOBUF_JSON_UTIL
        google::protobuf::util::JsonParseOptions options;
        options.ignore_unknown_fields = true;

        auto status = google::protobuf::util::JsonStringToMessage(
            json_line, alert.get(), options);

        if (!status.ok()) {
            SPDLOG_WARN("WalRelay: failed to parse WAL line from '{}': {}",
                        file_path, status.message().ToString());
            continue;
        }
#else
        // Fallback: base64 解码 → 二进制 protobuf 解析 (与 WalWriter 编码配对)
        std::string binary = base64Decode(json_line);
        if (!alert->ParseFromString(binary)) {
            SPDLOG_WARN("WalRelay: failed to parse WAL line from '{}' "
                        "(protobuf JSON util not available)",
                        file_path);
            continue;
        }
#endif

        // 补发到 AlertProducer
        try {
            producer_.sendAlert(alert);
            relayed_flags[line_idx] = true;
            ++relayed;
            SPDLOG_DEBUG("WalRelay: relayed alert_id={}", alert->alert_id());
        } catch (const std::exception& e) {
            SPDLOG_ERROR("WalRelay: failed to relay alert_id={}: {}",
                         alert->alert_id(), e.what());
        }
    }

    // 若全部补发成功，删除 WAL 文件
    if (relayed == static_cast<int64_t>(lines.size())) {
        if (std::remove(file_path.c_str()) == 0) {
            SPDLOG_INFO("WalRelay: removed fully relayed WAL file '{}'", file_path);
        } else {
            SPDLOG_WARN("WalRelay: failed to remove WAL file '{}': {}",
                        file_path, std::strerror(errno));
        }
    } else {
        // 部分补发失败 — 重写文件，只保留未补发的条目
        std::string tmp_path = file_path + ".tmp";
        std::ofstream outfile(tmp_path, std::ios::trunc);
        if (outfile.is_open()) {
            for (size_t i = 0; i < lines.size(); ++i) {
                if (!relayed_flags[i]) {
                    outfile << lines[i] << '\n';
                }
            }
            // 检查写入是否成功（磁盘满等）
            if (!outfile.good()) {
                SPDLOG_ERROR("WalRelay: write error while rewriting WAL file '{}'",
                             tmp_path);
            }
            outfile.close();
            // 检查 close 是否成功（flush 失败等）
            if (outfile.fail()) {
                SPDLOG_ERROR("WalRelay: close error while rewriting WAL file '{}'",
                             tmp_path);
                std::remove(tmp_path.c_str());
                return relayed;  // 保留原始文件，下次扫描重试
            }

            if (std::rename(tmp_path.c_str(), file_path.c_str()) != 0) {
                SPDLOG_ERROR("WalRelay: failed to rename tmp file: {}",
                             std::strerror(errno));
                std::remove(tmp_path.c_str());
                // rename 失败: 将原始文件移到 .failed 后缀，防止下次扫描重复补发
                std::string failed_path = file_path + ".failed";
                if (std::rename(file_path.c_str(), failed_path.c_str()) != 0) {
                    SPDLOG_ERROR("WalRelay: failed to rename to .failed '{}': {}",
                                 failed_path, std::strerror(errno));
                } else {
                    SPDLOG_WARN("WalRelay: original file renamed to '{}' for manual handling",
                                failed_path);
                }
            }
        }
        SPDLOG_INFO("WalRelay: {} of {} alerts relayed from '{}', "
                    "remaining {} kept in file",
                    relayed, lines.size(), file_path,
                    static_cast<int64_t>(lines.size()) - relayed);
    }

    return relayed;
}

}  // namespace wge::kafka::wal
