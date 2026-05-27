/**
 * @file wal_relay.cc
 * @brief WalRelay 实现 — WAL 补发中继器
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
// ============================================================================

namespace {

int base64CharValue(char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;  // invalid
}

std::string base64Decode(const std::string& input) {
    std::string output;
    output.reserve((input.size() * 3) / 4);

    int val = 0;
    int valb = -8;
    for (char c : input) {
        if (c == '=') break;
        int v = base64CharValue(c);
        if (v < 0) continue;  // skip invalid chars (e.g. whitespace)
        val = (val << 6) + v;
        valb += 6;
        if (valb >= 0) {
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
        // 等待扫描间隔 (可被打断)
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

        // 扫描 WAL 目录
        DIR* dir = ::opendir(wal_dir_.c_str());
        if (!dir) {
            SPDLOG_WARN("WalRelay: failed to open WAL dir '{}': {}",
                        wal_dir_, std::strerror(errno));
            continue;
        }

        int64_t total_relayed = 0;
        int64_t files_processed = 0;

        struct dirent* entry;
        while ((entry = ::readdir(dir)) != nullptr) {
            if (stopped_.load(std::memory_order_acquire)) {
                break;
            }

            // 匹配 WAL 文件命名: alert-*.log
            // 最小长度: "alert-" (6) + ".log" (4) = 10
            std::string name(entry->d_name);
            if (name.size() < 10 || name.substr(0, 6) != "alert-" ||
                name.substr(name.size() - 4) != ".log") {
                continue;
            }

            std::string file_path = wal_dir_;
            if (!file_path.empty() && file_path.back() != '/') {
                file_path += '/';
            }
            file_path += name;

            // 跳过当前小时的文件 (正在由 WalWriter 写入)
            // 可以通过比较文件名中的时间戳来判断
            // 简化实现: 跳过最近 2 小时内创建的文件

            struct stat file_stat;
            if (::stat(file_path.c_str(), &file_stat) == 0) {
                auto now = std::time(nullptr);
                auto age_sec = now - file_stat.st_mtime;
                constexpr int64_t kSkipAgeSec = 7200;  // 2 hours

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

        ::closedir(dir);

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
        std::remove(file_path.c_str());
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
            outfile.close();

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
                    lines.size() - relayed);
    }

    return relayed;
}

}  // namespace wge::kafka::wal
