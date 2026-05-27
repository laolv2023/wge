/**
 * @file wal_writer.cc
 * @brief WalWriter 实现
 */

#include "wal/wal_writer.h"

#include <cstdio>
#include <cstring>
#include <ctime>
#include <stdexcept>
#include <sys/stat.h>

#include "spdlog/spdlog.h"
#include "wge_alert.pb.h"

// protobuf JSON util (protobuf >= 3.0)
#if __has_include("google/protobuf/util/json_util.h")
#include "google/protobuf/util/json_util.h"
#define WGE_HAS_PROTOBUF_JSON_UTIL 1
#else
#define WGE_HAS_PROTOBUF_JSON_UTIL 0
#endif

namespace wge::kafka::wal {

// ============================================================================
// 构造与析构
// ============================================================================

WalWriter::WalWriter(const std::string& wal_dir)
    : wal_dir_(wal_dir) {

    if (wal_dir_.empty()) {
        throw std::runtime_error("WalWriter: wal_dir is empty");
    }

    // 创建目录 (递归)
    // 使用 mkdir -p 等效逻辑
    std::string current_path;
    for (size_t i = 0; i < wal_dir_.size(); ++i) {
        current_path += wal_dir_[i];
        if (wal_dir_[i] == '/' || i == wal_dir_.size() - 1) {
            // 去除末尾多余斜杠
            while (!current_path.empty() && current_path.back() == '/') {
                current_path.pop_back();
            }
            if (!current_path.empty()) {
                int rc = mkdir(current_path.c_str(), 0755);
                if (rc != 0 && errno != EEXIST) {
                    throw std::runtime_error(
                        "WalWriter: failed to create directory '" +
                        current_path + "': " + std::strerror(errno));
                }
            }
        }
    }

    SPDLOG_INFO("WalWriter created: wal_dir={}", wal_dir_);
}

WalWriter::~WalWriter() {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        if (current_file_) {
            std::fclose(current_file_);
            current_file_ = nullptr;
            SPDLOG_INFO("WalWriter: closed WAL file '{}'", current_file_path_);
        }
    } catch (const std::exception& e) {
        SPDLOG_ERROR("WalWriter destructor error: {}", e.what());
    } catch (...) {
        SPDLOG_ERROR("WalWriter destructor: unknown error");
    }
}

// ============================================================================
// write
// ============================================================================

void WalWriter::write(const std::shared_ptr<WgeAlertEvent>& alert) {
    if (!alert) {
        SPDLOG_WARN("WalWriter::write: null alert ignored");
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    // 获取当前时间
    auto now = std::time(nullptr);
    std::tm tm_now;
    ::localtime_r(&now, &tm_now);

    // 按需轮转
    rotateIfNeeded(tm_now);

    if (!current_file_) {
        throw std::runtime_error("WalWriter::write: WAL file not open");
    }

    // 序列化为 JSON 行
    std::string json_line = serializeToJsonLine(*alert);

    // 追加写入
    std::fwrite(json_line.data(), 1, json_line.size(), current_file_);

    // 追加换行符
    std::fputc('\n', current_file_);

    // 检查写入错误
    if (std::ferror(current_file_)) {
        throw std::runtime_error(
            "WalWriter::write: fwrite failed: " +
            std::string(std::strerror(errno)));
    }

    SPDLOG_DEBUG("WalWriter: wrote alert_id={} to {} ({} bytes)",
                 alert->alert_id(), current_file_path_, json_line.size());
}

// ============================================================================
// flush
// ============================================================================

void WalWriter::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (current_file_) {
        std::fflush(current_file_);
    }
}

// ============================================================================
// currentWalFilePath
// ============================================================================

std::string WalWriter::currentWalFilePath(const std::tm* t) const {
    char buf[64];
    std::tm local_tm;

    if (t) {
        local_tm = *t;
    } else {
        auto now = std::time(nullptr);
        ::localtime_r(&now, &local_tm);
    }

    std::strftime(buf, sizeof(buf), "alert-%Y%m%d-%H.log", &local_tm);

    std::string path = wal_dir_;
    if (!path.empty() && path.back() != '/') {
        path += '/';
    }
    path += buf;
    return path;
}

// ============================================================================
// rotateIfNeeded
// ============================================================================

void WalWriter::rotateIfNeeded(const std::tm& now) {
    std::string expected_path = currentWalFilePath(&now);

    // 相同文件，无需轮转
    if (expected_path == current_file_path_ && current_file_) {
        return;
    }

    // 关闭旧文件
    if (current_file_) {
        std::fclose(current_file_);
        current_file_ = nullptr;
        SPDLOG_INFO("WalWriter: rotated WAL file '{}' → '{}'",
                    current_file_path_, expected_path);
    }

    // 打开新文件 (append 模式)
    current_file_path_ = expected_path;
    current_file_ = std::fopen(current_file_path_.c_str(), "ab");
    if (!current_file_) {
        throw std::runtime_error(
            "WalWriter: failed to open WAL file '" +
            current_file_path_ + "': " + std::strerror(errno));
    }

    // 禁用 stdio 缓冲以便崩溃时数据安全
    std::setvbuf(current_file_, nullptr, _IONBF, 0);

    SPDLOG_INFO("WalWriter: opened WAL file '{}'", current_file_path_);
}

// ============================================================================
// serializeToJsonLine
// ============================================================================

std::string WalWriter::serializeToJsonLine(const WgeAlertEvent& alert) {
#if WGE_HAS_PROTOBUF_JSON_UTIL
    google::protobuf::util::JsonPrintOptions options;
    options.add_whitespace = false;
    options.preserve_proto_field_names = true;

    std::string json;
    auto status =
        google::protobuf::util::MessageToJsonString(alert, &json, options);
    if (status.ok()) {
        return json;
    }
    // 回退到 Utf8DebugString
#endif
    std::string text = alert.Utf8DebugString();
    std::string compressed;
    compressed.reserve(text.size());
    for (char c : text) {
        if (c == '\n' || c == '\r') {
            compressed += ' ';
        } else {
            compressed += c;
        }
    }
    return compressed;
}

}  // namespace wge::kafka::wal
