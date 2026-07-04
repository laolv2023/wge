/**
 * @file wal_writer.cc
 * @brief WalWriter 实现 — Write-Ahead Log 写入器
 *
 * ## 模块职责
 * WalWriter 实现 WAL（预写日志）机制，在发送告警到 Kafka 之前先将告警
 * 持久化到本地磁盘。当 Kafka 不可用或发送失败时，WalRelay 可从中继文件
 * 中恢复并补发告警。
 *
 * ## 关键设计
 * - **按小时轮转**: 文件名格式为 alert-YYYYMMDD-HH.log，按小时自动切分新文件
 * - **强制刷盘**: 每条写入后调用 fflush + fsync，确保崩溃时不丢失数据
 * - **无缓冲 I/O**: setvbuf(IONBF) 禁用 stdio 缓冲，进一步降低数据丢失风险
 * - **JSON Lines 格式**: 每行一个 JSON 序列化的 WgeAlertEvent，便于逐行恢复
 * - **多编码兼容**: 若 protobuf JSON util 不可用，回退为 base64(protobuf binary)
 *
 * ## 线程安全
 * 所有公有方法使用 mutex_ 保护，单写者模型。
 */

#include "wal/wal_writer.h"

#include <cstdio>
#include <cstring>
#include <ctime>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

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

    // 获取当前时间，用于按小时轮转 WAL 文件
    auto now = std::time(nullptr);
    std::tm tm_now;
    // 使用 UTC 时间 (gmtime_r) 而非本地时间 (localtime_r)，
    // 确保不同时区的服务器产生一致的 WAL 文件名
    ::gmtime_r(&now, &tm_now);

    // 检查是否需要轮转到新的小时文件
    rotateIfNeeded(tm_now);

    if (!current_file_) {
        throw std::runtime_error("WalWriter::write: WAL file not open");
    }

    // 序列化为 JSON 行（单行 JSON，便于逐行恢复）
    std::string json_line = serializeToJsonLine(*alert);

    // 追加写入 — 带短写重试
    size_t written = std::fwrite(json_line.data(), 1, json_line.size(), current_file_);
    if (written != json_line.size()) {
        SPDLOG_ERROR("WalWriter::write: short write: {} of {} bytes written (ferror={})",
                     written, json_line.size(), std::ferror(current_file_));
        if (std::ferror(current_file_)) {
            throw std::runtime_error(
                "WalWriter::write: fwrite failed: " +
                std::string(std::strerror(errno)));
        }
        // 非错误原因的短写（如磁盘空间不足但未报错），重试一次
        size_t remaining = json_line.size() - written;
        size_t written2 = std::fwrite(json_line.data() + written, 1, remaining, current_file_);
        if (written2 != remaining) {
            throw std::runtime_error(
                "WalWriter::write: fwrite retry failed: wrote " +
                std::to_string(written + written2) + " of " +
                std::to_string(json_line.size()) + " bytes");
        }
    }

    // 追加换行符（JSON Lines 格式: 每行一个 JSON 对象）
    if (std::fputc('\n', current_file_) == EOF) {
        SPDLOG_ERROR("WalWriter::write: fputc('\\n') failed (disk full?)");
        throw std::runtime_error(
            "WalWriter::write: fputc newline failed: " +
            std::string(std::strerror(errno)));
    }

    // 强制刷盘: fflush + fsync 确保数据写入物理介质
    // 这是 WAL 的核心保障 — 崩溃后数据不丢失
    if (std::fflush(current_file_) != 0) {
        SPDLOG_ERROR("WalWriter::write: fflush failed: {}", std::strerror(errno));
        throw std::runtime_error(
            "WalWriter::write: fflush failed: " +
            std::string(std::strerror(errno)));
    }
    int fd = ::fileno(current_file_);
    if (fd < 0) {
        SPDLOG_ERROR("WalWriter::write: fileno() returned {} (errno={}), skipping fsync",
                     fd, errno);
    } else {
        if (::fsync(fd) != 0) {
            // fsync 失败不抛异常（可能只是警告），但记录错误
            SPDLOG_ERROR("WalWriter::write: fsync failed (fd={}): {}",
                         fd, std::strerror(errno));
        }
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
        // 使用 UTC 时间 (gmtime_r) 而非本地时间 (localtime_r)，
        // 确保不同时区的服务器产生一致的 WAL 文件名
        ::gmtime_r(&now, &local_tm);
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

    // 相同文件，无需轮转（同一个小时间隔内）
    if (expected_path == current_file_path_ && current_file_) {
        return;
    }

    // 关闭旧文件（跨小时边界时触发）
    if (current_file_) {
        std::fclose(current_file_);
        current_file_ = nullptr;
        SPDLOG_INFO("WalWriter: rotated WAL file '{}' → '{}'",
                    current_file_path_, expected_path);
    }

    // 打开新文件 (append 模式，保留已有内容)
    current_file_path_ = expected_path;
    current_file_ = std::fopen(current_file_path_.c_str(), "ab");  // "ab" = 二进制追加
    if (!current_file_) {
        throw std::runtime_error(
            "WalWriter: failed to open WAL file '" +
            current_file_path_ + "': " + std::strerror(errno));
    }

    // 禁用 stdio 缓冲（_IONBF = 无缓冲），确保每字节立即写入内核
    // 配合 fflush + fsync 实现崩溃安全
    std::setvbuf(current_file_, nullptr, _IONBF, 0);

    SPDLOG_INFO("WalWriter: opened WAL file '{}'", current_file_path_);
}

// ============================================================================
// 简易 base64 编码 (用于 protobuf 二进制序列化的文本化)
// ============================================================================

namespace {

const char kBase64Table[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string base64Encode(const std::string& input) {
    std::string output;
    output.reserve(((input.size() + 2) / 3) * 4);

    unsigned char buf3[3];
    for (auto it = input.begin(); it != input.end(); ) {
        int len = 0;
        for (; len < 3 && it != input.end(); ++len, ++it) {
            buf3[len] = static_cast<unsigned char>(*it);
        }
        if (len == 0) break;

        output += kBase64Table[buf3[0] >> 2];
        if (len == 1) {
            output += kBase64Table[(buf3[0] & 0x03) << 4];
            output += '=';
            output += '=';
        } else {
            output += kBase64Table[((buf3[0] & 0x03) << 4) | (buf3[1] >> 4)];
            if (len == 2) {
                output += kBase64Table[(buf3[1] & 0x0f) << 2];
                output += '=';
            } else {
                output += kBase64Table[((buf3[1] & 0x0f) << 2) | (buf3[2] >> 6)];
                output += kBase64Table[buf3[2] & 0x3f];
            }
        }
    }
    return output;
}

}  // namespace

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
#endif
    // 无 JSON util 时使用二进制序列化 + base64，确保与 Relay 端兼容
    std::string binary;
    alert.SerializeToString(&binary);
    return base64Encode(binary);
}

}  // namespace wge::kafka::wal
