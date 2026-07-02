/**
 * @file field_applier.cc
 * @brief FieldApplier 实现 — Protobuf 字段写入器
 *
 * ## 模块职责
 * FieldApplier 通过 protobuf 反射 API 将提取的字段值写入 HttpAccessEvent。
 *
 * ## 关键设计
 * - **Protobuf 反射**: 使用 google::protobuf::Reflection 和 FieldDescriptor
 *   在运行时按字段名称查找并设置 protobuf 字段，避免为每个字段写硬编码 setter
 * - **类型安全**: 根据 FieldType 枚举选择正确的 setter：
 *   String / Int32 / Int64 / Bytes / Bool
 * - **类型兼容**: 对整数类型使用 from_chars 解析字符串，对 uint 类型校验非负
 * - **Bytes 解码**: 支持 raw / base64 / hex 三种编码
 * - **时间戳解析**: 支持 Unix epoch（秒/毫秒/微秒）、ISO 8601、Nginx 格式
 */

#include "mapper/field_applier.h"

#include <cerrno>
#include <charconv>
#include <cstring>
#include <stdexcept>

#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "http_access.pb.h"
#include "spdlog/spdlog.h"

namespace wge::kafka::mapper {

// ============================================================================
// applyFields
// ============================================================================

void FieldApplier::applyFields(
    const std::map<std::string, std::string>& fields,
    HttpAccessEvent& event,
    const std::vector<FieldMapping>& mappings) const {
    if (!mappings.empty()) {
        // 有类型定义，按类型写入
        for (const auto& mapping : mappings) {
            auto it = fields.find(mapping.target);
            if (it == fields.end()) {
                if (mapping.required) {
                    spdlog::warn("Required field '{}' missing in extracted fields",
                                 mapping.target);
                }
                continue;
            }
            setField(event, mapping.target, it->second, mapping.type);
        }
    } else {
        // 无类型定义，全部按 String 处理
        for (const auto& [target, value] : fields) {
            setStringField(event, target, value);
        }
    }
}

// ============================================================================
// setField
// ============================================================================

void FieldApplier::setField(HttpAccessEvent& event,
                            const std::string& field_name,
                            const std::string& raw_value,
                            FieldType type) const {
    switch (type) {
        case FieldType::String:
            setStringField(event, field_name, raw_value);
            break;
        case FieldType::Int32:
            setInt32Field(event, field_name, raw_value);
            break;
        case FieldType::Int64:
            setInt64Field(event, field_name, raw_value);
            break;
        case FieldType::Bytes:
            setBytesField(event, field_name, raw_value, "raw");
            break;
        case FieldType::Bool:
            setBoolField(event, field_name, raw_value);
            break;
        case FieldType::Header:
            // Header 类型由 JsonMapper/RegexMapper 单独处理
            spdlog::warn("FieldApplier::setField: Header type should be handled "
                         "by mapper directly, field='{}'",
                         field_name);
            break;
    }
}

// ============================================================================
// 反射辅助函数
// ============================================================================

namespace {

/**
 * @brief 通过 protobuf 反射查找字段描述符
 *
 * 使用 google::protobuf::Message::GetDescriptor() 获取元数据，
 * 然后通过 FindFieldByName 查找字段。
 *
 * @param msg Protobuf 消息实例
 * @param field_name 字段名（如 "request_uri"）
 * @return 字段描述符指针，未找到返回 nullptr
 */
const google::protobuf::FieldDescriptor* findFieldDescriptor(
    const google::protobuf::Message& msg, const std::string& field_name) {
    const auto* descriptor = msg.GetDescriptor();
    if (!descriptor) return nullptr;
    return descriptor->FindFieldByName(field_name);
}

}  // namespace

// ============================================================================
// setStringField
// ============================================================================

void FieldApplier::setStringField(HttpAccessEvent& event,
                                  const std::string& field_name,
                                  const std::string& value) const {
    const auto* fd = findFieldDescriptor(event, field_name);
    if (!fd) {
        spdlog::warn("Field '{}' not found in HttpAccessEvent descriptor",
                     field_name);
        return;
    }

    // 检查是否为 repeated 字段 — repeated 字段需要用 AddString 而非 SetString
    if (fd->is_repeated()) {
        spdlog::warn("Field '{}' is repeated, use AddString instead of SetString. "
                     "Skipping scalar set.", field_name);
        return;
    }

    if (fd->type() != google::protobuf::FieldDescriptor::TYPE_STRING &&
        fd->type() != google::protobuf::FieldDescriptor::TYPE_BYTES) {
        spdlog::warn("Field '{}' is not a string type (actual: {})",
                     field_name, fd->type_name());
        return;
    }

    auto* reflection = event.GetReflection();
    reflection->SetString(&event, fd, value);
}

// ============================================================================
// setInt32Field
// ============================================================================

void FieldApplier::setInt32Field(HttpAccessEvent& event,
                                 const std::string& field_name,
                                 const std::string& value) const {
    const auto* fd = findFieldDescriptor(event, field_name);
    if (!fd) {
        spdlog::warn("Field '{}' not found in HttpAccessEvent descriptor",
                     field_name);
        return;
    }

    // 检查是否为 repeated 字段
    if (fd->is_repeated()) {
        spdlog::warn("Field '{}' is repeated, skipping scalar set.", field_name);
        return;
    }

    int32_t result = 0;
    auto [ptr, ec] =
        std::from_chars(value.data(), value.data() + value.size(), result);

    if (ec != std::errc{}) {
        spdlog::warn("Failed to parse int32 from '{}' for field '{}': {}",
                     value, field_name, std::make_error_code(ec).message());
        return;
    }

    auto* reflection = event.GetReflection();
    switch (fd->type()) {
        case google::protobuf::FieldDescriptor::TYPE_INT32:
        case google::protobuf::FieldDescriptor::TYPE_SINT32:
        case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
            reflection->SetInt32(&event, fd, result);
            break;
        case google::protobuf::FieldDescriptor::TYPE_UINT32:
            if (result < 0) {
                spdlog::warn("Field '{}' has negative value {} for uint32, skipping",
                             field_name, result);
                return;
            }
            reflection->SetUInt32(&event, fd,
                                  static_cast<uint32_t>(result));
            break;
        default:
            spdlog::warn("Field '{}' type mismatch: expected int32-compatible, got {}",
                         field_name, fd->type_name());
            break;
    }
}

// ============================================================================
// setInt64Field
// ============================================================================

void FieldApplier::setInt64Field(HttpAccessEvent& event,
                                 const std::string& field_name,
                                 const std::string& value) const {
    const auto* fd = findFieldDescriptor(event, field_name);
    if (!fd) {
        spdlog::warn("Field '{}' not found in HttpAccessEvent descriptor",
                     field_name);
        return;
    }

    // 检查是否为 repeated 字段
    if (fd->is_repeated()) {
        spdlog::warn("Field '{}' is repeated, skipping scalar set.", field_name);
        return;
    }

    int64_t result = 0;
    auto [ptr, ec] =
        std::from_chars(value.data(), value.data() + value.size(), result);

    if (ec != std::errc{}) {
        spdlog::warn("Failed to parse int64 from '{}' for field '{}': {}",
                     value, field_name, std::make_error_code(ec).message());
        return;
    }

    auto* reflection = event.GetReflection();
    switch (fd->type()) {
        case google::protobuf::FieldDescriptor::TYPE_INT64:
        case google::protobuf::FieldDescriptor::TYPE_SINT64:
        case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
            reflection->SetInt64(&event, fd, result);
            break;
        case google::protobuf::FieldDescriptor::TYPE_UINT64:
            if (result < 0) {
                spdlog::warn("Field '{}' has negative value {} for uint64, skipping",
                             field_name, result);
                return;
            }
            reflection->SetUInt64(&event, fd,
                                  static_cast<uint64_t>(result));
            break;
        default:
            spdlog::warn("Field '{}' type mismatch: expected int64-compatible, got {}",
                         field_name, fd->type_name());
            break;
    }
}

// ============================================================================
// setBytesField
// ============================================================================

void FieldApplier::setBytesField(HttpAccessEvent& event,
                                 const std::string& field_name,
                                 const std::string& value,
                                 const std::string& encoding) const {
    const auto* fd = findFieldDescriptor(event, field_name);
    if (!fd) {
        spdlog::warn("Field '{}' not found in HttpAccessEvent descriptor",
                     field_name);
        return;
    }

    if (fd->type() != google::protobuf::FieldDescriptor::TYPE_BYTES &&
        fd->type() != google::protobuf::FieldDescriptor::TYPE_STRING) {
        spdlog::warn("Field '{}' is not bytes/string type (actual: {})",
                     field_name, fd->type_name());
        return;
    }

    std::string decoded = decodeBytes(value, encoding);

    auto* reflection = event.GetReflection();
    reflection->SetString(&event, fd, decoded);
}

// ============================================================================
// setBoolField
// ============================================================================

void FieldApplier::setBoolField(HttpAccessEvent& event,
                                const std::string& field_name,
                                const std::string& value) const {
    const auto* fd = findFieldDescriptor(event, field_name);
    if (!fd) {
        spdlog::warn("Field '{}' not found in HttpAccessEvent descriptor",
                     field_name);
        return;
    }

    if (fd->type() != google::protobuf::FieldDescriptor::TYPE_BOOL) {
        spdlog::warn("Field '{}' is not bool type (actual: {})",
                     field_name, fd->type_name());
        return;
    }

    // 支持多种 bool 表示
    bool result = false;
    std::string lower;
    lower.reserve(value.size());
    for (char c : value) {
        lower.push_back(
            static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    }

    if (lower == "true" || lower == "1" || lower == "yes" || lower == "on") {
        result = true;
    } else if (lower == "false" || lower == "0" || lower == "no" ||
               lower == "off" || lower.empty()) {
        result = false;
    } else {
        // 尝试解析为整数
        int int_val = 0;
        auto [ptr, ec] = std::from_chars(
            value.data(), value.data() + value.size(), int_val);
        if (ec == std::errc{}) {
            result = (int_val != 0);
        } else {
            spdlog::warn("Cannot parse bool from '{}' for field '{}'",
                         value, field_name);
            return;
        }
    }

    auto* reflection = event.GetReflection();
    reflection->SetBool(&event, fd, result);
}

// ============================================================================
// parseTimestamp
// ============================================================================

int64_t FieldApplier::parseTimestamp(
    const std::string& raw,
    const std::vector<std::string>& formats) const {
    if (raw.empty()) return -1;

    // 策略: 优先数字检测（最快），然后按配置格式尝试，最后回退到常见格式

    // 检测是否全为数字（快速路径：直接 from_chars）
    bool all_digits = !raw.empty();
    for (char c : raw) {
        if (c < '0' || c > '9') {
            all_digits = false;
            break;
        }
    }

    if (all_digits) {
        if (raw.size() == 13) {
            // Unix epoch milliseconds (13 位: 如 1705313400000)
            int64_t ms;
            auto [ptr, ec] =
                std::from_chars(raw.data(), raw.data() + raw.size(), ms);
            if (ec == std::errc{}) return ms;
        }
        if (raw.size() == 10) {
            // Unix epoch seconds (10 位: 如 1705313400)
            int64_t sec;
            auto [ptr, ec] =
                std::from_chars(raw.data(), raw.data() + raw.size(), sec);
            if (ec == std::errc{}) {
                // 溢出保护: sec * 1000 可能溢出 int64_t
                // INT64_MAX / 1000 ≈ 9.2e15，对应年份约公元 292277026596
                // 正常 Unix 秒时间戳远小于此，但仍做防御性检查
                if (sec > INT64_MAX / 1000 || sec < INT64_MIN / 1000) {
                    return 0;  // 溢出，返回 0
                }
                return sec * 1000;  // 秒 → 毫秒
            }
        }
        if (raw.size() == 16) {
            // Unix epoch microseconds (16 位: 如 1705313400000000)
            int64_t us;
            auto [ptr, ec] =
                std::from_chars(raw.data(), raw.data() + raw.size(), us);
            if (ec == std::errc{}) return us / 1000;   // 微秒 → 毫秒
        }
    }

    // 如果有明确的格式字符串，使用 strptime 尝试（strptime 支持 %Y %m %d 等格式符）
    if (!formats.empty()) {
        for (const auto& fmt : formats) {
            std::tm tm = {};
            char* end = strptime(raw.c_str(), fmt.c_str(), &tm);
            if (end != nullptr && *end == '\0') {  // 完整匹配
                tm.tm_isdst = -1;  // 让系统判断是否为夏令时
                errno = 0;
                auto epoch = timegm(&tm);  // UTC 时间（非本地时间）
                if (errno == 0) {
                    // 溢出保护: epoch * 1000 可能溢出
                    int64_t ep = static_cast<int64_t>(epoch);
                    if (ep > INT64_MAX / 1000 || ep < INT64_MIN / 1000) {
                        return 0;
                    }
                    return ep * 1000;
                }
            }
        }
    }

    // 回退: ISO 8601 检测
    // 格式: 2024-01-15T10:30:00.123Z 或 2024-01-15T10:30:00+08:00
    if (raw.size() >= 19 && raw[4] == '-' && raw[7] == '-') {
        std::tm tm = {};
        int ms = 0;
        int tz_h = 0, tz_m = 0;
        char tz_sign = '+';
        char sep = raw[10];  // 'T' 或 ' '

        int parsed = 0;
        if (raw.size() >= 23 && raw[19] == '.') {
            // 带毫秒: 2024-01-15T10:30:00.123+08:00
            parsed = std::sscanf(
                raw.c_str(), "%4d-%2d-%2d%c%2d:%2d:%2d.%3d%c%2d:%2d",
                &tm.tm_year, &tm.tm_mon, &tm.tm_mday, &sep,
                &tm.tm_hour, &tm.tm_min, &tm.tm_sec, &ms,
                &tz_sign, &tz_h, &tz_m);
        } else {
            // 无毫秒: 2024-01-15T10:30:00Z
            char tz_str[8] = {};
            parsed = std::sscanf(
                raw.c_str(), "%4d-%2d-%2d%c%2d:%2d:%2d%7s",
                &tm.tm_year, &tm.tm_mon, &tm.tm_mday, &sep,
                &tm.tm_hour, &tm.tm_min, &tm.tm_sec, tz_str);
            if (parsed == 7 && tz_str[0] != '\0') {
                if (tz_str[0] == 'Z' || tz_str[0] == 'z') {
                    parsed = 7;  // UTC 时区，无需额外调整
                } else {
                    std::sscanf(tz_str, "%c%2d:%2d", &tz_sign, &tz_h, &tz_m);
                    parsed = 10;
                }
            }
        }

        if (parsed >= 6) {
            tm.tm_year -= 1900;  // tm_year 是 1900 年起的偏移
            tm.tm_mon -= 1;       // tm_mon 是 0-11
            tm.tm_isdst = -1;

            errno = 0;
            auto epoch = timegm(&tm);  // UTC 秒
            if (errno == 0) {
                // 溢出保护
                int64_t ep = static_cast<int64_t>(epoch);
                if (ep > INT64_MAX / 1000 || ep < INT64_MIN / 1000) return 0;
                int64_t result = ep * 1000 + ms;

                // 减去时区偏移（因为 timegm 假设 UTC 输入）
                if (parsed >= 9) {
                    int tz_offset = tz_h * 3600 + tz_m * 60;
                    if (tz_sign == '-') tz_offset = -tz_offset;
                    result -= static_cast<int64_t>(tz_offset) * 1000;
                }
                return result;
            }
        }
    }

    // Nginx 格式: 02/Jan/2006:15:04:05 +0700
    if (raw.size() >= 26 && raw[2] == '/' && raw[6] == '/') {
        std::tm tm = {};
        char month_str[4] = {};
        int tz_h = 0, tz_m = 0;
        char tz_sign_c = '+';

        int parsed =
            std::sscanf(raw.c_str(), "%2d/%3[^/]/%4d:%2d:%2d:%2d %c%2d%2d",
                        &tm.tm_mday, month_str, &tm.tm_year,
                        &tm.tm_hour, &tm.tm_min, &tm.tm_sec,
                        &tz_sign_c, &tz_h, &tz_m);
        if (parsed == 9) {
            // 月份缩写 → 数字
            static const char* months[] = {"Jan", "Feb", "Mar", "Apr",
                                           "May", "Jun", "Jul", "Aug",
                                           "Sep", "Oct", "Nov", "Dec"};
            int mon = -1;
            for (int i = 0; i < 12; ++i) {
                if (std::strncmp(month_str, months[i], 3) == 0) {
                    mon = i;
                    break;
                }
            }
            if (mon >= 0) {
                tm.tm_mon = mon;
                tm.tm_year -= 1900;
                tm.tm_isdst = -1;
                errno = 0;
                auto epoch = timegm(&tm);
                if (errno == 0) {
                    // 溢出保护
                    int64_t ep = static_cast<int64_t>(epoch);
                    if (ep > INT64_MAX / 1000 || ep < INT64_MIN / 1000) return 0;
                    int64_t result = ep * 1000;
                    int tz_offset = tz_h * 3600 + tz_m * 60;
                    if (tz_sign_c == '-') tz_offset = -tz_offset;
                    result -= static_cast<int64_t>(tz_offset) * 1000;
                    return result;
                }
            }
        }
    }

    spdlog::warn("FieldApplier::parseTimestamp: unparseable: '{}'", raw);
    return -1;  // 解析失败
}

// ============================================================================
// decodeBytes
// ============================================================================

std::string FieldApplier::decodeBytes(const std::string& raw,
                                      const std::string& encoding) const {
    if (encoding == "raw" || encoding.empty()) {
        return raw;
    }

    if (encoding == "base64") {
        return base64Decode(raw);
    }

    if (encoding == "hex") {
        if (raw.size() % 2 != 0) {
            spdlog::warn("Hex string has odd length: {}", raw.size());
            return raw;
        }
        std::string result;
        result.reserve(raw.size() / 2);
        for (size_t i = 0; i + 1 < raw.size(); i += 2) {
            int high = hexCharToNibble(raw[i]);
            int low = hexCharToNibble(raw[i + 1]);
            if (high < 0 || low < 0) {
                spdlog::warn("Invalid hex chars at position {}: '{}'",
                             i, raw.substr(i, 2));
                return raw;
            }
            result.push_back(static_cast<char>((high << 4) | low));
        }
        return result;
    }

    spdlog::warn("Unknown bytes encoding: '{}', returning raw", encoding);
    return raw;
}

// ============================================================================
// applyConstant
// ============================================================================

void FieldApplier::applyConstant(HttpAccessEvent& event,
                                 const std::string& key,
                                 const std::string& value) const {
    setStringField(event, key, value);
}

// ============================================================================
// 辅助函数实现
// ============================================================================

int FieldApplier::hexCharToNibble(char c) noexcept {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

std::string FieldApplier::base64Decode(std::string_view encoded) {
    static const char kBase64Table[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    // 实际使用 std::find 方式
    auto decode_char = [](unsigned char c) -> int {
        if (c >= 'A' && c <= 'Z') return c - 'A';
        if (c >= 'a' && c <= 'z') return c - 'a' + 26;
        if (c >= '0' && c <= '9') return c - '0' + 52;
        if (c == '+') return 62;
        if (c == '/') return 63;
        return -1;
    };

    std::string result;
    result.reserve((encoded.size() * 3) / 4);

    int val = 0;
    int valb = -8;
    for (unsigned char c : encoded) {
        if (c == '=' || c == '\n' || c == '\r' || c == ' ') continue;

        int decoded = decode_char(c);
        if (decoded < 0) {
            spdlog::warn("Invalid base64 character: '{}' (0x{:02x})",
                         static_cast<char>(c), static_cast<int>(c));
            continue;
        }

        val = (val << 6) | decoded;
        valb += 6;
        if (valb >= 0) {
            result.push_back(static_cast<char>((val >> valb) & 0xFF));
            valb -= 8;
        }
    }

    // 注意: 不再 flush 残留位。
    // 标准 Base64 要求输入为 4 的倍数（不足时用 '=' 填充）。
    // 残留位（valb > -8）意味着输入不完整，此时输出残留字节会产生垃圾数据。
    // 安全做法: 丢弃残留位，只输出完整的 8 位字节。

    return result;
}

}  // namespace wge::kafka::mapper
