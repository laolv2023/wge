/// @file akto_preprocessor.cc
/// @brief Akto API 日志预处理器实现
///
/// 处理 Akto 特有的三处格式问题, 将原始日志转换为标准 JsonMapper 可识别的格式。

#include "akto_preprocessor.h"

#include <sstream>
#include <stdexcept>

// simdjson: 高性能 JSON 解析库
#include <simdjson.h>

namespace wge::kafka::adapter {

// =============================================================================
// 公开接口
// =============================================================================

std::expected<std::string, std::string>
AktoPreprocessor::preprocess(std::string_view raw_json) {
    using namespace simdjson;

    // ── 步骤 1: 解析原始 JSON ──
    ondemand::parser parser;
    auto doc_result = parser.iterate(raw_json);
    if (doc_result.error()) {
        return std::unexpected(
            "AktoPreprocessor: failed to parse raw JSON: " +
            std::string(error_message(doc_result.error())));
    }
    ondemand::document doc = doc_result.value_unsafe();

    // ── 步骤 2: 获取 JSON 对象 ──
    ondemand::object obj;
    auto obj_err = doc.get_object().get(obj);
    if (obj_err) {
        return std::unexpected(
            "AktoPreprocessor: root is not a JSON object: " +
            std::string(error_message(obj_err)));
    }

    // ── 步骤 3: 构建输出 JSON ──
    std::ostringstream out;
    out << "{";
    bool first = true;

    // 迭代所有顶层字段
    for (auto field : obj) {
        std::string_view key = field.unescaped_key().value();

        if (!first) out << ",";
        first = false;
        out << "\"" << key << "\":";

        // ── 特殊处理 1: requestHeaders — JSON 字符串 → 结构化数组 ──
        if (key == "requestHeaders" || key == "responseHeaders") {
            // 读取 JSON 字符串值
            std::string_view header_str;
            auto str_err = field.value().get_string().get(header_str);
            if (!str_err) {
                // 展开 JSON 字符串 headers → [{"key":"...","value":"..."}]
                auto expanded = expandHeaderJsonString(header_str);
                if (expanded.has_value()) {
                    out << expanded.value();
                    continue;
                }
                // 展开失败: 输出空数组, 继续处理
                out << "[]";
            } else {
                // 不是字符串 (可能已经是对象或 null), 回退处理
                // 尝试按对象处理, 否则输出原值
                auto value_doc = field.value().get_value();
                (void)value_doc;  // 保持编译器安静
                out << "[]";
            }
            continue;
        }

        // ── 特殊处理 2: time — 秒 → 毫秒 ──
        if (key == "time") {
            int64_t seconds = 0;
            auto int_err = field.value().get_int64().get(seconds);
            if (!int_err) {
                out << (seconds * 1000);
            } else {
                // 尝试按字符串读取
                std::string_view time_str;
                auto str_err = field.value().get_string().get(time_str);
                if (!str_err) {
                    out << fixTimestampSecondsToMs(time_str);
                } else {
                    out << "0";  // 无法解析, 设 0
                }
            }
            continue;
        }

        // ── 特殊处理 3: type — "HTTP/1.1" → "1.1" ──
        if (key == "type") {
            std::string_view type_str;
            auto str_err = field.value().get_string().get(type_str);
            if (!str_err) {
                out << "\"" << fixHttpVersion(type_str) << "\"";
            } else {
                out << "\"1.1\"";
            }
            continue;
        }

        // ── 普通字段: 按类型序列化 ──
        auto value_type = field.value().type();
        switch (value_type) {
        case ondemand::json_type::string: {
            std::string_view sv;
            field.value().get_string().get(sv);
            out << "\"" << sv << "\"";
            break;
        }
        case ondemand::json_type::number: {
            // simdjson ondemand 不区分 int/float
            // 尽力按 int64 读取, 失败则按 double
            int64_t int_val = 0;
            auto int_err = field.value().get_int64().get(int_val);
            if (!int_err) {
                out << int_val;
            } else {
                double dbl_val = 0;
                auto dbl_err = field.value().get_double().get(dbl_val);
                if (!dbl_err) {
                    out << dbl_val;
                } else {
                    out << "0";
                }
            }
            break;
        }
        case ondemand::json_type::boolean: {
            bool b = false;
            field.value().get_bool().get(b);
            out << (b ? "true" : "false");
            break;
        }
        case ondemand::json_type::null:
            out << "null";
            break;
        case ondemand::json_type::object:
            // 嵌入式 JSON 对象: 直接序列化
            out << simdjson::to_json_string(field.value().get_value());
            break;
        case ondemand::json_type::array:
            // 数组: 直接序列化
            out << simdjson::to_json_string(field.value().get_value());
            break;
        }
    }

    out << "}";
    return out.str();
}

// =============================================================================
// 私有方法
// =============================================================================

std::expected<std::string, std::string>
AktoPreprocessor::expandHeaderJsonString(std::string_view header_json_str) {
    using namespace simdjson;

    // 解析 JSON 字符串为 key-value 对象
    // 输入: "{\"Accept\":\"application/json\",\"Host\":\"example.com\"}"
    // 输出: [{"key":"Accept","value":"application/json"},{"key":"Host","value":"example.com"}]

    ondemand::parser parser;
    auto doc_result = parser.iterate(header_json_str);
    if (doc_result.error()) {
        return std::unexpected(
            "expandHeaderJsonString: failed to parse header JSON: " +
            std::string(error_message(doc_result.error())));
    }
    ondemand::document doc = doc_result.value_unsafe();

    ondemand::object header_obj;
    auto obj_err = doc.get_object().get(header_obj);
    if (obj_err) {
        return std::unexpected(
            "expandHeaderJsonString: header value is not a JSON object");
    }

    std::ostringstream out;
    out << "[";
    bool first = true;
    for (auto field : header_obj) {
        std::string_view key = field.unescaped_key().value();
        std::string_view value;

        auto value_err = field.value().get_string().get(value);
        if (value_err) {
            // 非字符串值 (如数字), 跳过
            continue;
        }

        if (!first) out << ",";
        first = false;

        // 构建 {"key":"...","value":"..."} 结构
        out << "{\"key\":\"";
        // ── JSON 转义: key ──
        for (char c : key) {
            switch (c) {
            case '"':  out << "\\\""; break;
            case '\\': out << "\\\\"; break;
            default:   out << c;      break;
            }
        }
        out << "\",\"value\":\"";
        // ── JSON 转义: value ──
        for (char c : value) {
            switch (c) {
            case '"':  out << "\\\""; break;
            case '\\': out << "\\\\"; break;
            default:   out << c;      break;
            }
        }
        out << "\"}";
    }
    out << "]";
    return out.str();
}

std::string
AktoPreprocessor::fixTimestampSecondsToMs(std::string_view epoch_seconds) {
    // 秒级时间戳 → 毫秒级
    // 输入: "1779867214" → 输出: "1779867214000"
    try {
        int64_t sec = std::stoll(std::string(epoch_seconds));
        return std::to_string(sec * 1000);
    } catch (...) {
        return "0";
    }
}

std::string
AktoPreprocessor::fixHttpVersion(std::string_view http_version) {
    // "HTTP/1.1" → "1.1"
    // "HTTP/2.0" → "2.0"
    // "HTTP/1.1 (TLS)" → "1.1"
    constexpr std::string_view prefix = "HTTP/";
    if (http_version.size() > prefix.size() &&
        http_version.substr(0, prefix.size()) == prefix) {
        // 去除 "HTTP/" 前缀, 可能还有 "(TLS)" 后缀等
        std::string_view after = http_version.substr(prefix.size());
        // 取第一个空格前的部分
        auto space_pos = after.find(' ');
        if (space_pos != std::string_view::npos) {
            return std::string(after.substr(0, space_pos));
        }
        return std::string(after);
    }
    // 没有 "HTTP/" 前缀, 原样返回
    return std::string(http_version);
}

} // namespace wge::kafka::adapter
