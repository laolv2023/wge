/**
 * @file regex_mapper.cc
 * @brief RegexMapper 实现
 */

#include "mapper/regex_mapper.h"

#include <charconv>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <sstream>

#include "re2/re2.h"
#include "spdlog/spdlog.h"

namespace wge::kafka::mapper {

// ============================================================================
// 静态缓存
// ============================================================================

std::mutex RegexMapper::cache_mutex_;
std::map<std::string, std::shared_ptr<re2::RE2>> RegexMapper::compile_cache_;

// ============================================================================
// 内置 Grok 模式定义
// ============================================================================

const std::map<std::string, std::string>& RegexMapper::builtinGrokPatterns() {
    static const std::map<std::string, std::string> patterns = {
        // IP 地址
        {"IP",
         R"((?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))"},

        // 主机名
        {"HOSTNAME", R"((?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,})"},

        // IP 或主机名
        {"IPORHOST", R"((?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)|(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,})"},

        // 正整数
        {"INT", R"((?:[+-]?\d+))"},

        // 数字
        {"NUMBER", R"((?:\d+(?:\.\d+)?))"},

        // 单词
        {"WORD", R"((?:\w+))"},

        // 非空白字符
        {"NOTSPACE", R"((?:\S+))"},

        // 任意数据
        {"DATA", R"((?:.*?))"},

        // 贪婪数据
        {"GREEDYDATA", R"((?:.*))"},

        // 引号内字符串
        {"QUOTEDSTRING", R"((?:"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'))"},

        // UUID
        {"UUID",
         R"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"},

        // Email
        {"EMAILADDRESS", R"((?:\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b))"},

        // URI
        {"URI", R"((?:/[^\s?]*)(?:\?[^\s]*)?)"},

        // HTTP method
        {"HTTPMETHOD", R"((?:GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS|CONNECT|TRACE))"},

        // HTTP version
        {"HTTPVERSION", R"(HTTP/\d(?:\.\d)?)"},

        // HTTP 状态码
        {"HTTPSTATUS", R"((?:\d{3}))"},

        // 时间戳: ISO 8601
        {"TIMESTAMP_ISO8601",
         R"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)"},

        // Nginx 风格时间戳
        {"NGINX_TIMESTAMP",
         R"(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4})"},

        // 常用日志级别
        {"LOGLEVEL", R"((?:TRACE|DEBUG|INFO|WARN|ERROR|FATAL|CRITICAL))"},
    };
    return patterns;
}

// ============================================================================
// 构造函数与析构
// ============================================================================

RegexMapper::~RegexMapper() = default;

// ============================================================================
// compile
// ============================================================================

std::expected<void, std::string> RegexMapper::compile(
    const std::string& pattern) {
    auto result = getOrCompile(pattern);
    if (!result) return std::unexpected(result.error());

    std::lock_guard<std::mutex> lock(cache_mutex_);
    compiled_regex_ = std::make_unique<re2::RE2>(pattern);
    if (!compiled_regex_->ok()) {
        return std::unexpected(
            std::string("RE2 compilation failed: ") + compiled_regex_->error());
    }
    return {};
}

// ============================================================================
// getOrCompile
// ============================================================================

std::expected<std::shared_ptr<re2::RE2>, std::string>
RegexMapper::getOrCompile(const std::string& pattern) const {
    // 先尝试读缓存（读锁）
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        auto it = compile_cache_.find(pattern);
        if (it != compile_cache_.end()) {
            return it->second;
        }
    }

    // 编译
    auto re = std::make_shared<re2::RE2>(pattern);
    if (!re->ok()) {
        return std::unexpected(
            std::string("RE2 compilation error: ") + re->error() +
            " (pattern: " + pattern + ")");
    }

    // 写入缓存
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        // Double-check: 可能已有其他线程写入
        auto it = compile_cache_.find(pattern);
        if (it != compile_cache_.end()) {
            return it->second;
        }
        compile_cache_[pattern] = re;
    }

    return re;
}

// ============================================================================
// extract
// ============================================================================

std::expected<std::map<std::string, std::string>, std::string>
RegexMapper::extract(std::string_view raw_payload, const std::string& pattern,
                     const std::vector<FieldMapping>& field_mappings) const {
    if (raw_payload.empty()) {
        return std::unexpected(std::string("Empty payload for regex extraction"));
    }
    if (pattern.empty()) {
        return std::unexpected(std::string("Empty regex pattern"));
    }

    auto re_result = getOrCompile(pattern);
    if (!re_result) return std::unexpected(re_result.error());

    return fullMatchAndExtract(raw_payload, **re_result, field_mappings);
}

// ============================================================================
// extractGrok
// ============================================================================

std::expected<std::map<std::string, std::string>, std::string>
RegexMapper::extractGrok(
    std::string_view raw_payload, const std::string& grok_pattern,
    const std::map<std::string, std::string>& custom_patterns,
    const std::vector<FieldMapping>& field_mappings) const {
    if (raw_payload.empty()) {
        return std::unexpected(std::string("Empty payload for Grok extraction"));
    }
    if (grok_pattern.empty()) {
        return std::unexpected(std::string("Empty Grok pattern"));
    }

    // 1. Grok → Regex
    auto regex_result = grokToRegex(grok_pattern, custom_patterns);
    if (!regex_result) return std::unexpected(regex_result.error());

    spdlog::trace("Grok '{}' → Regex '{}'", grok_pattern, *regex_result);

    // 2. 编译并匹配
    auto re_result = getOrCompile(*regex_result);
    if (!re_result) return std::unexpected(re_result.error());

    return fullMatchAndExtract(raw_payload, **re_result, field_mappings);
}

// ============================================================================
// fullMatchAndExtract
// ============================================================================

std::expected<std::map<std::string, std::string>, std::string>
RegexMapper::fullMatchAndExtract(
    std::string_view raw_payload, const re2::RE2& regex,
    const std::vector<FieldMapping>& field_mappings) const {
    // 获取命名捕获组
    const auto& named_groups = regex.NamedCapturingGroups();
    if (named_groups.empty()) {
        return std::unexpected(
            std::string("Regex has no named capturing groups"));
    }

    // 分配匹配数组
    int num_groups = regex.NumberOfCapturingGroups();
    if (num_groups <= 0) {
        return std::unexpected(
            std::string("Regex has no capturing groups"));
    }

    std::vector<re2::StringPiece> matches(
        static_cast<size_t>(num_groups + 1));  // +1 for full match

    std::string payload_copy(raw_payload);
    re2::StringPiece input(payload_copy);

    if (!regex.Match(input, 0, payload_copy.size(), re2::RE2::UNANCHORED,
                     matches.data(), static_cast<int>(matches.size()))) {
        return std::unexpected(
            std::string("Regex did not match payload (") +
            std::to_string(raw_payload.size()) + " bytes)");
    }

    // 构建捕获组名 → 值
    std::map<std::string, std::string> captured;
    for (const auto& [name, idx] : named_groups) {
        if (idx >= 0 && static_cast<size_t>(idx) < matches.size()) {
            const auto& piece = matches[static_cast<size_t>(idx)];
            if (piece.data() != nullptr) {
                captured[name] = std::string(piece.data(), piece.size());
            }
        }
    }

    // 根据 field_mappings 提取
    std::map<std::string, std::string> result;
    for (const auto& mapping : field_mappings) {
        if (mapping.source.empty()) continue;

        auto it = captured.find(mapping.source);
        if (it != captured.end()) {
            result[mapping.target] = it->second;
            spdlog::trace("Regex: '{}' → '{}' = '{}'", mapping.source,
                          mapping.target, it->second);
        } else {
            if (mapping.required) {
                return std::unexpected(
                    std::string("Required capture group '") +
                    mapping.source + "' (→ " + mapping.target +
                    ") not found in regex match");
            }
            if (mapping.default_value.has_value()) {
                result[mapping.target] = *mapping.default_value;
                spdlog::trace("Regex: '{}' → '{}': using default '{}'",
                              mapping.source, mapping.target,
                              *mapping.default_value);
            } else {
                spdlog::trace("Regex: '{}' → '{}': group not captured, skipping",
                              mapping.source, mapping.target);
            }
        }
    }

    return result;
}

// ============================================================================
// grokToRegex
// ============================================================================

std::expected<std::string, std::string> RegexMapper::grokToRegex(
    const std::string& grok_pattern,
    const std::map<std::string, std::string>& custom_patterns) {
    // 合并内置和自定义模式
    std::map<std::string, std::string> all_patterns = builtinGrokPatterns();
    for (const auto& [name, regex] : custom_patterns) {
        all_patterns[name] = regex;
    }

    std::string result = grok_pattern;

    // Grok 语法: %{PATTERN_NAME:capture_name} 或 %{PATTERN_NAME}
    // 使用简单的手动解析避免引入复杂依赖

    // 查找 %{ 的位置
    size_t pos = 0;
    int max_iterations = 200;  // 防止无限循环
    while (max_iterations-- > 0) {
        size_t start = result.find("%{", pos);
        if (start == std::string::npos) break;

        size_t end = result.find('}', start);
        if (end == std::string::npos) {
            return std::unexpected(
                std::string("Unclosed Grok pattern at position ") +
                std::to_string(start));
        }

        // 提取 %{...} 内部内容
        std::string inner = result.substr(start + 2, end - start - 2);

        std::string pattern_name;
        std::string capture_name;

        // 查找冒号分隔的模式名和捕获名
        size_t colon = inner.find(':');
        if (colon != std::string::npos) {
            pattern_name = inner.substr(0, colon);
            capture_name = inner.substr(colon + 1);
        } else {
            pattern_name = inner;
        }

        // 去除首尾空格
        auto trim = [](std::string& s) {
            s.erase(0, s.find_first_not_of(" \t\n\r"));
            s.erase(s.find_last_not_of(" \t\n\r") + 1);
        };
        trim(pattern_name);
        trim(capture_name);

        // 查找模式定义
        auto pit = all_patterns.find(pattern_name);
        if (pit == all_patterns.end()) {
            return std::unexpected(
                std::string("Unknown Grok pattern: '") + pattern_name + "'");
        }

        // 构建替换文本
        std::string replacement;
        if (!capture_name.empty()) {
            // 有捕获名: 生成命名捕获组
            replacement = "(?P<" + capture_name + ">" + pit->second + ")";
        } else {
            // 无捕获名: 只展开模式
            replacement = "(?:" + pit->second + ")";
        }

        // 替换
        result.replace(start, end - start + 1, replacement);

        // 从替换位置继续（可能包含嵌套 Grok 模式）
        pos = start;
    }

    return result;
}

// ============================================================================
// parseTimestamp
// ============================================================================

int64_t RegexMapper::parseTimestamp(
    const std::string& raw,
    const std::vector<std::string>& formats) const {
    if (raw.empty()) return -1;

    // 如果指定了 format，按指定顺序尝试
    if (!formats.empty()) {
        // 这里只实现常用格式，复杂格式可以使用 date::parse (Howard Hinnant)
        // 生产环境中可引入 date.h 或 absl::Time
        // 目前先实现核心的格式检测
    }

    // 默认顺序尝试

    // 1. Unix epoch milliseconds (13 位数字)
    if (raw.size() == 13 && raw.find_first_not_of("0123456789") == std::string::npos) {
        int64_t ms;
        auto [ptr, ec] = std::from_chars(raw.data(), raw.data() + raw.size(), ms);
        if (ec == std::errc{}) return ms;
    }

    // 2. Unix epoch seconds (10 位数字)
    if (raw.size() == 10 && raw.find_first_not_of("0123456789") == std::string::npos) {
        int64_t sec;
        auto [ptr, ec] = std::from_chars(raw.data(), raw.data() + raw.size(), sec);
        if (ec == std::errc{}) return sec * 1000;
    }

    // 3. ISO 8601 格式: 2024-01-15T10:30:00Z 或 2024-01-15T10:30:00+08:00
    {
        std::tm tm = {};
        int tz_hour = 0;
        int tz_min = 0;
        char tz_sign = '+';
        int ms = 0;

        // 尝试带毫秒的格式
        int parsed = std::sscanf(raw.c_str(),
                                  "%4d-%2d-%2dT%2d:%2d:%2d.%3d%c%2d:%2d",
                                  &tm.tm_year, &tm.tm_mon, &tm.tm_mday,
                                  &tm.tm_hour, &tm.tm_min, &tm.tm_sec,
                                  &ms, &tz_sign, &tz_hour, &tz_min);
        if (parsed >= 7) {
            tm.tm_year -= 1900;
            tm.tm_mon -= 1;
            tm.tm_isdst = -1;

            auto time_epoch = timegm(&tm);
            if (time_epoch != -1) {
                int64_t result = static_cast<int64_t>(time_epoch) * 1000 + ms;

                // 调整时区
                if (parsed >= 9) {
                    int tz_offset = tz_hour * 3600 + tz_min * 60;
                    if (tz_sign == '-') tz_offset = -tz_offset;
                    result -= static_cast<int64_t>(tz_offset) * 1000;
                }
                return result;
            }
        }

        // 尝试无毫秒格式 (Z 后缀)
        std::tm tm2 = {};
        parsed = std::sscanf(raw.c_str(), "%4d-%2d-%2dT%2d:%2d:%2dZ",
                              &tm2.tm_year, &tm2.tm_mon, &tm2.tm_mday,
                              &tm2.tm_hour, &tm2.tm_min, &tm2.tm_sec);
        if (parsed == 6) {
            tm2.tm_year -= 1900;
            tm2.tm_mon -= 1;
            tm2.tm_isdst = -1;
            auto time_epoch = timegm(&tm2);
            if (time_epoch != -1) {
                return static_cast<int64_t>(time_epoch) * 1000;
            }
        }
    }

    // 4. Nginx 格式: 02/Jan/2006:15:04:05 +0700
    {
        std::tm tm = {};
        char month_str[4] = {};
        int tz_h = 0, tz_m = 0;
        char tz_sign_c = '+';

        int parsed = std::sscanf(raw.c_str(), "%2d/%3[^/]/%4d:%2d:%2d:%2d %c%2d%2d",
                                  &tm.tm_mday, month_str, &tm.tm_year,
                                  &tm.tm_hour, &tm.tm_min, &tm.tm_sec,
                                  &tz_sign_c, &tz_h, &tz_m);
        if (parsed == 9) {
            // 月份名转数字
            static const char* months[] = {
                "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
            };
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

                auto time_epoch = timegm(&tm);
                if (time_epoch != -1) {
                    int64_t result = static_cast<int64_t>(time_epoch) * 1000;
                    int tz_offset = tz_h * 3600 + tz_m * 60;
                    if (tz_sign_c == '-') tz_offset = -tz_offset;
                    result -= static_cast<int64_t>(tz_offset) * 1000;
                    return result;
                }
            }
        }
    }

    // 5. 简单格式: 2006-01-02 15:04:05
    {
        std::tm tm = {};
        int parsed = std::sscanf(raw.c_str(), "%4d-%2d-%2d %2d:%2d:%2d",
                                  &tm.tm_year, &tm.tm_mon, &tm.tm_mday,
                                  &tm.tm_hour, &tm.tm_min, &tm.tm_sec);
        if (parsed == 6) {
            tm.tm_year -= 1900;
            tm.tm_mon -= 1;
            tm.tm_isdst = -1;
            auto time_epoch = timegm(&tm);
            if (time_epoch != -1) {
                return static_cast<int64_t>(time_epoch) * 1000;
            }
        }
    }

    spdlog::warn("Failed to parse timestamp: '{}'", raw);
    return -1;
}

}  // namespace wge::kafka::mapper
