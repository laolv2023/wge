#pragma once

/**
 * @file regex_mapper.h
 * @brief 正则表达式 / Grok 日志映射器
 *
 * 使用 Google RE2 进行高性能正则表达式匹配。
 * 支持命名捕获组提取、Grok 模式转换和时间戳解析。
 *
 * 线程安全: RE2 对象构造后不可变，所有 const 方法线程安全。
 *           编译缓存使用 mutex 保护，线程安全。
 */

#include <expected>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "mapper/mapper_config.h"

namespace re2 {
class RE2;
}

namespace wge::kafka::mapper {

/**
 * @brief 正则表达式映射器
 *
 * 使用 RE2 进行正则匹配，支持:
 * - 命名捕获组提取
 * - Grok 模式（转换为 RE2 正则表达式）
 * - 多格式时间戳解析
 */
class RegexMapper {
public:
    RegexMapper() = default;
    ~RegexMapper();

    // 不可拷贝、可移动
    RegexMapper(const RegexMapper&) = delete;
    RegexMapper& operator=(const RegexMapper&) = delete;
    RegexMapper(RegexMapper&&) = default;
    RegexMapper& operator=(RegexMapper&&) = default;

    /**
     * @brief 编译正则表达式（带缓存）
     *
     * @param pattern RE2 正则表达式模式
     * @return std::expected<void, std::string>
     */
    [[nodiscard]] std::expected<void, std::string>
    compile(const std::string& pattern);

    /**
     * @brief 使用正则表达式提取字段
     *
     * 对 raw_payload 执行 FullMatch，提取命名捕获组的值。
     *
     * @param raw_payload 原始日志行
     * @param pattern 正则表达式模式
     * @param field_mappings 字段映射 (source = 捕获组名)
     * @return std::expected<std::map<std::string, std::string>, std::string>
     */
    [[nodiscard]] std::expected<std::map<std::string, std::string>, std::string>
    extract(std::string_view raw_payload, const std::string& pattern,
            const std::vector<FieldMapping>& field_mappings) const;

    /**
     * @brief 使用 Grok 模式提取字段
     *
     * 先将 Grok 模式转换为 RE2 正则表达式，再执行匹配。
     *
     * @param raw_payload 原始日志行
     * @param grok_pattern Grok 表达式 (如 "%{IP:client_ip} %{HTTPD:request}")
     * @param custom_patterns 自定义 Grok 模式
     * @param field_mappings 字段映射
     * @return std::expected<std::map<std::string, std::string>, std::string>
     */
    [[nodiscard]] std::expected<std::map<std::string, std::string>, std::string>
    extractGrok(std::string_view raw_payload, const std::string& grok_pattern,
                const std::map<std::string, std::string>& custom_patterns,
                const std::vector<FieldMapping>& field_mappings) const;

    /**
     * @brief 解析时间戳字符串
     *
     * 按 formats 顺序尝试解析，若无指定格式则按默认顺序:
     *   1. ISO 8601: "2006-01-02T15:04:05Z07:00"
     *   2. Nginx:    "02/Jan/2006:15:04:05 -0700"
     *   3. Simple:   "2006-01-02 15:04:05"
     *   4. Unix epoch (seconds)
     *   5. Unix epoch (milliseconds)
     *
     * @param raw 时间戳字符串
     * @param formats 尝试的格式列表，空则使用默认顺序
     * @return int64_t epoch milliseconds，解析失败返回 -1
     */
    [[nodiscard]] int64_t parseTimestamp(
        const std::string& raw,
        const std::vector<std::string>& formats) const;

private:
    /// @brief 当前编译的 RE2 对象
    std::unique_ptr<re2::RE2> compiled_regex_;

    /// @brief 编译缓存: pattern → RE2 对象
    /// @note 使用 mutex 保护，线程安全
    static std::mutex cache_mutex_;
    static std::map<std::string, std::shared_ptr<re2::RE2>> compile_cache_;

    /**
     * @brief 获取或创建编译后的 RE2 对象
     */
    [[nodiscard]] std::expected<std::shared_ptr<re2::RE2>, std::string>
    getOrCompile(const std::string& pattern) const;

    /**
     * @brief 将 Grok 模式转为 RE2 正则表达式
     */
    [[nodiscard]] static std::expected<std::string, std::string>
    grokToRegex(const std::string& grok_pattern,
                const std::map<std::string, std::string>& custom_patterns);

    /**
     * @brief 获取内置 Grok 模式定义
     */
    [[nodiscard]] static const std::map<std::string, std::string>&
    builtinGrokPatterns();

    /**
     * @brief 进行 FullMatch 并提取命名捕获组
     */
    [[nodiscard]] std::expected<std::map<std::string, std::string>, std::string>
    fullMatchAndExtract(std::string_view raw_payload, const re2::RE2& regex,
                        const std::vector<FieldMapping>& field_mappings) const;
};

}  // namespace wge::kafka::mapper
