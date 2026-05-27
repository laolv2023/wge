/// @file akto_preprocessor.h
/// @brief Akto API 日志预处理器 — 处理 JSON 字符串嵌套的特殊格式
///
/// Akto 的 Kafka 输出有 3 个特殊之处:
/// 1. requestHeaders / responseHeaders 是 JSON 字符串, 不是 JSON 对象
///    → 需要二次解析, 展开为 [{"key":"...","value":"..."}] 格式
/// 2. time 字段是 Unix 秒级时间戳, 非毫秒级
///    → 需要 ×1000
/// 3. type 字段是 "HTTP/1.1" 格式
///    → 需要 strip "HTTP/" 前缀
///
/// 预处理流程:
///   原始 JSON 字符串 → 解析 → 展开 headers → 修正 time → 修正 type → 重新序列化为 JSON → 交给标准 mapper

#pragma once

#include <string>
#include <expected>
#include <string_view>

namespace wge::kafka::adapter {

/// @brief Akto 预处理器
///
/// 线程安全: 所有方法为 const, 无内部可变状态
class AktoPreprocessor {
public:
    /// @brief 预处理 Akto 原始 JSON 日志
    /// @param raw_json 原始 Kafka 消息 payload (完整的 Akto JSON 字符串)
    /// @return 预处理后的 JSON 字符串 (可直接交给标准 JsonMapper), 或错误信息
    ///
    /// 处理步骤:
    ///   1. 解析原始 JSON
    ///   2. 展开 requestHeaders (JSON 字符串 → 结构化数组)
    ///   3. 展开 responseHeaders (JSON 字符串 → 结构化数组)
    ///   4. 修正 time 字段 (秒 → 毫秒)
    ///   5. 修正 type 字段 (HTTP/1.1 → 1.1)
    ///   6. 重新序列化为 JSON 字符串
    static std::expected<std::string, std::string>
    preprocess(std::string_view raw_json);

private:
    /// @brief 展开 JSON 字符串格式的 headers
    ///
    /// 输入: "{\"Accept\":\"application/json\",\"Host\":\"example.com\"}"
    /// 输出: [{"key":"Accept","value":"application/json"},{"key":"Host","value":"example.com"}]
    ///
    /// @param header_json_str JSON 字符串表示的 headers
    /// @return 结构化 JSON 字符串, 或错误
    static std::expected<std::string, std::string>
    expandHeaderJsonString(std::string_view header_json_str);

    /// @brief 修正秒级时间戳为毫秒级
    /// @param epoch_seconds Unix 秒级时间戳
    /// @return 毫秒级时间戳
    static std::string fixTimestampSecondsToMs(std::string_view epoch_seconds);

    /// @brief 修正 HTTP 版本号
    /// @param http_version 如 "HTTP/1.1" 或 "HTTP/2.0"
    /// @return 纯版本号 如 "1.1" 或 "2.0"
    static std::string fixHttpVersion(std::string_view http_version);
};

} // namespace wge::kafka::adapter
