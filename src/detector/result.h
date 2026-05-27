#pragma once

/**
 * @file result.h
 * @brief Detector 检测结果数据结构
 *
 * 定义 WGE 检测引擎输出的匹配规则信息 MatchedRuleInfo
 * 和聚合检测结果 AlertResult。
 *
 * 线程安全: 所有结构体为值类型，拷贝后互不干扰，天然线程安全。
 */

#include <cstdint>
#include <string>
#include <vector>

namespace wge::kafka::detector {

// ============================================================================
// MatchedRuleInfo
// ============================================================================

/**
 * @brief 单条匹配规则信息
 *
 * 描述 WGE 引擎匹配到的一条安全规则及其上下文。
 * 从 WGE Rule::detail_ 和 matched_variables 中提取。
 */
struct MatchedRuleInfo {
    /// @brief 规则 ID (SecRule id)
    uint64_t rule_id{0};

    /// @brief 规则消息/描述
    std::string rule_msg{};

    /// @brief 规则严重级别数字 (0=EMERGENCY .. 7=DEBUG)
    int severity{0};

    /// @brief 规则集版本
    std::string rule_ver{};

    /// @brief 规则标签列表
    std::vector<std::string> rule_tags{};

    /// @brief 匹配变量名 (如 REQUEST_URI, ARGS, REQUEST_HEADERS:User-Agent)
    std::string matched_var_name{};

    /// @brief 匹配变量值 (变换后)
    std::string matched_var_value{};

    /// @brief 匹配变量原始值
    std::string matched_var_original{};

    /// @brief 操作符名称 (如 @rx, @detectSQLi, @detectXSS)
    std::string operator_name{};

    /// @brief 操作符参数 (如正则表达式)
    std::string operator_param{};
};

// ============================================================================
// AlertResult — 聚合检测结果
// ============================================================================

/**
 * @brief 聚合检测结果
 *
 * 一次检测 (一个 HttpAccessEvent) 可能匹配多条规则。
 * AlertResult 聚合所有匹配规则及阻断决策信息。
 */
struct AlertResult {
    /// @brief 关联的事件 ID
    std::string event_id{};

    /// @brief 检测时间戳 (epoch ms)
    int64_t timestamp_ms{0};

    /// @brief 是否触发了阻断/干预动作
    bool intervened{false};

    /// @brief 阻断动作类型: DENY / DROP / REDIRECT / ALLOW / PASS
    std::string disruptive_action{};

    /// @brief 规则设定的 HTTP 响应码 (如 403)
    int response_code{0};

    /// @brief REDIRECT 目标 URL
    std::string redirect_url{};

    /// @brief 所有匹配的规则列表
    std::vector<MatchedRuleInfo> matched_rules{};

    /**
     * @brief 是否有任何规则匹配
     */
    [[nodiscard]] bool hasMatches() const noexcept {
        return !matched_rules.empty();
    }
};

// ============================================================================
// severityToString — 数字严重级别 → 可读字符串
// ============================================================================

/**
 * @brief 将数字严重级别转换为可读字符串
 *
 * 映射关系 (参考 syslog RFC 5424):
 *   0 → EMERGENCY
 *   1 → ALERT
 *   2 → CRITICAL
 *   3 → ERROR
 *   4 → WARNING
 *   5 → NOTICE
 *   6 → INFO
 *   7 → DEBUG
 *   其他 → UNKNOWN
 *
 * @param severity 数字严重级别
 * @return const char* 可读的严重级别字符串 (静态生命周期)
 */
[[nodiscard]] inline const char* severityToString(int severity) noexcept {
    switch (severity) {
        case 0:  return "EMERGENCY";
        case 1:  return "ALERT";
        case 2:  return "CRITICAL";
        case 3:  return "ERROR";
        case 4:  return "WARNING";
        case 5:  return "NOTICE";
        case 6:  return "INFO";
        case 7:  return "DEBUG";
        default: return "UNKNOWN";
    }
}

}  // namespace wge::kafka::detector
