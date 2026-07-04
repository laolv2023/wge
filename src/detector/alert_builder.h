#pragma once

/**
 * @file alert_builder.h
 * @brief AlertBuilder — 从 AlertResult 构建 WgeAlertEvent
 *
 * AlertBuilder 将内部检测结果 AlertResult 与原始请求上下文信息
 * 合并为完整的 WgeAlertEvent protobuf 消息，用于发送到 Kafka 告警 topic。
 *
 * 线程安全: 所有方法为静态纯函数，不访问共享状态，天然线程安全。
 */

#include <cctype>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace wge::kafka {

// 前向声明 protobuf 类型
class WgeAlertEvent;

namespace detector {

// 前向声明
struct AlertResult;

// ============================================================================
// AlertBuilder
// ============================================================================

/**
 * @brief 告警事件构建器
 *
 * 将 AlertResult + 上下文信息合并为完整 WgeAlertEvent protobuf。
 *
 * 使用示例:
 * @code
 *   auto alert = AlertBuilder::build(
 *       result, event->event_id(), event->collector_id(),
 *       event->request_method(), event->request_uri(),
 *       event->downstream_ip(), event->upstream_ip());
 * @endcode
 */
class AlertBuilder {
public:
    /**
     * @brief 从 AlertResult 构建 WgeAlertEvent
     *
     * @param result          检测结果 (匹配规则 + 阻断信息)
     * @param event_id        关联的原始 HttpAccessEvent.event_id
     * @param collector_id    采集源标识
     * @param request_method  HTTP 请求方法
     * @param request_uri     请求 URI
     * @param downstream_ip   下游 (客户端) IP
     * @param upstream_ip     上游 (后端) IP
     * @return std::shared_ptr<WgeAlertEvent> 完整的告警事件
     * @throws std::runtime_error 若 result.event_id 为空
     *
     * @note 生成的 alert_id 使用 UUID v7 格式 (基于时间戳 + 随机数)。
     * @note 线程安全: 纯静态函数，无共享状态。
     */
    [[nodiscard]] static std::shared_ptr<WgeAlertEvent> build(
        const AlertResult& result,
        const std::string& event_id,
        const std::string& collector_id,
        const std::string& request_method,
        const std::string& request_uri,
        const std::string& downstream_ip,
        const std::string& upstream_ip,
        // Akto 透传字段 (从 HttpAccessEvent 透传)
        const std::string& akto_account_id = "",
        int32_t akto_collection_id = 0,
        const std::string& request_body = "",
        int32_t response_status_code = 0,
        const std::string& request_host = "");

    /// @brief 禁止实例化
    AlertBuilder() = delete;

private:
    /// @brief 从 operator_name / rule_tags 推断攻击类型 (Akto sub_category)
    [[nodiscard]] static std::string inferAttackType(
        const std::string& operator_name,
        const std::vector<std::string>& rule_tags);

    /// @brief WGE 数字严重级别 → Akto 字符串 (CRITICAL/HIGH/MEDIUM/LOW)
    [[nodiscard]] static std::string mapSeverityToAkto(int wge_severity);
    /**
     * @brief 生成 UUID v7 (timestamp-ordered UUID)
     *
     * UUID v7 格式: 前 48 bits 为 Unix 时间戳 (ms)，后续为随机数。
     * 格式: 00000000-0000-7xxx-yxxx-xxxxxxxxxxxx
     *
     * @return std::string UUID v7 字符串 (36 字符 + null)
     */
    [[nodiscard]] static std::string generateUuidV7();
};

}  // namespace detector
}  // namespace wge::kafka
