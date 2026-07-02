/**
 * @file akto_adapter.h
 * @brief WGE-Akto Adapter: WgeAlertEvent → MaliciousEventKafkaEnvelope 协议转换
 *
 * 实施报告第5章描述的5大功能模块:
 *   1. 告警分级过滤阀 (丢弃限流/合规噪音)
 *   2. 降维 IP 级限流器 (防扫描器 URL 遍历雪崩)
 *   3. 保守穿透判定 (避免与 Akto YAML 规则冲突)
 *   4. 上下文与标签强制打标 (context_source / label)
 *   5. api_collection_id 防0兜底
 *
 * 来源: 架构设计报告第5章 + Akto proto MaliciousEventMessage
 */

#pragma once

#include <string>
#include <unordered_map>
#include <chrono>
#include <mutex>
#include <deque>

namespace wge::akto {

/**
 * @brief Akto 攻击类型映射表 (WGE攻击类型 → Akto sub_category)
 *
 * 对齐 Akto ThreatCategory.java 中的硬编码枚举
 */
static const std::unordered_map<std::string, std::string> AKTO_FILTER_ID_MAP = {
    {"SQLi",              "SQLInjection"},
    {"XSS",               "XSS"},
    {"LFI",               "LocalFileInclusionLFIRFI"},
    {"RFI",               "LocalFileInclusionLFIRFI"},
    {"RCE",               "OSCommandInjection"},
    {"CommandInjection",  "OSCommandInjection"},
    {"SSRF",              "SSRF"},
    {"PathTraversal",     "LocalFileInclusionLFIRFI"},
    {"XXE",               "XXE"},
    {"SecurityMisconfig", "SecurityMisconfig"},
};

/**
 * @brief IP 级限流器 (降维限流: Key = IP + Account + Category)
 *
 * 防止扫描器 URL 遍历导致 Akto MongoDB CPU 飙升
 * 限制: 每个 IP+Account+Category 组合 ≤5条/分钟
 */
class IpRateLimiter {
public:
    bool allow(const std::string& ip, const std::string& account_id,
               const std::string& category, int max_per_minute = 5);

private:
    struct Key {
        std::string ip;
        std::string account_id;
        std::string category;
        bool operator==(const Key& o) const {
            return ip == o.ip && account_id == o.account_id && category == o.category;
        }
    };
    struct KeyHash {
        size_t operator()(const Key& k) const {
            return std::hash<std::string>()(k.ip) ^
                   (std::hash<std::string>()(k.account_id) << 1) ^
                   (std::hash<std::string>()(k.category) << 2);
        }
    };
    struct TimestampWindow {
        std::deque<int64_t> timestamps;
    };
    std::unordered_map<Key, TimestampWindow, KeyHash> windows_;
    std::mutex mutex_;
};

/**
 * @brief WGE-Akto Adapter: 将 WgeAlertEvent 转换为 Akto 兼容的 JSON 消息
 *
 * 输出格式对齐 Akto MaliciousEventKafkaEnvelope:
 * {
 *   "account_id": "...",
 *   "actor": "...",
 *   "malicious_event": {
 *     "actor": "...",
 *     "filter_id": "WGE_...",
 *     "detected_at": 1234567890,
 *     "latest_api_ip": "...",
 *     "latest_api_endpoint": "...",
 *     "latest_api_method": "...",
 *     "latest_api_collection_id": 1,
 *     "latest_api_payload": "...",
 *     "event_type": 1,  // EVENT_TYPE_SINGLE
 *     "category": "ApiAbuse",
 *     "sub_category": "SQLInjection",
 *     "severity": "HIGH",
 *     "successful_exploit": false,
 *     "label": "THREAT",
 *     "host": "...",
 *     "status": "...",
 *     "context_source": "API"
 *   }
 * }
 */
class AktoAdapter {
public:
    /**
     * @brief 处理一条 WGE 告警, 返回 Akto 兼容的 JSON 字符串
     *
     * 5大功能模块:
     *   1. 告警分级过滤: 丢弃 RateLimit/LOW 级别
     *   2. IP 级限流: ≤5条/分钟/IP+Account+Category
     *   3. 保守穿透判定: successful_exploit 始终 false
     *   4. 强制打标: context_source="API", label="THREAT"
     *   5. api_collection_id 防0: 为0时用 Host 兜底, 兜底失败则丢弃
     *
     * @param alert_json WgeAlertEvent 的 JSON 字符串
     * @return Akto 格式 JSON 字符串, 若被过滤则返回空字符串
     */
    std::string convert(const std::string& alert_json);

private:
    IpRateLimiter rate_limiter_;

    /// api_collection_id 防0兜底: Host → CollectionID 映射
    static const std::unordered_map<std::string, int32_t> HOST_COLLECTION_FALLBACK;

    /**
     * @brief 构造 Akto MaliciousEventMessage JSON
     */
    std::string buildMaliciousEventJson(
        const std::string& actor_ip,
        const std::string& filter_id,
        int64_t detected_at,
        const std::string& api_ip,
        const std::string& api_endpoint,
        const std::string& api_method,
        int32_t api_collection_id,
        const std::string& api_payload,
        const std::string& sub_category,
        const std::string& severity,
        const std::string& host,
        const std::string& status
    );
};

}  // namespace wge::akto
