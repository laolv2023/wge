/**
 * @file akto_adapter.cc
 * @brief WGE-Akto Adapter 实现
 *
 * 将 WgeAlertEvent 转换为 Akto MaliciousEventKafkaEnvelope JSON 格式,
 * 写入 akto.threat_detection.malicious_events Topic。
 *
 * V6.0 升级:
 *   - 集成 Prometheus 指标埋点
 *   - 集成 DLQ 死信队列
 *   - JSON 字符串转义防注入
 */

#include "akto_adapter.h"
#include "akto_metrics.h"
#include "akto_dlq.h"

#include <spdlog/spdlog.h>
#include <simdjson.h>
#include <chrono>
#include <sstream>

namespace wge::akto {

// Host → CollectionID 兜底映射 (生产环境应从 Akto API 同步)
const std::unordered_map<std::string, int32_t> AktoAdapter::HOST_COLLECTION_FALLBACK = {
    {"api.example.com", 1},
    {"admin.example.com", 2},
};

// ============================================================================
// JSON 字符串转义 (防止注入)
// ============================================================================

std::string AktoAdapter::escapeJson(const std::string& s) {
    std::string result;
    result.reserve(s.size() + 16);
    for (char c : s) {
        switch (c) {
            case '"':  result += "\\\""; break;
            case '\\': result += "\\\\"; break;
            case '\n': result += "\\n";  break;
            case '\r': result += "\\r";  break;
            case '\t': result += "\\t";  break;
            case '\b': result += "\\b";  break;
            case '\f': result += "\\f";  break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    // 控制字符 → \uXXXX
                    char buf[8];
                    snprintf(buf, sizeof(buf), "\\u%04x", c);
                    result += buf;
                } else {
                    result += c;
                }
                break;
        }
    }
    return result;
}

// ============================================================================
// IpRateLimiter
// ============================================================================

bool IpRateLimiter::allow(const std::string& ip, const std::string& account_id,
                           const std::string& category, int max_per_minute) {
    std::lock_guard<std::mutex> lock(mutex_);
    Key key{ip, account_id, category};
    auto& window = windows_[key];

    auto now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // 清理1分钟前的时间戳
    while (!window.timestamps.empty() && window.timestamps.front() < now - 60) {
        window.timestamps.pop_front();
    }

    if (static_cast<int>(window.timestamps.size()) >= max_per_minute) {
        return false;
    }

    window.timestamps.push_back(now);
    return true;
}

// ============================================================================
// AktoAdapter
// ============================================================================

AktoAdapter::AktoAdapter(std::shared_ptr<AktoDlq> dlq)
    : dlq_(std::move(dlq)) {}

std::string AktoAdapter::convert(const std::string& alert_json) {
    // 指标: 告警消费计数
    AktoMetrics::instance().incAlertsConsumed();

    simdjson::ondemand::parser parser;
    simdjson::ondemand::document doc;

    auto error = parser.allocate(alert_json.size());
    if (error) {
        spdlog::warn("[akto_adapter] JSON parser allocate failed: {}", simdjson::error_message(error));
        return "";
    }

    error = parser.parse(alert_json).get(doc);
    if (error) {
        spdlog::warn("[akto_adapter] JSON parse failed: {}", simdjson::error_message(error));
        return "";
    }

    // 提取字段
    std::string alert_id, attack_type, severity, request_method, request_uri;
    std::string downstream_ip, akto_account_id, request_host, request_body, response_status;
    int64_t timestamp_ms = 0;
    int32_t akto_collection_id = 0;
    bool successful_exploit = false;

    try {
        doc.get_string("alert_id").get(alert_id);
        doc.get_int64("timestamp_ms").get(timestamp_ms);
        doc.get_string("attack_type").get(attack_type);
        doc.get_string("severity").get(severity);
        doc.get_string("request_method").get(request_method);
        doc.get_string("request_uri").get(request_uri);
        doc.get_string("downstream_ip").get(downstream_ip);
        doc.get_string("akto_account_id").get(akto_account_id);
        doc.get_int64("akto_collection_id").get(akto_collection_id);
        doc.get_string("request_host").get(request_host);
        doc.get_string("request_body").get(request_body);
        doc.get_string("response_status").get(response_status);
        doc.get_bool("successful_exploit").get(successful_exploit);
    } catch (const simdjson::simdjson_error& e) {
        // 部分字段可能不存在, 继续
    }

    // ── 功能1: 告警分级过滤 ──
    // 丢弃限流/低危噪音, 防止 CloudflareWafSyncCron 误封禁
    if (attack_type == "RateLimit" || severity == "LOW") {
        spdlog::debug("[akto_adapter] Dropping low-severity/RateLimit alert: {}", alert_id);
        return "";
    }

    // ── 功能2: 多租户生命线校验 ──
    if (akto_account_id.empty()) {
        spdlog::warn("[akto_adapter] Dropping alert without akto_account_id: {}", alert_id);
        return "";
    }

    // ── 功能3: api_collection_id 防0兜底 ──
    if (akto_collection_id == 0) {
        auto it = HOST_COLLECTION_FALLBACK.find(request_host);
        if (it != HOST_COLLECTION_FALLBACK.end()) {
            akto_collection_id = it->second;
            spdlog::info("[akto_adapter] Collection ID fallback: host={} → id={}", request_host, akto_collection_id);
        } else {
            // 指标: collection_id=0 丢弃
            AktoMetrics::instance().incCollectionIdZeroDrops();
            spdlog::warn("[akto_adapter] Dropping alert: api_collection_id=0 and no host fallback for {}", request_host);
            return "";  // 严禁将0注入Akto
        }
    }

    // ── 功能4: 攻击类型映射 ──
    std::string sub_category = attack_type;
    auto it = AKTO_FILTER_ID_MAP.find(attack_type);
    if (it != AKTO_FILTER_ID_MAP.end()) {
        sub_category = it->second;
    }

    // ── 功能5: IP 级限流 (≤5条/分钟/IP+Account+Category) ──
    if (!rate_limiter_.allow(downstream_ip, akto_account_id, sub_category)) {
        // 指标: 限流丢弃
        AktoMetrics::instance().incRateLimitedDrops();
        spdlog::debug("[akto_adapter] Rate limited: ip={} category={}", downstream_ip, sub_category);
        return "";
    }

    // ── 构造 Akto JSON ──
    // filter_id 格式: WGE_{rule_id}
    std::string filter_id = "WGE_" + alert_id.substr(0, 8);

    // detected_at: 毫秒 → 秒
    int64_t detected_at = timestamp_ms / 1000;

    // 保守穿透判定: 始终 false (避免与 Akto YAML 规则冲突)
    // successful_exploit 已从 proto 读取, 默认 false

    // 构造 JSON (所有字符串字段使用 escapeJson 转义, 防止注入)
    std::ostringstream oss;
    oss << "{";
    // MaliciousEventKafkaEnvelope
    oss << "\"account_id\":\"" << escapeJson(akto_account_id) << "\",";
    oss << "\"actor\":\"" << escapeJson(downstream_ip) << "\",";
    oss << "\"malicious_event\":{";
    // MaliciousEventMessage
    oss << "\"actor\":\"" << escapeJson(downstream_ip) << "\",";
    oss << "\"filter_id\":\"" << escapeJson(filter_id) << "\",";
    oss << "\"detected_at\":" << detected_at << ",";
    oss << "\"latest_api_ip\":\"" << escapeJson(downstream_ip) << "\",";
    oss << "\"latest_api_endpoint\":\"" << escapeJson(request_uri) << "\",";
    oss << "\"latest_api_method\":\"" << escapeJson(request_method) << "\",";
    oss << "\"latest_api_collection_id\":" << akto_collection_id << ",";
    oss << "\"latest_api_payload\":\"" << escapeJson(request_body) << "\",";
    oss << "\"event_type\":1,";  // EVENT_TYPE_SINGLE
    oss << "\"category\":\"ApiAbuse\",";
    oss << "\"sub_category\":\"" << escapeJson(sub_category) << "\",";
    oss << "\"severity\":\"" << escapeJson(severity) << "\",";
    oss << "\"successful_exploit\":" << (successful_exploit ? "true" : "false") << ",";
    oss << "\"label\":\"THREAT\",";
    oss << "\"host\":\"" << escapeJson(request_host) << "\",";
    oss << "\"status\":\"" << escapeJson(response_status) << "\",";
    oss << "\"context_source\":\"API\"";
    oss << "}}";

    // 指标: Akto 事件产出计数
    AktoMetrics::instance().incAktoEventsProduced();

    return oss.str();
}

}  // namespace wge::akto
