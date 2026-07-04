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
    for (unsigned char uc : s) {
        switch (uc) {
            case '"':  result += "\\\""; break;
            case '\\': result += "\\\\"; break;
            case '\n': result += "\\n";  break;
            case '\r': result += "\\r";  break;
            case '\t': result += "\\t";  break;
            case '\b': result += "\\b";  break;
            case '\f': result += "\\f";  break;
            default:
                if (uc < 0x20) {
                    // 控制字符 → \uXXXX
                    char buf[8];
                    snprintf(buf, sizeof(buf), "\\u%04x",
                             static_cast<unsigned int>(uc));
                    result += buf;
                } else {
                    result += static_cast<char>(uc);
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

    // 定期清理空窗口，防止 windows_ map 无限增长导致 OOM。
    // 每处理 1024 次调用执行一次清理（摊销开销极低）。
    if (++cleanup_counter_ % 1024 == 0) {
        cleanupEmptyWindows(now);
    }

    return true;
}

void IpRateLimiter::cleanupEmptyWindows(int64_t now) {
    // 清理所有时间戳已全部过期（窗口为空）的条目
    for (auto it = windows_.begin(); it != windows_.end(); ) {
        auto& w = it->second;
        // 先清理过期时间戳
        while (!w.timestamps.empty() && w.timestamps.front() < now - 60) {
            w.timestamps.pop_front();
        }
        // 如果窗口为空，删除该条目
        if (w.timestamps.empty()) {
            it = windows_.erase(it);
        } else {
            ++it;
        }
    }
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

    simdjson::padded_string padded_json(alert_json);
    auto error = parser.iterate(padded_json).get(doc);
    if (error) {
        spdlog::warn("[akto_adapter] JSON parse failed: {}", simdjson::error_message(error));
        // 发送到 DLQ，避免数据丢失
        if (dlq_) {
            dlq_->send(alert_json, std::string("JSON parse failed: ") + simdjson::error_message(error));
        }
        return "";
    }

    // 提取字段
    std::string alert_id, attack_type, severity, request_method, request_uri;
    std::string downstream_ip, akto_account_id, request_host, request_body, response_status;
    int64_t timestamp_ms = 0;
    int32_t akto_collection_id = 0;
    bool successful_exploit = false;

    try {
        // 使用 simdjson ondemand API: doc["key"].get_string(output)
        std::string_view sv;
        if (!doc["alert_id"].get_string().get(sv)) alert_id = sv;
        if (!doc["timestamp_ms"].get_int64().get(timestamp_ms)) {}
        if (!doc["attack_type"].get_string().get(sv)) attack_type = sv;
        if (!doc["severity"].get_string().get(sv)) severity = sv;
        if (!doc["request_method"].get_string().get(sv)) request_method = sv;
        if (!doc["request_uri"].get_string().get(sv)) request_uri = sv;
        if (!doc["downstream_ip"].get_string().get(sv)) downstream_ip = sv;
        if (!doc["akto_account_id"].get_string().get(sv)) akto_account_id = sv;
        int64_t collection_id_tmp = 0;
        if (!doc["akto_collection_id"].get_int64().get(collection_id_tmp)) {
            akto_collection_id = static_cast<int32_t>(collection_id_tmp);
        }
        if (!doc["request_host"].get_string().get(sv)) request_host = sv;
        if (!doc["request_body"].get_string().get(sv)) request_body = sv;
        if (!doc["response_status"].get_string().get(sv)) response_status = sv;
        bool exploit_tmp = false;
        if (!doc["successful_exploit"].get_bool().get(exploit_tmp)) {
            successful_exploit = exploit_tmp;
        }
    } catch (const simdjson::simdjson_error& e) {
        // 部分字段可能不存在, 继续
        spdlog::debug("[akto_adapter] Partial field extraction error: {}", e.what());
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
        // 发送到 DLQ，避免数据丢失
        if (dlq_) {
            dlq_->send(alert_json, "Missing akto_account_id");
        }
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
            // 发送到 DLQ，避免数据丢失
            if (dlq_) {
                dlq_->send(alert_json, "api_collection_id=0 and no host fallback");
            }
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
    // 生成 filter_id: WGE_ + alert_id 前8位
    // 如果 alert_id 不足8位，使用全部内容（substr 安全处理短字符串）
    std::string filter_id = "WGE_" + alert_id.substr(0, 8);

    // detected_at: 毫秒 → 秒
    // 如果 timestamp_ms 为 0（字段提取失败），使用当前时间作为兜底
    int64_t detected_at;
    if (timestamp_ms > 0) {
        detected_at = timestamp_ms / 1000;
    } else {
        detected_at = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        spdlog::warn("[akto_adapter] timestamp_ms is 0 or missing for alert {}, "
                     "using current time as detected_at", alert_id);
    }

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
