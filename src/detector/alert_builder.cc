/**
 * @file alert_builder.cc
 * @brief AlertBuilder 实现 — 告警构建器
 *
 * ## 模块职责
 * AlertBuilder 将检测结果（AlertResult）和原始事件信息组合，
 * 构建 WgeAlertEvent protobuf 消息。
 *
 * ## 功能
 * - **UUID v7 生成**: 基于时间戳的 UUID（毫秒精度），保证告警 ID 全局唯一且可排序
 * - **时间戳补全**: 若 result 中无时间戳，使用当前系统时间
 * - **阻断动作映射**: 根据 intervened/response_code/redirect_url 推导 disruptive_action
 *   (ALLOW / DENY / DROP / REDIRECT)
 * - **匹配规则填充**: 将 matched_rules 列表写入 protobuf repeated 字段
 */

#include "detector/alert_builder.h"

#include <chrono>
#include <random>
#include <sstream>
#include <stdexcept>

#include "detector/result.h"
#include "spdlog/spdlog.h"
#include "wge_alert.pb.h"

namespace wge::kafka::detector {

// ============================================================================
// UUID v7 生成
// ============================================================================

std::string AlertBuilder::generateUuidV7() {
    // UUID v7 格式 (RFC 9562):
    //   timestamp_ms (48 bits) + version (4 bits) +
    //   random_a (12 bits) + variant (2 bits) + random_b (62 bits)
    //
    // 格式: 00000000-0000-7xxx-yxxx-xxxxxxxxxxxx
    // - 前 12 hex 位: Unix 毫秒时间戳（单调递增）
    // - 第 13 位:    版本号固定为 7
    // - 第 17 位:    变体固定为 10xx (0x8-0xB)

    using clock = std::chrono::system_clock;
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      clock::now().time_since_epoch())
                      .count();

    // 线程安全的随机数生成（thread_local + mt19937_64）
    thread_local std::random_device rd;
    thread_local std::mt19937_64 gen(rd());
    thread_local std::uniform_int_distribution<uint64_t> dist;

    // RFC 9562 UUIDv7 位布局:
    //   bits [0:47]   = unix_ts_ms (48 bits)
    //   bits [48:51]  = ver = 0x7 (4 bits)
    //   bits [52:63]  = rand_a (12 bits)
    //   bits [64:65]  = var = 0b10 (2 bits)
    //   bits [66:127] = rand_b (62 bits)
    uint64_t rand_a = dist(gen) & 0xFFF;        // 12 bits 随机部分 A
    uint64_t rand_b = dist(gen) & 0x3FFFFFFFFFFFFFFFULL;  // 62 bits 随机部分 B

    // 时间戳: 取低 48 bits（支持到公元 10889 年）
    uint64_t ts = static_cast<uint64_t>(now_ms) & 0xFFFFFFFFFFFFULL;

    std::ostringstream oss;
    oss << std::hex << std::setfill('0');

    // time_low: timestamp 的 [47:16] 位 → 8 hex
    oss << std::setw(8) << (ts >> 16);
    oss << '-';
    // time_mid: timestamp 的 [15:0] 位 → 4 hex
    oss << std::setw(4) << (ts & 0xFFFF);
    oss << '-';
    // version (7) + rand_a (12 bits): '7' + 3 hex
    oss << '7' << std::setw(3) << (rand_a & 0xFFF);
    oss << '-';
    // variant (10xx) + rand_b 高 6 位: 2 hex
    // variant_byte = 0b10xxxxxx = 0x80 | (rand_b >> 56)
    uint8_t variant_byte = static_cast<uint8_t>(
        0x80 | ((rand_b >> 56) & 0x3F));
    oss << std::setw(2) << static_cast<unsigned>(variant_byte);
    // rand_b 中间 8 位: 2 hex
    oss << std::setw(2) << static_cast<unsigned>(
        (rand_b >> 48) & 0xFF);
    oss << '-';
    // rand_b 低 48 位: 12 hex
    oss << std::setw(12) << (rand_b & 0xFFFFFFFFFFFFULL);

    return oss.str();
}

// ============================================================================
// build
// ============================================================================

std::shared_ptr<WgeAlertEvent> AlertBuilder::build(
    const AlertResult& result,
    const std::string& event_id,
    const std::string& collector_id,
    const std::string& request_method,
    const std::string& request_uri,
    const std::string& downstream_ip,
    const std::string& upstream_ip,
    // Akto 透传字段
    const std::string& akto_account_id,
    int32_t akto_collection_id,
    const std::string& request_body,
    int32_t response_status_code,
    const std::string& request_host) {

    if (event_id.empty()) {
        throw std::runtime_error("AlertBuilder::build: event_id is empty");
    }

    auto alert = std::make_shared<WgeAlertEvent>();

    // ---- 告警元信息 ----
    alert->set_alert_id(generateUuidV7());
    alert->set_timestamp_ms(
        result.timestamp_ms > 0
            ? result.timestamp_ms
            : std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::system_clock::now().time_since_epoch())
                  .count());

    // ---- 关联信息 ----
    alert->set_event_id(event_id);
    alert->set_collector_id(collector_id);

    // ---- 阻断信息 ----
    alert->set_intervened(result.intervened);
    alert->set_disruptive_action(result.disruptive_action);
    alert->set_response_code(result.response_code);
    alert->set_redirect_url(result.redirect_url);

    // ---- 原始请求摘要 ----
    alert->set_request_method(request_method);
    alert->set_request_uri(request_uri);
    alert->set_downstream_ip(downstream_ip);
    alert->set_upstream_ip(upstream_ip);

    // ---- 匹配规则列表 ----
    for (const auto& mr : result.matched_rules) {
        auto* matched_rule = alert->add_matched_rules();

        matched_rule->set_rule_id(mr.rule_id);
        matched_rule->set_rule_msg(mr.rule_msg);
        matched_rule->set_rule_severity(severityToString(mr.severity));
        matched_rule->set_rule_ver(mr.rule_ver);

        for (const auto& tag : mr.rule_tags) {
            matched_rule->add_rule_tags(tag);
        }

        matched_rule->set_matched_var_name(mr.matched_var_name);
        matched_rule->set_matched_var_value(mr.matched_var_value);
        matched_rule->set_matched_var_original(mr.matched_var_original);

        matched_rule->set_operator_name(mr.operator_name);
        matched_rule->set_operator_param(mr.operator_param);
    }

    // ---- Akto 集成字段 (透传 + 推断) ----
    alert->set_akto_account_id(akto_account_id);
    alert->set_akto_collection_id(akto_collection_id);
    alert->set_request_body(request_body);
    if (response_status_code > 0) {
        alert->set_response_status(std::to_string(response_status_code));
    }
    alert->set_request_host(request_host);

    // 从 matched_rules 推断 attack_type 和 severity
    // 优先使用 operator_name 映射，其次从 rule_tags 推断
    if (!result.matched_rules.empty()) {
        const auto& first_rule = result.matched_rules[0];
        std::string attack_type = inferAttackType(
            first_rule.operator_name, first_rule.rule_tags);
        alert->set_attack_type(attack_type);
        // severity: WGE 用 0-7 数字 (syslog 级别)，Akto 用 CRITICAL/HIGH/MEDIUM/LOW
        // 映射: 0-2 → CRITICAL, 3-4 → HIGH, 5 → MEDIUM, 6-7 → LOW
        alert->set_severity(mapSeverityToAkto(first_rule.severity));
    } else {
        alert->set_attack_type("SecurityMisconfig");
        alert->set_severity("MEDIUM");
    }

    alert->set_successful_exploit(false);  // 保守判定
    alert->set_context_source("API");
    alert->set_label("THREAT");

    SPDLOG_DEBUG("AlertBuilder: built alert_id={}, event_id={}, matched_rules={}, "
                 "attack_type={}, akto_account={}",
                 alert->alert_id(), event_id, result.matched_rules.size(),
                 alert->attack_type(), akto_account_id);

    return alert;
}

// ============================================================================
// inferAttackType — 从 operator_name / rule_tags 推断攻击类型
// ============================================================================

std::string AlertBuilder::inferAttackType(
    const std::string& operator_name,
    const std::vector<std::string>& rule_tags) {
    // 1. 从 operator_name 推断
    // WGE 操作符: @detectSQLi, @detectXSS, @rx, @pm, @contains 等
    if (operator_name.find("detectSQLi") != std::string::npos ||
        operator_name.find("detectSqli") != std::string::npos) {
        return "SQLInjection";
    }
    if (operator_name.find("detectXSS") != std::string::npos ||
        operator_name.find("detectXss") != std::string::npos) {
        return "XSS";
    }

    // 2. 从 rule_tags 推断 (OWASP CRS tag 格式: attack-sqli, attack-xss 等)
    for (const auto& tag : rule_tags) {
        // 转小写匹配
        std::string lower_tag = tag;
        for (auto& c : lower_tag) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (lower_tag.find("sqli") != std::string::npos ||
            lower_tag.find("sql-injection") != std::string::npos) {
            return "SQLInjection";
        }
        if (lower_tag.find("xss") != std::string::npos) {
            return "XSS";
        }
        if (lower_tag.find("lfi") != std::string::npos ||
            lower_tag.find("local-file-inclusion") != std::string::npos) {
            return "LocalFileInclusionLFIRFI";
        }
        if (lower_tag.find("rfi") != std::string::npos ||
            lower_tag.find("remote-file-inclusion") != std::string::npos) {
            return "LocalFileInclusionLFIRFI";
        }
        // 用前缀匹配避免 'rce' 误匹配 'force'
        if (lower_tag.find("rce") == 0 || lower_tag.find("-rce") != std::string::npos ||
            lower_tag.find("command-injection") != std::string::npos ||
            lower_tag.find("os-command") != std::string::npos) {
            return "OSCommandInjection";
        }
        if (lower_tag.find("ssrf") != std::string::npos) {
            return "SSRF";
        }
        if (lower_tag.find("xxe") != std::string::npos) {
            return "XXE";
        }
    }

    // 3. 从 operator_name 通用模式推断
    if (operator_name.find("rx") != std::string::npos) {
        // @rx 是通用正则匹配，无法确定具体类型
        return "SecurityMisconfig";
    }

    // 4. 兜底
    return "SecurityMisconfig";
}

// ============================================================================
// mapSeverityToAkto — WGE 数字严重级别 → Akto 字符串
// ============================================================================

std::string AlertBuilder::mapSeverityToAkto(int wge_severity) {
    // WGE (syslog RFC 5424): 0=EMERGENCY, 1=ALERT, 2=CRITICAL, 3=ERROR,
    //                        4=WARNING, 5=NOTICE, 6=INFO, 7=DEBUG
    // Akto: CRITICAL, HIGH, MEDIUM, LOW, INFO
    switch (wge_severity) {
        case 0:  // EMERGENCY
        case 1:  // ALERT
        case 2:  // CRITICAL
            return "CRITICAL";
        case 3:  // ERROR
        case 4:  // WARNING
            return "HIGH";
        case 5:  // NOTICE
            return "MEDIUM";
        case 6:  // INFO
        case 7:  // DEBUG
            return "LOW";
        default:
            return "MEDIUM";
    }
}

}  // namespace wge::kafka::detector
