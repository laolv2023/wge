/**
 * @file alert_builder.cc
 * @brief AlertBuilder 实现
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
    // UUID v7: Unix epoch ms (48 bits) + version (4 bits) +
    //           random a (12 bits) + variant (2 bits) + random b (62 bits)

    using clock = std::chrono::system_clock;
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      clock::now().time_since_epoch())
                      .count();

    // 线程安全的随机数生成
    thread_local std::random_device rd;
    thread_local std::mt19937_64 gen(rd());
    thread_local std::uniform_int_distribution<uint64_t> dist;

    uint64_t rand_a = dist(gen) & 0xFFF;        // 12 bits
    uint64_t rand_b_hi = dist(gen) & 0x3FFFFFFFFFFFFFFFULL;  // 62 bits (high)
    uint64_t rand_b_lo = dist(gen);             // 64 bits (low)

    // Format: 00000000-0000-7xxx-yxxx-xxxxxxxxxxxx
    // timestamp_ms (48 bits) 填满前 12 hex digits
    uint64_t ts = static_cast<uint64_t>(now_ms) & 0xFFFFFFFFFFFFULL;

    std::ostringstream oss;
    oss << std::hex << std::setfill('0');

    // time_low (8 hex)
    oss << std::setw(8) << (ts >> 16);
    oss << '-';
    // time_mid (4 hex)
    oss << std::setw(4) << (ts & 0xFFFF);
    oss << '-';
    // version (7) + time_high (3 hex)
    oss << '7' << std::setw(3) << ((rand_a >> 8) & 0xFFF);
    oss << '-';
    // variant (10xx) + rand_a[7:0]
    uint16_t variant_byte = 0x80 | ((rand_a >> 4) & 0x3F);
    oss << std::setw(2) << variant_byte;
    oss << std::setw(2) << (rand_a & 0xFF);
    oss << '-';
    // rand_b (12 hex)
    oss << std::setw(12) << (rand_b_lo & 0xFFFFFFFFFFFFULL);

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
    const std::string& upstream_ip) {

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

    SPDLOG_DEBUG("AlertBuilder: built alert_id={}, event_id={}, matched_rules={}",
                 alert->alert_id(), event_id, result.matched_rules.size());

    return alert;
}

}  // namespace wge::kafka::detector
