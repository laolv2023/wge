/**
 * @file test_alert_builder.cc
 * @brief AlertBuilder 单元测试 — 验证告警事件构建的完整性和正确性
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "detector/alert_builder.h"
#include "detector/result.h"
#include "wge_alert.pb.h"

using namespace wge::kafka::detector;
using namespace wge::kafka;

// ============================================================================
// 辅助函数
// ============================================================================

namespace {

/// 构造一个基础 AlertResult，便于各测试复用
AlertResult makeBaseResult() {
    AlertResult r;
    r.event_id = "evt-001";
    r.timestamp_ms = 1734567890123L;
    r.intervened = false;
    r.disruptive_action = "";
    r.response_code = 0;
    r.redirect_url = "";
    return r;
}

/// 添加一条匹配规则
void addRule(AlertResult& result,
             uint64_t rule_id,
             const std::string& msg,
             int severity,
             const std::string& ver = "1.0",
             const std::vector<std::string>& tags = {},
             const std::string& var_name = "REQUEST_URI",
             const std::string& var_value = "/api/test",
             const std::string& var_original = "/api/test",
             const std::string& op_name = "@rx",
             const std::string& op_param = "select.+from") {
    MatchedRuleInfo m;
    m.rule_id = rule_id;
    m.rule_msg = msg;
    m.severity = severity;
    m.rule_ver = ver;
    m.rule_tags = tags;
    m.matched_var_name = var_name;
    m.matched_var_value = var_value;
    m.matched_var_original = var_original;
    m.operator_name = op_name;
    m.operator_param = op_param;
    result.matched_rules.push_back(std::move(m));
}

}  // namespace

// ============================================================================
// 1. BuildAlertWithIntervention — intervened=true + 阻断字段
// ============================================================================

TEST(AlertBuilderTest, BuildAlertWithIntervention) {
    AlertResult result = makeBaseResult();
    result.intervened = true;
    result.disruptive_action = "DENY";
    result.response_code = 403;
    result.redirect_url = "";

    auto alert = AlertBuilder::build(
        result,
        "evt-550e8400-e29b-41d4-a716-446655440000",
        "collector-edge-7",
        "POST",
        "/admin/exec?cmd=whoami",
        "192.168.1.100",
        "10.0.0.5");

    ASSERT_NE(alert, nullptr);

    // 阻断字段
    EXPECT_TRUE(alert->intervened());
    EXPECT_EQ(alert->disruptive_action(), "DENY");
    EXPECT_EQ(alert->response_code(), 403);
    EXPECT_TRUE(alert->redirect_url().empty());

    // 关联字段
    EXPECT_EQ(alert->event_id(), "evt-550e8400-e29b-41d4-a716-446655440000");
    EXPECT_EQ(alert->collector_id(), "collector-edge-7");

    // 请求摘要
    EXPECT_EQ(alert->request_method(), "POST");
    EXPECT_EQ(alert->request_uri(), "/admin/exec?cmd=whoami");
    EXPECT_EQ(alert->downstream_ip(), "192.168.1.100");
    EXPECT_EQ(alert->upstream_ip(), "10.0.0.5");

    // 时间戳
    EXPECT_EQ(alert->timestamp_ms(), 1734567890123L);

    // alert_id 格式
    EXPECT_EQ(alert->alert_id().size(), 36u);
    EXPECT_NE(alert->alert_id().find('-'), std::string::npos);

    // 无匹配规则
    EXPECT_EQ(alert->matched_rules_size(), 0);
}

// ============================================================================
// 2. BuildAlertWithoutIntervention — intervened=false, 无 matched_rules
// ============================================================================

TEST(AlertBuilderTest, BuildAlertWithoutIntervention) {
    AlertResult result = makeBaseResult();
    result.intervened = false;

    auto alert = AlertBuilder::build(
        result,
        "evt-002",
        "collector-01",
        "GET",
        "/index.html",
        "10.0.0.1",
        "10.0.0.2");

    ASSERT_NE(alert, nullptr);

    EXPECT_FALSE(alert->intervened());
    EXPECT_TRUE(alert->disruptive_action().empty());
    EXPECT_EQ(alert->response_code(), 0);
    EXPECT_EQ(alert->matched_rules_size(), 0);
    EXPECT_EQ(alert->request_method(), "GET");
    EXPECT_EQ(alert->request_uri(), "/index.html");
}

// ============================================================================
// 3. BuildAlertWithMatchedRules — 多条匹配规则
// ============================================================================

TEST(AlertBuilderTest, BuildAlertWithMatchedRules) {
    AlertResult result = makeBaseResult();
    result.intervened = true;
    result.disruptive_action = "DENY";
    result.response_code = 403;

    addRule(result, 1001, "SQL Injection detected", 2, "2024.1",
            {"OWASP_CRS", "SQL_INJECTION"},
            "REQUEST_URI", "/login?user=1' OR '1'='1",
            "/login?user=1' OR '1'='1",
            "@detectSQLi", "");

    addRule(result, 1002, "XSS Cross-site scripting", 3, "2024.1",
            {"OWASP_CRS", "XSS"},
            "ARGS:q", "<script>alert(1)</script>",
            "<script>alert(1)</script>",
            "@detectXSS", "");

    addRule(result, 2001, "Path traversal attempt", 4, "2024.1",
            {"OWASP_CRS", "TRAVERSAL"},
            "REQUEST_URI", "/../../../etc/passwd",
            "/../../../etc/passwd",
            "@rx", "\\.\\./");

    auto alert = AlertBuilder::build(
        result,
        "evt-003",
        "collector-02",
        "GET",
        "/login?user=1' OR '1'='1",
        "10.1.1.1",
        "10.2.2.2");

    ASSERT_NE(alert, nullptr);
    EXPECT_EQ(alert->matched_rules_size(), 3);

    // 规则 1
    {
        const auto& mr = alert->matched_rules(0);
        EXPECT_EQ(mr.rule_id(), 1001u);
        EXPECT_EQ(mr.rule_msg(), "SQL Injection detected");
        EXPECT_EQ(mr.rule_severity(), "CRITICAL");
        EXPECT_EQ(mr.rule_ver(), "2024.1");
        EXPECT_EQ(mr.rule_tags_size(), 2);
        EXPECT_EQ(mr.rule_tags(0), "OWASP_CRS");
        EXPECT_EQ(mr.rule_tags(1), "SQL_INJECTION");
        EXPECT_EQ(mr.matched_var_name(), "REQUEST_URI");
        EXPECT_EQ(mr.operator_name(), "@detectSQLi");
    }

    // 规则 2
    {
        const auto& mr = alert->matched_rules(1);
        EXPECT_EQ(mr.rule_id(), 1002u);
        EXPECT_EQ(mr.rule_msg(), "XSS Cross-site scripting");
        EXPECT_EQ(mr.rule_severity(), "ERROR");
        EXPECT_EQ(mr.operator_name(), "@detectXSS");
    }

    // 规则 3
    {
        const auto& mr = alert->matched_rules(2);
        EXPECT_EQ(mr.rule_id(), 2001u);
        EXPECT_EQ(mr.rule_msg(), "Path traversal attempt");
        EXPECT_EQ(mr.rule_severity(), "WARNING");
        EXPECT_EQ(mr.operator_name(), "@rx");
        EXPECT_EQ(mr.operator_param(), "\\.\\./");
    }
}

// ============================================================================
// 4. BuildAlertUuidV7 — 验证 alert_id 格式为 UUID (36 字符, 含连字符)
// ============================================================================

TEST(AlertBuilderTest, BuildAlertUuidV7) {
    AlertResult result = makeBaseResult();

    // 生成多个 alert，验证每个都有合法 UUID 格式
    std::vector<std::string> alert_ids;
    alert_ids.reserve(10);

    for (int i = 0; i < 10; ++i) {
        auto alert = AlertBuilder::build(
            result,
            "evt-" + std::to_string(i),
            "collector-01",
            "GET", "/", "127.0.0.1", "127.0.0.1");
        ASSERT_NE(alert, nullptr);
        alert_ids.push_back(alert->alert_id());
    }

    for (const auto& id : alert_ids) {
        // UUID 标准长度: 36 字符
        EXPECT_EQ(id.size(), 36u) << "alert_id=" << id;

        // 连字符位置: 8-4-4-4-12
        EXPECT_EQ(id[8], '-');
        EXPECT_EQ(id[13], '-');
        EXPECT_EQ(id[18], '-');
        EXPECT_EQ(id[23], '-');

        // UUID v7: 第 13 位为版本号 '7'
        EXPECT_EQ(id[14], '7') << "Expected UUID v7 version nibble: " << id;

        // 第 18 位之后的 variant: 应为 8/9/a/b
        char variant = id[19];
        EXPECT_TRUE(variant == '8' || variant == '9' ||
                    variant == 'a' || variant == 'b' ||
                    variant == 'A' || variant == 'B')
            << "Variant char: " << variant << " in " << id;

        // 所有字符应为 hex 或 '-'
        for (char c : id) {
            EXPECT_TRUE((c >= '0' && c <= '9') ||
                        (c >= 'a' && c <= 'f') ||
                        (c >= 'A' && c <= 'F') ||
                        c == '-')
                << "Invalid char '" << c << "' in " << id;
        }
    }

    // 验证唯一性（10 个 UUID 应全部不同）
    std::sort(alert_ids.begin(), alert_ids.end());
    auto dup = std::adjacent_find(alert_ids.begin(), alert_ids.end());
    EXPECT_EQ(dup, alert_ids.end())
        << "Duplicate UUID found: " << *dup;
}

// ============================================================================
// 5. BuildAlertSeverityMapping — 验证 severity 数字 → 字符串映射
// ============================================================================

TEST(AlertBuilderTest, BuildAlertSeverityMapping) {
    // 验证 severityToString 的完整映射
    EXPECT_STREQ(severityToString(0), "EMERGENCY");
    EXPECT_STREQ(severityToString(1), "ALERT");
    EXPECT_STREQ(severityToString(2), "CRITICAL");
    EXPECT_STREQ(severityToString(3), "ERROR");
    EXPECT_STREQ(severityToString(4), "WARNING");
    EXPECT_STREQ(severityToString(5), "NOTICE");
    EXPECT_STREQ(severityToString(6), "INFO");
    EXPECT_STREQ(severityToString(7), "DEBUG");
    EXPECT_STREQ(severityToString(-1), "UNKNOWN");
    EXPECT_STREQ(severityToString(8), "UNKNOWN");
    EXPECT_STREQ(severityToString(999), "UNKNOWN");

    // 通过 build() 端到端验证 severity 字符串出现在 matched_rule 中
    AlertResult result = makeBaseResult();

    // 每个 severity 一条规则
    struct { uint64_t id; int sev; std::string msg; } rules[] = {
        {1, 0, "Emergency rule"},
        {2, 2, "Critical rule"},
        {3, 3, "Error rule"},
        {4, 4, "Warning rule"},
        {5, 5, "Notice rule"},
    };

    for (const auto& r : rules) {
        addRule(result, r.id, r.msg, r.sev);
    }

    auto alert = AlertBuilder::build(
        result, "evt-severity", "c1", "GET", "/", "1.1.1.1", "2.2.2.2");

    ASSERT_NE(alert, nullptr);
    ASSERT_EQ(alert->matched_rules_size(), 5);

    EXPECT_EQ(alert->matched_rules(0).rule_severity(), "EMERGENCY");
    EXPECT_EQ(alert->matched_rules(1).rule_severity(), "CRITICAL");
    EXPECT_EQ(alert->matched_rules(2).rule_severity(), "ERROR");
    EXPECT_EQ(alert->matched_rules(3).rule_severity(), "WARNING");
    EXPECT_EQ(alert->matched_rules(4).rule_severity(), "NOTICE");
}

// ============================================================================
// 6. BuildAlertEmptyMatchedVar — matched_var 字段为空时正确处理
// ============================================================================

TEST(AlertBuilderTest, BuildAlertEmptyMatchedVar) {
    AlertResult result = makeBaseResult();

    // 添加一条所有 matched_var 字段都为空的规则
    MatchedRuleInfo m;
    m.rule_id = 9999;
    m.rule_msg = "Empty var rule";
    m.severity = 3;
    m.rule_ver = "1.0";
    // 明确设置为空
    m.matched_var_name = "";
    m.matched_var_value = "";
    m.matched_var_original = "";
    m.operator_name = "";
    m.operator_param = "";
    result.matched_rules.push_back(std::move(m));

    auto alert = AlertBuilder::build(
        result, "evt-empty-var", "c1", "GET", "/", "1.1.1.1", "2.2.2.2");

    ASSERT_NE(alert, nullptr);
    ASSERT_EQ(alert->matched_rules_size(), 1);

    const auto& mr = alert->matched_rules(0);
    EXPECT_TRUE(mr.matched_var_name().empty());
    EXPECT_TRUE(mr.matched_var_value().empty());
    EXPECT_TRUE(mr.matched_var_original().empty());
    EXPECT_TRUE(mr.operator_name().empty());
    EXPECT_TRUE(mr.operator_param().empty());
    EXPECT_EQ(mr.rule_id(), 9999u);
    EXPECT_EQ(mr.rule_msg(), "Empty var rule");
    EXPECT_EQ(mr.rule_severity(), "ERROR");
}

// ============================================================================
// 7. BuildAlertResponseCodeDefault — response_code 默认值
// ============================================================================

TEST(AlertBuilderTest, BuildAlertResponseCodeDefault) {
    AlertResult result = makeBaseResult();
    // response_code 保持默认值 0
    result.response_code = 0;

    auto alert = AlertBuilder::build(
        result, "evt-rcd", "c1", "GET", "/", "1.1.1.1", "2.2.2.2");

    ASSERT_NE(alert, nullptr);
    EXPECT_EQ(alert->response_code(), 0);

    // 验证非零 response_code 也能正确传递
    AlertResult r403 = makeBaseResult();
    r403.response_code = 403;
    auto alert403 = AlertBuilder::build(
        r403, "evt-403", "c1", "GET", "/", "1.1.1.1", "2.2.2.2");
    EXPECT_EQ(alert403->response_code(), 403);

    AlertResult r302 = makeBaseResult();
    r302.response_code = 302;
    r302.redirect_url = "/login";
    auto alert302 = AlertBuilder::build(
        r302, "evt-302", "c1", "GET", "/", "1.1.1.1", "2.2.2.2");
    EXPECT_EQ(alert302->response_code(), 302);
    EXPECT_EQ(alert302->redirect_url(), "/login");
}

// ============================================================================
// 8. BuildAlertTimestampFallback — timestamp_ms 为 0 时使用当前时间
// ============================================================================

TEST(AlertBuilderTest, BuildAlertTimestampFallback) {
    AlertResult result = makeBaseResult();
    result.timestamp_ms = 0;  // 使用 fallback (当前时间)

    auto before = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    auto alert = AlertBuilder::build(
        result, "evt-ts-fb", "c1", "GET", "/", "1.1.1.1", "2.2.2.2");

    auto after = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    ASSERT_NE(alert, nullptr);
    // timestamp_ms 应在 [before, after] 范围内
    EXPECT_GE(alert->timestamp_ms(), before);
    EXPECT_LE(alert->timestamp_ms(), after);
}

// ============================================================================
// 9. BuildAlertRuleTagsEmpty — rule_tags 为空列表时正确处理
// ============================================================================

TEST(AlertBuilderTest, BuildAlertRuleTagsEmpty) {
    AlertResult result = makeBaseResult();
    addRule(result, 5000, "No tags rule", 4, "2.0",
            {},  // 空的 tags
            "ARGS", "test", "test", "@rx", ".*");

    auto alert = AlertBuilder::build(
        result, "evt-no-tags", "c1", "POST", "/api", "1.1.1.1", "2.2.2.2");

    ASSERT_NE(alert, nullptr);
    ASSERT_EQ(alert->matched_rules_size(), 1);
    EXPECT_EQ(alert->matched_rules(0).rule_tags_size(), 0);
}

// ============================================================================
// 10. BuildAlertMultipleRuleTags — 多 tag 正确复制
// ============================================================================

TEST(AlertBuilderTest, BuildAlertMultipleRuleTags) {
    AlertResult result = makeBaseResult();
    addRule(result, 6000, "Multi-tag rule", 2, "3.0",
            {"TAG_A", "TAG_B", "TAG_C", "TAG_D"},
            "REQUEST_HEADERS:User-Agent",
            "curl/7.0", "curl/7.0", "@rx", "curl");

    auto alert = AlertBuilder::build(
        result, "evt-tags", "c1", "GET", "/", "1.1.1.1", "2.2.2.2");

    ASSERT_NE(alert, nullptr);
    ASSERT_EQ(alert->matched_rules_size(), 1);
    const auto& mr = alert->matched_rules(0);
    ASSERT_EQ(mr.rule_tags_size(), 4);
    EXPECT_EQ(mr.rule_tags(0), "TAG_A");
    EXPECT_EQ(mr.rule_tags(1), "TAG_B");
    EXPECT_EQ(mr.rule_tags(2), "TAG_C");
    EXPECT_EQ(mr.rule_tags(3), "TAG_D");
}

// ============================================================================
// 11. BuildAlertEmptyEventIdThrows — event_id 为空抛异常
// ============================================================================

TEST(AlertBuilderTest, BuildAlertEmptyEventIdThrows) {
    AlertResult result = makeBaseResult();

    EXPECT_THROW(
        {
            AlertBuilder::build(result, "", "c1", "GET", "/",
                                "1.1.1.1", "2.2.2.2");
        },
        std::runtime_error);
}
