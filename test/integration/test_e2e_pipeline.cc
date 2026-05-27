/**
 * @file test_e2e_pipeline.cc
 * @brief 端到端数据管道集成测试
 *
 * 测试从原始日志 → mapper → detect 逻辑 → alert 的完整管道。
 * 由于沙盒中没有真正的 WGE/Kafka，使用 mock/fake 测试核心逻辑。
 *
 * 所有测试自包含，不依赖外部服务。
 *
 * 测试覆盖:
 *   1. NginxAccessLogToAlert          — Nginx combined log → RegexMapper → HttpAccessEvent
 *   2. JsonApiLogToAlert              — JSON API log → JsonMapper → 验证嵌套字段和headers
 *   3. ProtobufDirectPassthrough      — Protobuf 格式直接反序列化
 *   4. MalformedInputToDLQ            — 非法输入正确路由到DLQ
 *   5. MissingRequiredFields          — 必填字段缺失时Pipeline返回错误
 *   6. LargePayloadHandling           — 大body(接近limit)的payload处理
 *   7. BatchProcessing                — 批量100条日志一次性处理
 *   8. TimestampParsingFormats        — 5种时间戳格式全部正确解析
 *   9. Base64BodyDecoding             — base64编码的body正确解码
 *  10. HeaderExtractionEmbedded       — embedded模式header提取
 *  11. HeaderExtractionPrefix         — prefix模式header提取
 */

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "detector/alert_builder.h"
#include "detector/result.h"
#include "http_access.pb.h"
#include "mapper/field_applier.h"
#include "mapper/json_mapper.h"
#include "mapper/mapper.h"
#include "mapper/mapper_config.h"
#include "mapper/regex_mapper.h"
#include "wge_alert.pb.h"

using namespace wge::kafka::mapper;
using namespace wge::kafka::detector;
using namespace wge::kafka;

// ============================================================================
// 辅助函数与fixture
// ============================================================================

namespace {

/// @brief 便捷构造 FieldMapping
FieldMapping makeField(const std::string& source, const std::string& target,
                       FieldType type = FieldType::String,
                       bool required = false,
                       std::optional<std::string> default_value = {}) {
  FieldMapping fm;
  fm.source = source;
  fm.target = target;
  fm.type = type;
  fm.required = required;
  fm.default_value = std::move(default_value);
  return fm;
}

/// @brief 构造 JSON 格式 MapperConfig
MapperConfig makeJsonConfig(
    std::vector<FieldMapping> mappings = {},
    HeaderExtractionConfig req_headers = {},
    HeaderExtractionConfig resp_headers = {},
    std::vector<ConstantField> constants = {}) {
  MapperConfig cfg;
  cfg.format = Format::Json;
  cfg.field_mappings = std::move(mappings);
  cfg.request_headers = std::move(req_headers);
  cfg.response_headers = std::move(resp_headers);
  cfg.constant_fields = std::move(constants);
  return cfg;
}

/// @brief 构造 Regex 格式 MapperConfig
MapperConfig makeRegexConfig(
    const std::string& pattern,
    std::vector<FieldMapping> mappings,
    TimestampConfig ts_cfg = {}) {
  MapperConfig cfg;
  cfg.format = Format::Regex;
  cfg.regex_pattern = pattern;
  cfg.field_mappings = std::move(mappings);
  cfg.timestamp_config = std::move(ts_cfg);
  return cfg;
}

/// @brief 构造 Protobuf 格式 MapperConfig
MapperConfig makeProtobufConfig(
    std::vector<ConstantField> constants = {}) {
  MapperConfig cfg;
  cfg.format = Format::Protobuf;
  cfg.constant_fields = std::move(constants);
  return cfg;
}

}  // namespace

// ============================================================================
// 1. NginxAccessLogToAlert — Nginx combined log → RegexMapper → HttpAccessEvent
// ============================================================================

/**
 * @brief 模拟Nginx combined log格式输入，经过 RegexMapper 映射为 HttpAccessEvent，
 *        验证所有字段提取正确。
 *
 * Nginx combined log 格式:
 *   $remote_addr - $remote_user [$time_local] "$request" $status
 *   $body_bytes_sent "$http_referer" "$http_user_agent"
 */
TEST(E2ePipelineTest, NginxAccessLogToAlert) {
  // 标准 Nginx combined log 行
  const std::string log_line =
      R"(192.168.1.10 - john [10/Oct/2023:13:55:36 +0000] "GET /api/users HTTP/1.1" 200 2326 "https://example.com/" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")";

  // 正则表达式: 命名捕获组映射 Nginx 字段
  const std::string pattern =
      R"((?P<remote_addr>\S+) - (?P<remote_user>\S+) \[(?P<timestamp>[^\]]+)\] "(?P<request_method>\S+) (?P<request_uri>\S+) (?P<request_version>\S+)" (?P<response_status>\d+) (?P<body_bytes_sent>\d+) "(?P<http_referer>[^"]*)" "(?P<http_user_agent>[^"]*)")";

  auto config = makeRegexConfig(
      pattern,
      {
          makeField("remote_addr", "downstream_ip"),
          makeField("request_method", "request_method"),
          makeField("request_uri", "request_uri"),
          makeField("request_version", "request_version"),
          makeField("response_status", "response_status"),
      });

  // 配置时间戳解析
  config.timestamp_config.source_field = "timestamp";
  config.timestamp_config.formats = {};  // 使用默认格式链

  LogMapper mapper(config);
  auto result = mapper.map(log_line);

  ASSERT_TRUE(result.has_value()) << "map() failed: " << result.error();
  auto event = *result;
  ASSERT_NE(event, nullptr);

  // 验证所有字段
  EXPECT_EQ(event->downstream_ip(), "192.168.1.10");
  EXPECT_EQ(event->request_method(), "GET");
  EXPECT_EQ(event->request_uri(), "/api/users");
  EXPECT_EQ(event->request_version(), "HTTP/1.1");
  EXPECT_EQ(event->response_status(), 200);

  // 验证时间戳已解析 (Nginx 格式: 10/Oct/2023:13:55:36 +0000)
  EXPECT_GT(event->timestamp_ms(), 0)
      << "Timestamp should be parsed from Nginx format";
}

// ============================================================================
// 2. JsonApiLogToAlert — JSON API log → JsonMapper → 验证嵌套字段和headers
// ============================================================================

/**
 * @brief 模拟JSON格式API日志输入，经过 JsonMapper 映射为 HttpAccessEvent，
 *        验证嵌套字段 (request.method, downstream.ip 等) 和 headers 提取。
 */
TEST(E2ePipelineTest, JsonApiLogToAlert) {
  const std::string json = R"({
    "request": {
      "method": "POST",
      "uri": "/api/v1/login?redirect=/home",
      "version": "1.1",
      "headers": {
        "Host": "www.example.com",
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/json",
        "Accept": "*/*"
      }
    },
    "downstream": {
      "ip": "192.168.1.100",
      "port": 54321
    },
    "upstream": {
      "ip": "10.0.0.5",
      "port": 8080
    },
    "response": {
      "status": 200,
      "version": "1.1"
    },
    "event_id": "550e8400-e29b-41d4-a716-446655440000",
    "collector_id": "edge-node-7",
    "timestamp_ms": "1734567890123"
  })";

  // 配置 embedded 模式 header 提取
  HeaderExtractionConfig req_headers;
  req_headers.strategy = HeaderStrategy::Embedded;
  req_headers.embedded_path = "request.headers";
  req_headers.is_request = true;

  auto config = makeJsonConfig(
      {
          makeField("request.method", "request_method"),
          makeField("request.uri", "request_uri"),
          makeField("request.version", "request_version"),
          makeField("downstream.ip", "downstream_ip"),
          makeField("upstream.ip", "upstream_ip"),
          makeField("response.status", "response_status", FieldType::Int32),
          makeField("response.version", "response_version"),
          makeField("event_id", "event_id"),
          makeField("collector_id", "collector_id"),
          makeField("timestamp_ms", "timestamp_ms", FieldType::Int64),
      },
      req_headers);

  LogMapper mapper(config);
  auto result = mapper.map(json);

  ASSERT_TRUE(result.has_value()) << "map() failed: " << result.error();
  auto event = *result;
  ASSERT_NE(event, nullptr);

  // 验证嵌套字段
  EXPECT_EQ(event->request_method(), "POST");
  EXPECT_EQ(event->request_uri(), "/api/v1/login?redirect=/home");
  EXPECT_EQ(event->request_version(), "1.1");
  EXPECT_EQ(event->downstream_ip(), "192.168.1.100");
  EXPECT_EQ(event->upstream_ip(), "10.0.0.5");
  EXPECT_EQ(event->response_status(), 200);
  EXPECT_EQ(event->response_version(), "1.1");
  EXPECT_EQ(event->event_id(), "550e8400-e29b-41d4-a716-446655440000");
  EXPECT_EQ(event->collector_id(), "edge-node-7");
  EXPECT_EQ(event->timestamp_ms(), 1734567890123L);

  // 验证 embedded headers 已提取
  ASSERT_EQ(event->request_headers_size(), 4);
  // 检查存在性 (顺序可能不固定)
  std::map<std::string, std::string> header_map;
  for (const auto& h : event->request_headers()) {
    header_map[h.key()] = h.value();
  }
  EXPECT_EQ(header_map["Host"], "www.example.com");
  EXPECT_EQ(header_map["User-Agent"], "Mozilla/5.0");
  EXPECT_EQ(header_map["Content-Type"], "application/json");
  EXPECT_EQ(header_map["Accept"], "*/*");
}

// ============================================================================
// 3. ProtobufDirectPassthrough — Protobuf 格式直接反序列化
// ============================================================================

/**
 * @brief Protobuf 格式输入直接反序列化为 HttpAccessEvent。
 *        验证 Protobuf 二进制 → 直接 ParseFromArray 的工作路径。
 */
TEST(E2ePipelineTest, ProtobufDirectPassthrough) {
  // 构造一个 HttpAccessEvent protobuf 消息
  HttpAccessEvent original;
  original.set_event_id("evt-direct-proto-001");
  original.set_collector_id("collector-proto");
  original.set_timestamp_ms(1700000000000L);
  original.set_downstream_ip("10.10.10.10");
  original.set_downstream_port(12345);
  original.set_upstream_ip("172.16.0.1");
  original.set_upstream_port(8080);
  original.set_request_method("DELETE");
  original.set_request_uri("/admin/users/42");
  original.set_request_version("1.1");
  original.set_response_status(204);

  // 序列化为二进制
  std::string binary;
  ASSERT_TRUE(original.SerializeToString(&binary));
  ASSERT_FALSE(binary.empty());

  // 通过 LogMapper 反序列化
  auto config = makeProtobufConfig();
  LogMapper mapper(config);
  auto result = mapper.map(binary);

  ASSERT_TRUE(result.has_value()) << "map() failed: " << result.error();
  auto event = *result;
  ASSERT_NE(event, nullptr);

  // 验证所有字段
  EXPECT_EQ(event->event_id(), "evt-direct-proto-001");
  EXPECT_EQ(event->collector_id(), "collector-proto");
  EXPECT_EQ(event->timestamp_ms(), 1700000000000L);
  EXPECT_EQ(event->downstream_ip(), "10.10.10.10");
  EXPECT_EQ(event->downstream_port(), 12345);
  EXPECT_EQ(event->upstream_ip(), "172.16.0.1");
  EXPECT_EQ(event->upstream_port(), 8080);
  EXPECT_EQ(event->request_method(), "DELETE");
  EXPECT_EQ(event->request_uri(), "/admin/users/42");
  EXPECT_EQ(event->request_version(), "1.1");
  EXPECT_EQ(event->response_status(), 204);
}

// ============================================================================
// 4. MalformedInputToDLQ — 非法输入正确路由到DLQ
// ============================================================================

/**
 * @brief 非法输入应返回错误（模拟路由到DLQ）。
 *        测试三种非法输入: 空payload、无效JSON、不匹配的regex。
 */
TEST(E2ePipelineTest, MalformedInputToDLQ) {
  // 4a. 空 payload
  {
    auto config = makeJsonConfig({makeField("method", "request_method")});
    LogMapper mapper(config);
    auto result = mapper.map("");
    EXPECT_FALSE(result.has_value())
        << "Empty payload should fail, simulating DLQ routing";
    EXPECT_NE(result.error().find("Empty"), std::string::npos);
  }

  // 4b. 无效 JSON
  {
    auto config = makeJsonConfig({makeField("method", "request_method")});
    LogMapper mapper(config);
    auto result = mapper.map("this is not valid json {{{");
    EXPECT_FALSE(result.has_value())
        << "Invalid JSON should fail, simulating DLQ routing";
  }

  // 4c. 不匹配正则表达式的输入
  {
    const std::string pattern =
        R"((?P<ip>\S+) - - \[(?P<ts>[^\]]+)\] "(?P<method>\S+).*")";
    auto config = makeRegexConfig(
        pattern,
        {makeField("ip", "downstream_ip"), makeField("method", "request_method")});
    LogMapper mapper(config);
    auto result = mapper.map("completely invalid format line");
    EXPECT_FALSE(result.has_value())
        << "Non-matching regex should fail, simulating DLQ routing";
    EXPECT_NE(result.error().find("match"), std::string::npos);
  }
}

// ============================================================================
// 5. MissingRequiredFields — 必填字段缺失时Pipeline返回错误
// ============================================================================

/**
 * @brief 当必填字段在输入中缺失时，Pipeline 应返回错误。
 *        验证 required=true 的 FieldMapping 行为。
 */
TEST(E2ePipelineTest, MissingRequiredFields) {
  // JSON 中缺少 "request.method"
  const std::string json = R"({
    "request": {
      "uri": "/api/health"
    }
  })";

  auto config = makeJsonConfig({
      makeField("request.method", "request_method", FieldType::String,
                /*required=*/true),  // 必填
      makeField("request.uri", "request_uri", FieldType::String,
                /*required=*/false),
  });

  LogMapper mapper(config);
  auto result = mapper.map(json);

  // request.method 缺失且 required=true → 应失败
  EXPECT_FALSE(result.has_value())
      << "Missing required field should cause pipeline error";
  if (!result.has_value()) {
    // 错误信息应包含字段名或路径
    bool has_field_info =
        result.error().find("method") != std::string::npos ||
        result.error().find("Field") != std::string::npos ||
        result.error().find("required") != std::string::npos;
    EXPECT_TRUE(has_field_info)
        << "Error should mention the missing required field: " << result.error();
  }

  // 对比: 提供必填字段时应成功
  const std::string json_ok = R"({
    "request": {
      "method": "GET",
      "uri": "/api/health"
    }
  })";
  auto result_ok = mapper.map(json_ok);
  EXPECT_TRUE(result_ok.has_value())
      << "With required field present, mapping should succeed";
}

// ============================================================================
// 6. LargePayloadHandling — 大body(接近limit)的payload处理
// ============================================================================

/**
 * @brief 测试大 payload（接近 ~10MB protobuf limit）的处理。
 *        构造一个大的 JSON body 字段，验证映射器不会崩溃或OOM。
 */
TEST(E2ePipelineTest, LargePayloadHandling) {
  // 构造约 1MB 的 JSON payload（远小于 10MB limit，但足够验证大body路径）
  constexpr size_t kBodySize = 1024 * 1024;  // 1 MB
  std::string large_body(kBodySize, 'X');
  // 使内容有效但大
  for (size_t i = 0; i < large_body.size(); ++i) {
    large_body[i] = static_cast<char>('A' + (i % 26));
  }

  std::ostringstream oss;
  oss << R"({
    "request": {
      "method": "POST",
      "uri": "/upload",
      "body": ")" << large_body << R"("
    },
    "event_id": "evt-large-001"
  })";
  std::string json = oss.str();

  auto config = makeJsonConfig({
      makeField("request.method", "request_method"),
      makeField("request.uri", "request_uri"),
      makeField("event_id", "event_id"),
  });

  LogMapper mapper(config);
  auto result = mapper.map(json);

  ASSERT_TRUE(result.has_value())
      << "Large payload (1MB) should be handled: " << result.error();
  auto event = *result;

  EXPECT_EQ(event->request_method(), "POST");
  EXPECT_EQ(event->request_uri(), "/upload");
  EXPECT_EQ(event->event_id(), "evt-large-001");
}

// ============================================================================
// 7. BatchProcessing — 批量100条日志一次性处理，验证全部正确
// ============================================================================

/**
 * @brief 批量生成100条日志，逐条通过 RegexMapper 映射，
 *        验证全部100条正确提取字段。
 */
TEST(E2ePipelineTest, BatchProcessing) {
  const std::string pattern =
      R"((?P<ip>\S+) - - \[(?P<ts>[^\]]+)\] "(?P<method>\S+) (?P<uri>\S+) [^"]*" (?P<status>\d+).*)";

  auto config = makeRegexConfig(
      pattern,
      {
          makeField("ip", "downstream_ip"),
          makeField("method", "request_method"),
          makeField("uri", "request_uri"),
          makeField("status", "response_status", FieldType::Int32),
      });

  LogMapper mapper(config);

  int success_count = 0;
  int fail_count = 0;

  for (int i = 0; i < 100; ++i) {
    // 生成 Nginx 风格的日志行，IP 动态变化
    int octet = i + 1;
    std::ostringstream log_oss;
    log_oss << "10.0." << (octet / 256) << "." << (octet % 256)
            << " - - [10/Oct/2023:13:55:36 +0000] \"GET /api/item/"
            << i << " HTTP/1.1\" " << (200 + (i % 10)) << " 1234 \"-\" \"curl/7.0\"";
    std::string log_line = log_oss.str();

    auto result = mapper.map(log_line);
    if (result.has_value()) {
      auto event = *result;
      // 验证关键字段
      EXPECT_EQ(event->request_method(), "GET");
      EXPECT_EQ(event->request_uri(), "/api/item/" + std::to_string(i));
      ++success_count;
    } else {
      ++fail_count;
    }
  }

  EXPECT_EQ(success_count, 100) << "All 100 entries should succeed";
  EXPECT_EQ(fail_count, 0) << "No entries should fail";
}

// ============================================================================
// 8. TimestampParsingFormats — 5种时间戳格式全部正确解析
// ============================================================================

/**
 * @brief 验证 RegexMapper/FieldApplier 对5种时间戳格式的正确解析:
 *   1. ISO 8601:  "2023-10-10T13:55:36Z"
 *   2. Nginx:     "10/Oct/2023:13:55:36 +0000"
 *   3. Simple:    "2023-10-10 13:55:36"
 *   4. Unix (s):  "1696946136"
 *   5. Unix (ms): "1696946136000"
 */
TEST(E2ePipelineTest, TimestampParsingFormats) {
  FieldApplier applier;

  // 所有格式应解析为同一时刻: 2023-10-10T13:55:36 UTC
  // Unix timestamp: 1696946136 seconds → 1696946136000 ms

  struct TestCase {
    std::string raw_ts;
    int64_t expected_ms;
    std::string description;
  };

  std::vector<TestCase> cases = {
      // ISO 8601 with Z
      {"2023-10-10T13:55:36Z", 1696946136000L, "ISO 8601 with Z"},
      // ISO 8601 with timezone offset
      {"2023-10-10T13:55:36+00:00", 1696946136000L, "ISO 8601 with +00:00"},
      // Nginx timestamp
      {"10/Oct/2023:13:55:36 +0000", 1696946136000L, "Nginx format"},
      // Simple datetime
      {"2023-10-10 13:55:36", 1696946136000L, "Simple format"},
      // Unix epoch seconds (numeric string)
      {"1696946136", 1696946136000L, "Unix epoch seconds"},
      // Unix epoch milliseconds (numeric string)
      {"1696946136000", 1696946136000L, "Unix epoch milliseconds"},
  };

  for (const auto& tc : cases) {
    int64_t result = applier.parseTimestamp(tc.raw_ts, {});
    EXPECT_NE(result, -1)
        << "Failed to parse: " << tc.description << " input='" << tc.raw_ts << "'";
    if (result != -1) {
      EXPECT_EQ(result, tc.expected_ms)
          << "Mismatch for: " << tc.description << " input='" << tc.raw_ts << "'";
    }
  }
}

// ============================================================================
// 9. Base64BodyDecoding — base64编码的body正确解码
// ============================================================================

/**
 * @brief 验证 FieldApplier 对 base64 编码的 request_body 的正确解码。
 *        "SGVsbG8gV29ybGQ=" → "Hello World"
 */
TEST(E2ePipelineTest, Base64BodyDecoding) {
  FieldApplier applier;

  // "Hello World" in base64
  std::string decoded = applier.decodeBytes("SGVsbG8gV29ybGQ=", "base64");
  EXPECT_EQ(decoded, "Hello World");

  // 空 base64
  std::string empty = applier.decodeBytes("", "base64");
  EXPECT_TRUE(empty.empty());

  // base64 编码的 JSON body
  // {"user":"admin","action":"delete"} → base64
  std::string json_body = R"({"user":"admin","action":"delete"})";
  // 手动 base64 编码验证: eyJ1c2VyIjoiYWRtaW4iLCJhY3Rpb24iOiJkZWxldGUifQ==
  std::string decoded_json = applier.decodeBytes(
      "eyJ1c2VyIjoiYWRtaW4iLCJhY3Rpb24iOiJkZWxldGUifQ==", "base64");
  EXPECT_EQ(decoded_json, json_body);
}

// ============================================================================
// 10. HeaderExtractionEmbedded — embedded模式header提取
// ============================================================================

/**
 * @brief 验证 JSON 中嵌入的 header 子对象被正确提取。
 *        strategy=Embedded, embedded_path="request.headers"
 */
TEST(E2ePipelineTest, HeaderExtractionEmbedded) {
  const std::string json = R"({
    "request": {
      "method": "GET",
      "uri": "/index.html",
      "headers": {
        "Host": "www.example.com",
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html",
        "Accept-Encoding": "gzip, deflate",
        "X-Request-Id": "abc-123-def"
      }
    }
  })";

  HeaderExtractionConfig req_headers;
  req_headers.strategy = HeaderStrategy::Embedded;
  req_headers.embedded_path = "request.headers";
  req_headers.is_request = true;

  auto config = makeJsonConfig(
      {
          makeField("request.method", "request_method"),
          makeField("request.uri", "request_uri"),
      },
      req_headers);

  LogMapper mapper(config);
  auto result = mapper.map(json);

  ASSERT_TRUE(result.has_value()) << "map() failed: " << result.error();
  auto event = *result;

  // 验证 header 数量
  EXPECT_EQ(event->request_headers_size(), 5);

  // 验证具体 header 值
  std::map<std::string, std::string> hdrs;
  for (const auto& h : event->request_headers()) {
    hdrs[h.key()] = h.value();
  }
  EXPECT_EQ(hdrs["Host"], "www.example.com");
  EXPECT_EQ(hdrs["User-Agent"], "Mozilla/5.0");
  EXPECT_EQ(hdrs["Accept"], "text/html");
  EXPECT_EQ(hdrs["Accept-Encoding"], "gzip, deflate");
  EXPECT_EQ(hdrs["X-Request-Id"], "abc-123-def");
}

// ============================================================================
// 11. HeaderExtractionPrefix — prefix模式header提取
// ============================================================================

/**
 * @brief 验证以前缀命名的顶层字段被正确提取为 headers。
 *        strategy=Prefix, prefix="http_header_"
 */
TEST(E2ePipelineTest, HeaderExtractionPrefix) {
  const std::string json = R"({
    "request": {
      "method": "POST",
      "uri": "/submit"
    },
    "http_header_Host": "api.example.com",
    "http_header_Content_Type": "application/json",
    "http_header_Authorization": "Bearer token123",
    "http_header_X_Custom": "custom-value",
    "some_other_field": "should-be-ignored"
  })";

  HeaderExtractionConfig req_headers;
  req_headers.strategy = HeaderStrategy::Prefix;
  req_headers.prefix = "http_header_";
  req_headers.is_request = true;
  req_headers.normalize_keys = true;

  auto config = makeJsonConfig(
      {
          makeField("request.method", "request_method"),
          makeField("request.uri", "request_uri"),
      },
      req_headers);

  LogMapper mapper(config);
  auto result = mapper.map(json);

  ASSERT_TRUE(result.has_value()) << "map() failed: " << result.error();
  auto event = *result;

  // 应有4个header被提取（前缀匹配）， "some_other_field" 被忽略
  EXPECT_EQ(event->request_headers_size(), 4);

  // 验证规范化后的 key（下划线转横线）
  std::map<std::string, std::string> hdrs;
  for (const auto& h : event->request_headers()) {
    hdrs[h.key()] = h.value();
  }
  EXPECT_EQ(hdrs["Host"], "api.example.com");
  EXPECT_EQ(hdrs["Content-Type"], "application/json");
  EXPECT_EQ(hdrs["Authorization"], "Bearer token123");
  EXPECT_EQ(hdrs["X-Custom"], "custom-value");
}

// ============================================================================
// 辅助: 集成 AlertBuilder 验证 → 完整管道
// ============================================================================

/**
 * @brief 端到端: Nginx log → HttpAccessEvent → AlertResult → WgeAlertEvent
 *        验证完整的检测-告警管道。
 */
TEST(E2ePipelineTest, FullPipelineNginxToWgeAlert) {
  // Step 1: 映射 Nginx log → HttpAccessEvent
  const std::string log_line =
      R"(10.0.0.1 - - [10/Oct/2023:13:55:36 +0000] "POST /login?user=admin'-- HTTP/1.1" 403 52 "https://portal.example.com/" "Mozilla/5.0")";

  const std::string pattern =
      R"((?P<ip>\S+) - (?P<user>\S+) \[(?P<ts>[^\]]+)\] "(?P<method>\S+) (?P<uri>\S+) [^"]*" (?P<status>\d+).*)";

  auto config = makeRegexConfig(
      pattern,
      {
          makeField("ip", "downstream_ip"),
          makeField("method", "request_method"),
          makeField("uri", "request_uri"),
      });
  config.timestamp_config.source_field = "ts";

  LogMapper mapper(config);
  auto map_result = mapper.map(log_line);
  ASSERT_TRUE(map_result.has_value());
  auto event = *map_result;

  // Step 2: 构造检测结果 (模拟 WGE 引擎检测)
  AlertResult alert_result;
  alert_result.event_id = "evt-550e8400-e29b-41d4-a716-446655440000";
  alert_result.timestamp_ms = event->timestamp_ms();
  alert_result.intervened = true;
  alert_result.disruptive_action = "DENY";
  alert_result.response_code = 403;

  MatchedRuleInfo rule;
  rule.rule_id = 942100;
  rule.rule_msg = "SQL Injection Attempt";
  rule.severity = 2;  // CRITICAL
  rule.rule_ver = "2024.1";
  rule.rule_tags = {"OWASP_CRS", "SQL_INJECTION"};
  rule.matched_var_name = "REQUEST_URI";
  rule.matched_var_value = "/login?user=admin'--";
  rule.matched_var_original = "/login?user=admin'--";
  rule.operator_name = "@detectSQLi";
  alert_result.matched_rules.push_back(std::move(rule));

  // Step 3: 构建告警事件
  auto alert = AlertBuilder::build(
      alert_result,
      "evt-550e8400-e29b-41d4-a716-446655440000",
      "collector-nginx-01",
      event->request_method(),
      event->request_uri(),
      event->downstream_ip(),
      "10.0.0.5" /* upstream_ip */);

  ASSERT_NE(alert, nullptr);

  // Step 4: 验证 WgeAlertEvent 字段
  EXPECT_EQ(alert->event_id(), "evt-550e8400-e29b-41d4-a716-446655440000");
  EXPECT_EQ(alert->collector_id(), "collector-nginx-01");
  EXPECT_TRUE(alert->intervened());
  EXPECT_EQ(alert->disruptive_action(), "DENY");
  EXPECT_EQ(alert->response_code(), 403);
  EXPECT_EQ(alert->request_method(), "POST");
  EXPECT_EQ(alert->request_uri(), "/login?user=admin'--");
  EXPECT_EQ(alert->downstream_ip(), "10.0.0.1");
  EXPECT_EQ(alert->upstream_ip(), "10.0.0.5");

  // 验证匹配规则
  ASSERT_EQ(alert->matched_rules_size(), 1);
  const auto& matched = alert->matched_rules(0);
  EXPECT_EQ(matched.rule_id(), 942100u);
  EXPECT_EQ(matched.rule_msg(), "SQL Injection Attempt");
  EXPECT_EQ(matched.rule_severity(), "CRITICAL");
  EXPECT_EQ(matched.rule_ver(), "2024.1");
  EXPECT_EQ(matched.rule_tags_size(), 2);
  EXPECT_EQ(matched.matched_var_name(), "REQUEST_URI");
  EXPECT_EQ(matched.operator_name(), "@detectSQLi");

  // alert_id 应为 UUID v7 格式 (36字符 + 4个短横线)
  EXPECT_EQ(alert->alert_id().size(), 36u);
  EXPECT_NE(alert->alert_id().find('-'), std::string::npos);
}
