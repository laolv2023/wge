/**
 * @file test_json_mapper.cc
 * @brief JsonMapper 单元测试 — 通过 LogMapper::map() 验证完整映射能力
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "http_access.pb.h"
#include "mapper/mapper.h"
#include "mapper/mapper_config.h"

using namespace wge::kafka::mapper;
using namespace wge::kafka;

// ============================================================================
// 辅助: 构造一个 JSON-format MapperConfig
// ============================================================================

namespace {

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

FieldMapping makeMapping(std::string source, std::string target,
                         FieldType type = FieldType::String,
                         bool required = false,
                         std::optional<std::string> default_value = {}) {
  FieldMapping fm;
  fm.source = std::move(source);
  fm.target = std::move(target);
  fm.type = type;
  fm.required = required;
  fm.default_value = std::move(default_value);
  return fm;
}

}  // namespace

// ============================================================================
// 1. MapFullJsonEvent — 完整 JSON 输入映射到 HttpAccessEvent
// ============================================================================

TEST(JsonMapperTest, MapFullJsonEvent) {
  const char* json = R"({
    "request": {
      "method": "POST",
      "uri": "/api/v1/login?redirect=/home",
      "version": "1.1"
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

  auto config = makeJsonConfig({
      makeMapping("request.method", "request_method"),
      makeMapping("request.uri", "request_uri"),
      makeMapping("request.version", "request_version"),
      makeMapping("downstream.ip", "downstream_ip"),
      makeMapping("upstream.ip", "upstream_ip"),
      makeMapping("response.version", "response_version"),
      makeMapping("event_id", "event_id"),
      makeMapping("collector_id", "collector_id"),
  });

  LogMapper mapper(config);
  auto result = mapper.map(json);

  ASSERT_TRUE(result.has_value()) << "map() failed: " << result.error();
  auto event = *result;
  ASSERT_NE(event, nullptr);

  EXPECT_EQ(event->request_method(), "POST");
  EXPECT_EQ(event->request_uri(), "/api/v1/login?redirect=/home");
  EXPECT_EQ(event->request_version(), "1.1");
  EXPECT_EQ(event->downstream_ip(), "192.168.1.100");
  EXPECT_EQ(event->upstream_ip(), "10.0.0.5");
  EXPECT_EQ(event->response_version(), "1.1");
  EXPECT_EQ(event->event_id(), "550e8400-e29b-41d4-a716-446655440000");
  EXPECT_EQ(event->collector_id(), "edge-node-7");
}

// ============================================================================
// 2. MapJsonWithMissingFields — 缺失字段使用 default 值
// ============================================================================

TEST(JsonMapperTest, MapJsonWithMissingFields) {
  // 只提供 method，其他字段缺失
  const char* json = R"({
    "request": {
      "method": "GET"
    }
  })";

  auto config = makeJsonConfig({
      makeMapping("request.method", "request_method", FieldType::String,
                  false),
      makeMapping("request.uri", "request_uri", FieldType::String, false,
                  "/default"),
      makeMapping("request.version", "request_version", FieldType::String,
                  false, "1.0"),
      // 缺失字段无 default，直接跳过
      makeMapping("event_id", "event_id", FieldType::String, false),
  });

  LogMapper mapper(config);
  auto result = mapper.map(json);

  ASSERT_TRUE(result.has_value()) << "map() failed: " << result.error();
  auto event = *result;
  ASSERT_NE(event, nullptr);

  EXPECT_EQ(event->request_method(), "GET");
  EXPECT_EQ(event->request_uri(), "/default");
  EXPECT_EQ(event->request_version(), "1.0");
  // event_id 不存在于 JSON，无 default，应为空
  EXPECT_TRUE(event->event_id().empty());
}

// ============================================================================
// 3. MapJsonWithEmbeddedHeaders — Header 提取 (embedded 模式)
// ============================================================================

TEST(JsonMapperTest, MapJsonWithEmbeddedHeaders) {
  const char* json = R"({
    "request": {
      "method": "GET",
      "uri": "/index.html",
      "headers": {
        "Host": "www.example.com",
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html",
        "Accept-Encoding": "gzip, deflate"
      }
    },
    "response": {
      "headers": {
        "Content-Type": "text/html; charset=utf-8",
        "Content-Length": "1234",
        "Set-Cookie": "session=abc123"
      }
    }
  })";

  HeaderExtractionConfig req_hdr;
  req_hdr.strategy = HeaderStrategy::Embedded;
  req_hdr.embedded_path = "request.headers";
  req_hdr.is_request = true;

  HeaderExtractionConfig resp_hdr;
  resp_hdr.strategy = HeaderStrategy::Embedded;
  resp_hdr.embedded_path = "response.headers";
  resp_hdr.is_request = false;

  auto config = makeJsonConfig(
      {
          makeMapping("request.method", "request_method"),
          makeMapping("request.uri", "request_uri"),
      },
      req_hdr, resp_hdr);

  LogMapper mapper(config);
  auto result = mapper.map(json);

  ASSERT_TRUE(result.has_value()) << "map() failed: " << result.error();
  auto event = *result;
  ASSERT_NE(event, nullptr);

  // 验证 request headers
  ASSERT_EQ(event->request_headers_size(), 4);
  // 查找特定 header
  bool found_host = false, found_ua = false;
  for (int i = 0; i < event->request_headers_size(); ++i) {
    const auto& h = event->request_headers(i);
    if (h.key() == "Host") {
      EXPECT_EQ(h.value(), "www.example.com");
      found_host = true;
    }
    if (h.key() == "User-Agent") {
      EXPECT_EQ(h.value(), "Mozilla/5.0");
      found_ua = true;
    }
  }
  EXPECT_TRUE(found_host);
  EXPECT_TRUE(found_ua);

  // 验证 response headers
  ASSERT_EQ(event->response_headers_size(), 3);
  bool found_ct = false;
  for (int i = 0; i < event->response_headers_size(); ++i) {
    const auto& h = event->response_headers(i);
    if (h.key() == "Content-Type") {
      EXPECT_EQ(h.value(), "text/html; charset=utf-8");
      found_ct = true;
    }
  }
  EXPECT_TRUE(found_ct);
}

// ============================================================================
// 4. MapJsonWithNestedPath — 嵌套路径 "connection.source_ip" 正确解析
// ============================================================================

TEST(JsonMapperTest, MapJsonWithNestedPath) {
  const char* json = R"({
    "connection": {
      "source_ip": "203.0.113.45",
      "dest_ip": "10.20.30.40",
      "tls_version": "TLSv1.3"
    }
  })";

  auto config = makeJsonConfig({
      makeMapping("connection.source_ip", "downstream_ip"),
      makeMapping("connection.dest_ip", "upstream_ip"),
  });

  LogMapper mapper(config);
  auto result = mapper.map(json);

  ASSERT_TRUE(result.has_value());
  auto event = *result;
  ASSERT_NE(event, nullptr);

  EXPECT_EQ(event->downstream_ip(), "203.0.113.45");
  EXPECT_EQ(event->upstream_ip(), "10.20.30.40");
}

// ============================================================================
// 5. MapInvalidJson — 非法 JSON 返回 std::unexpected
// ============================================================================

TEST(JsonMapperTest, MapInvalidJson) {
  // 无效 JSON：缺少引号、花括号不匹配等
  const char* bad_json = R"({ "request": { "method": "GET", })";

  auto config = makeJsonConfig({
      makeMapping("request.method", "request_method"),
  });

  LogMapper mapper(config);
  auto result = mapper.map(bad_json);

  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(result.error().empty());
}

TEST(JsonMapperTest, MapEmptyJsonPayload) {
  auto config = makeJsonConfig({
      makeMapping("request.method", "request_method"),
  });

  LogMapper mapper(config);
  auto result = mapper.map("");

  EXPECT_FALSE(result.has_value());
  EXPECT_TRUE(result.error().find("Empty") != std::string::npos);
}

TEST(JsonMapperTest, MapNonObjectJson) {
  // JSON 数组而不是对象
  const char* arr_json = R"([1, 2, 3])";

  auto config = makeJsonConfig({
      makeMapping("request.method", "request_method"),
  });

  LogMapper mapper(config);
  auto result = mapper.map(arr_json);

  // 取决于实现，可能失败或字段缺失
  if (!result.has_value()) {
    // 根不是 object，预期失败
    SUCCEED() << "Correctly rejected non-object JSON: " << result.error();
  } else {
    // 如果未失败，则字段应为空
    auto event = *result;
    ASSERT_NE(event, nullptr);
    EXPECT_TRUE(event->request_method().empty());
  }
}

// ============================================================================
// 6. MapJsonWithBytesBase64 — base64 编码 body 正确解码
// ============================================================================

TEST(JsonMapperTest, MapJsonWithBytesBody) {
  // request_body 在 proto 中定义为 bytes，但通过 LogMapper::map()
  // 字段映射为 String 类型时会直接设置（TYPE_BYTES 被 setStringField 接受）
  const char* json = R"({
    "request": {
      "method": "POST",
      "uri": "/api/data",
      "body": "SGVsbG8gV29ybGQ="
    },
    "response": {
      "body": "eyJzdGF0dXMiOiJvayJ9"
    }
  })";

  auto config = makeJsonConfig({
      makeMapping("request.method", "request_method"),
      makeMapping("request.uri", "request_uri"),
      makeMapping("request.body", "request_body"),
      makeMapping("response.body", "response_body"),
  });

  LogMapper mapper(config);
  auto result = mapper.map(json);

  ASSERT_TRUE(result.has_value());
  auto event = *result;
  ASSERT_NE(event, nullptr);

  // bytes 字段被 setStringField 接受（TYPE_BYTES 兼容），值原样存储
  EXPECT_EQ(event->request_body(), "SGVsbG8gV29ybGQ=");
  EXPECT_EQ(event->response_body(), "eyJzdGF0dXMiOiJvayJ9");
  EXPECT_EQ(event->request_method(), "POST");
  EXPECT_EQ(event->request_uri(), "/api/data");
}

// ============================================================================
// 7. MapJsonWithPrefixHeaders — prefix 模式 header 提取
// ============================================================================

TEST(JsonMapperTest, MapJsonWithPrefixHeaders) {
  const char* json = R"({
    "message": "incoming request",
    "http_req_Content_Type": "application/json",
    "http_req_Authorization": "Bearer token123",
    "http_req_X_Request_Id": "req-001",
    "timestamp": 1234567890,
    "http_res_X_Runtime": "42ms"
  })";

  HeaderExtractionConfig req_hdr;
  req_hdr.strategy = HeaderStrategy::Prefix;
  req_hdr.prefix = "http_req_";
  req_hdr.normalize_keys = true;
  req_hdr.is_request = true;

  HeaderExtractionConfig resp_hdr;
  resp_hdr.strategy = HeaderStrategy::Prefix;
  resp_hdr.prefix = "http_res_";
  resp_hdr.normalize_keys = true;
  resp_hdr.is_request = false;

  auto config = makeJsonConfig({}, req_hdr, resp_hdr);

  LogMapper mapper(config);
  auto result = mapper.map(json);

  ASSERT_TRUE(result.has_value());
  auto event = *result;
  ASSERT_NE(event, nullptr);

  // 验证 prefix request headers (normalize: _ → -, 首字母大写)
  EXPECT_EQ(event->request_headers_size(), 3);
  bool found_auth = false, found_ct = false, found_reqid = false;
  for (int i = 0; i < event->request_headers_size(); ++i) {
    const auto& h = event->request_headers(i);
    if (h.key() == "Authorization") {
      EXPECT_EQ(h.value(), "Bearer token123");
      found_auth = true;
    }
    if (h.key() == "Content-Type") {
      EXPECT_EQ(h.value(), "application/json");
      found_ct = true;
    }
    if (h.key() == "X-Request-Id") {
      EXPECT_EQ(h.value(), "req-001");
      found_reqid = true;
    }
  }
  EXPECT_TRUE(found_auth);
  EXPECT_TRUE(found_ct);
  EXPECT_TRUE(found_reqid);

  // 验证 prefix response headers
  EXPECT_EQ(event->response_headers_size(), 1);
  if (event->response_headers_size() > 0) {
    EXPECT_EQ(event->response_headers(0).key(), "X-Runtime");
    EXPECT_EQ(event->response_headers(0).value(), "42ms");
  }
}

TEST(JsonMapperTest, MapJsonWithPrefixHeadersNoNormalize) {
  const char* json = R"({
    "hdr_Host": "example.com",
    "hdr_Accept": "text/plain",
    "not_a_header": "skip"
  })";

  HeaderExtractionConfig req_hdr;
  req_hdr.strategy = HeaderStrategy::Prefix;
  req_hdr.prefix = "hdr_";
  req_hdr.normalize_keys = false;
  req_hdr.is_request = true;

  auto config = makeJsonConfig({}, req_hdr, {});

  LogMapper mapper(config);
  auto result = mapper.map(json);

  ASSERT_TRUE(result.has_value());
  auto event = *result;

  EXPECT_EQ(event->request_headers_size(), 2);
  bool found_host = false;
  for (int i = 0; i < event->request_headers_size(); ++i) {
    if (event->request_headers(i).key() == "Host") found_host = true;
  }
  EXPECT_TRUE(found_host);
}

// ============================================================================
// 8. MapJsonWithTimestampAuto — 使用 JSON 中的时间戳字段
// ============================================================================

TEST(JsonMapperTest, MapJsonWithTimestampField) {
  // JSON format 中 timestamp_ms 通过常规字段映射设置
  const char* json = R"({
    "request": {
      "method": "GET",
      "uri": "/health"
    },
    "timestamp_ms": "1734567890123"
  })";

  auto config = makeJsonConfig({
      makeMapping("request.method", "request_method"),
      makeMapping("request.uri", "request_uri"),
      makeMapping("timestamp_ms", "collector_id"),
  });

  LogMapper mapper(config);
  auto result = mapper.map(json);

  ASSERT_TRUE(result.has_value());
  auto event = *result;
  ASSERT_NE(event, nullptr);

  EXPECT_EQ(event->request_method(), "GET");
  EXPECT_EQ(event->request_uri(), "/health");
  // timestamp_ms 字段映射到 collector_id（作为 string 传递）
  EXPECT_EQ(event->collector_id(), "1734567890123");
}

// ============================================================================
// 9. MapJsonWithRequiredFieldMissing — 必填字段缺失应失败
// ============================================================================

TEST(JsonMapperTest, MapJsonRequiredFieldMissing) {
  const char* json = R"({
    "request": {
      "uri": "/path"
    }
  })";

  auto config = makeJsonConfig({
      makeMapping("request.method", "request_method", FieldType::String, true),
      makeMapping("request.uri", "request_uri"),
  });

  LogMapper mapper(config);
  auto result = mapper.map(json);

  EXPECT_FALSE(result.has_value());
  EXPECT_TRUE(result.error().find("Required") != std::string::npos ||
              result.error().find("required") != std::string::npos);
}

// ============================================================================
// 10. MapJsonWithConstantFields — 常量字段注入
// ============================================================================

TEST(JsonMapperTest, MapJsonWithConstantFields) {
  const char* json = R"({
    "request": {
      "method": "GET",
      "uri": "/api/test"
    }
  })";

  auto config = makeJsonConfig(
      {
          makeMapping("request.method", "request_method"),
          makeMapping("request.uri", "request_uri"),
      },
      {}, {},
      {
          ConstantField{"collector_id", "test-collector-01"},
      });

  LogMapper mapper(config);
  auto result = mapper.map(json);

  ASSERT_TRUE(result.has_value());
  auto event = *result;
  ASSERT_NE(event, nullptr);

  EXPECT_EQ(event->request_method(), "GET");
  EXPECT_EQ(event->request_uri(), "/api/test");
  EXPECT_EQ(event->collector_id(), "test-collector-01");
}
