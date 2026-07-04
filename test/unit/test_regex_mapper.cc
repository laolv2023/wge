/**
 * @file test_regex_mapper.cc
 * @brief RegexMapper 单元测试 — 测试 RE2 正则表达式日志映射
 */

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "mapper/mapper_config.h"
#include "mapper/regex_mapper.h"

using namespace wge::kafka::mapper;

// ============================================================================
// 辅助函数
// ============================================================================

namespace {

std::vector<FieldMapping> makeRegexMappings(
    std::vector<std::pair<std::string, std::string>> pairs) {
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

}  // namespace

// ============================================================================
// 1. MapNginxCombinedLog — 标准 Nginx combined log 格式解析
// ============================================================================

TEST(RegexMapperTest, MapNginxCombinedLog) {
  // 标准 Nginx combined log:
  // $remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"
  const char* log_line =
      R"(192.168.1.10 - john [10/Oct/2023:13:55:36 +0000] "GET /api/users HTTP/1.1" 200 2326 "https://example.com/" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")";

  const char* pattern =
      R"((?P<remote_addr>\S+) - (?P<remote_user>\S+) \[(?P<timestamp>[^\]]+)\] "(?P<request_method>\S+) (?P<request_uri>\S+) (?P<request_version>\S+)" (?P<response_status>\d+) (?P<body_bytes_sent>\d+) "(?P<http_referer>[^"]*)" "(?P<http_user_agent>[^"]*)")";

  RegexMapper mapper;
  auto compile_result = mapper.compile(pattern);
  ASSERT_TRUE(compile_result.has_value())
      << "Compile failed: " << compile_result.error();

  auto mappings = makeRegexMappings({
      {"remote_addr", "downstream_ip"},
      {"remote_user", "remote_user"},
      {"timestamp", "timestamp"},
      {"request_method", "request_method"},
      {"request_uri", "request_uri"},
      {"request_version", "request_version"},
      {"response_status", "response_status"},
      {"body_bytes_sent", "body_bytes_sent"},
      {"http_referer", "http_referer"},
      {"http_user_agent", "http_user_agent"},
  });

  auto result = mapper.extract(log_line, pattern, mappings);

  ASSERT_TRUE(result.has_value()) << "Extract failed: " << result.error();
  const auto& fields = *result;

  EXPECT_EQ(fields.at("downstream_ip"), "192.168.1.10");
  EXPECT_EQ(fields.at("remote_user"), "john");
  EXPECT_EQ(fields.at("timestamp"), "10/Oct/2023:13:55:36 +0000");
  EXPECT_EQ(fields.at("request_method"), "GET");
  EXPECT_EQ(fields.at("request_uri"), "/api/users");
  EXPECT_EQ(fields.at("request_version"), "HTTP/1.1");
  EXPECT_EQ(fields.at("response_status"), "200");
  EXPECT_EQ(fields.at("body_bytes_sent"), "2326");
  EXPECT_EQ(fields.at("http_referer"), "https://example.com/");
  EXPECT_EQ(fields.at("http_user_agent"),
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36");
}

// ============================================================================
// 2. MapRegexWithMissingGroup — 部分分组缺失的情况
// ============================================================================

TEST(RegexMapperTest, MapRegexWithMissingGroup) {
  // 只有 IP 和 method，不包含 referer 和 user-agent（可选字段）
  const char* log_line =
      R"(10.0.0.1 - - [10/Oct/2023:13:55:36 +0000] "GET /health HTTP/1.1" 200 5 "-" "-")";

  const char* pattern =
      R"((?P<remote_addr>\S+) - (?P<remote_user>\S+) \[(?P<timestamp>[^\]]+)\] "(?P<request_method>\S+) (?P<request_uri>\S+) (?P<request_version>\S+)" (?P<response_status>\d+) (?P<body_bytes_sent>\d+) "(?P<http_referer>[^"]*)" "(?P<http_user_agent>[^"]*)")";

  RegexMapper mapper;
  auto compile_result = mapper.compile(pattern);
  ASSERT_TRUE(compile_result.has_value());

  auto mappings = makeRegexMappings({
      {"remote_addr", "downstream_ip"},
      {"http_user_agent", "user_agent"},
  });

  // 添加一个有 default 的
  FieldMapping fm_ref;
  fm_ref.source = "http_referer";
  fm_ref.target = "referer";
  fm_ref.type = FieldType::String;
  fm_ref.default_value = "direct";
  mappings.push_back(fm_ref);

  auto result = mapper.extract(log_line, pattern, mappings);

  ASSERT_TRUE(result.has_value());
  const auto& fields = *result;

  EXPECT_EQ(fields.at("downstream_ip"), "10.0.0.1");
  EXPECT_EQ(fields.at("user_agent"), "-");
  EXPECT_EQ(fields.at("referer"), "direct");
}

// ============================================================================
// 3. MapRegexNoMatch — 完全不匹配的输入
// ============================================================================

TEST(RegexMapperTest, MapRegexNoMatch) {
  const char* log_line = "This is not a valid nginx log line at all";

  const char* pattern =
      R"((?P<remote_addr>\S+) - (?P<remote_user>\S+) \[(?P<timestamp>[^\]]+)\] "(?P<request_method>\S+).*")";

  RegexMapper mapper;
  auto compile_result = mapper.compile(pattern);
  ASSERT_TRUE(compile_result.has_value());

  auto mappings = makeRegexMappings({
      {"remote_addr", "downstream_ip"},
  });

  auto result = mapper.extract(log_line, pattern, mappings);

  EXPECT_FALSE(result.has_value());
  EXPECT_TRUE(result.error().find("did not match") != std::string::npos ||
              result.error().find("not match") != std::string::npos);
}

TEST(RegexMapperTest, MapRegexEmptyPayload) {
  const char* pattern = R"((?P<field>\S+))";

  RegexMapper mapper;
  auto compile_result = mapper.compile(pattern);
  ASSERT_TRUE(compile_result.has_value());

  auto mappings = makeRegexMappings({{"field", "target"}});

  auto result = mapper.extract("", pattern, mappings);
  EXPECT_FALSE(result.has_value());
}

// ============================================================================
// 4. MapRegexInvalidPattern — 非法正则表达式
// ============================================================================

TEST(RegexMapperTest, MapRegexInvalidPattern) {
  RegexMapper mapper;

  // 非法的正则表达式：未闭合的括号
  const char* bad_pattern = R"((?P<name>\S+";

  auto result = mapper.compile(bad_pattern);
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(result.error().empty());
}

TEST(RegexMapperTest, MapRegexNoNamedGroups) {
  // 有捕获组但没有命名
  const char* pattern = R"((\S+) (\S+))";

  RegexMapper mapper;
  auto result = mapper.compile(pattern);

  if (result.has_value()) {
    // 编译成功，但提取时应失败（没有命名捕获组）
    auto extract_result = mapper.extract("hello world", pattern,
                                         makeRegexMappings({{"grp", "tgt"}}));
    EXPECT_FALSE(extract_result.has_value());
  } else {
    SUCCEED() << "RE2 rejected pattern without named groups";
  }
}

// ============================================================================
// 5. MapRegexWithTimestampExtract — 时间戳提取并正确转换
// ============================================================================

TEST(RegexMapperTest, MapRegexWithTimestampExtract) {
  const char* log_line =
      R"(10.0.0.1 - - [10/Oct/2023:13:55:36 +0000] "GET / HTTP/1.1" 200 100 "-" "-")";

  const char* pattern =
      R"((?P<remote_addr>\S+) - (?P<remote_user>\S+) \[(?P<timestamp>[^\]]+)\] "(?P<request_method>\S+) (?P<request_uri>\S+) (?P<request_version>\S+)" (?P<response_status>\d+) (?P<body_bytes_sent>\d+).*)";

  RegexMapper mapper;
  auto compile_result = mapper.compile(pattern);
  ASSERT_TRUE(compile_result.has_value());

  // 验证 timestamp 捕获组存在
  auto mappings = makeRegexMappings({
      {"remote_addr", "downstream_ip"},
      {"timestamp", "timestamp"},
  });

  auto result = mapper.extract(log_line, pattern, mappings);
  ASSERT_TRUE(result.has_value());

  const auto& fields = *result;
  EXPECT_EQ(fields.at("timestamp"), "10/Oct/2023:13:55:36 +0000");

  // 通过 RegexMapper::parseTimestamp 解析 Nginx 格式
  auto ts_ms = mapper.parseTimestamp(fields.at("timestamp"), {});
  EXPECT_GT(ts_ms, 0);
}

// ============================================================================
// 6. MapRegexWithHeaderMapping — Header 特殊映射 (User-Agent, Referer)
// ============================================================================

TEST(RegexMapperTest, MapRegexWithHeaderMapping) {
  const char* log_line =
      R"(10.0.0.2 - - [10/Oct/2023:13:55:36 +0000] "GET /api/data HTTP/1.1" 200 1234 "https://referer.example.com/page" "curl/7.88.1")";

  const char* pattern =
      R"((?P<remote_addr>\S+) - (?P<remote_user>\S+) \[(?P<timestamp>[^\]]+)\] "(?P<request_method>\S+) (?P<request_uri>\S+) (?P<request_version>\S+)" (?P<response_status>\d+) (?P<body_bytes_sent>\d+) "(?P<http_referer>[^"]*)" "(?P<http_user_agent>[^"]*)")";

  RegexMapper mapper;
  auto compile_result = mapper.compile(pattern);
  ASSERT_TRUE(compile_result.has_value());

  auto mappings = makeRegexMappings({
      {"http_referer", "referer"},
      {"http_user_agent", "user_agent"},
      {"request_method", "request_method"},
  });

  auto result = mapper.extract(log_line, pattern, mappings);
  ASSERT_TRUE(result.has_value());
  const auto& fields = *result;

  EXPECT_EQ(fields.at("referer"), "https://referer.example.com/page");
  EXPECT_EQ(fields.at("user_agent"), "curl/7.88.1");
  EXPECT_EQ(fields.at("request_method"), "GET");
}

// ============================================================================
// 7. MapRegexRequestVersionStrip — HTTP/1.1 → 1.1 前缀去除
// ============================================================================

TEST(RegexMapperTest, MapRegexRequestVersionStrip) {
  // 验证 HTTP version 捕获中 "HTTP/" 前缀的存在
  // 实际 strip 在 FieldApplier 层完成，这里验证捕获组包含完整版本字符串
  const char* log_line =
      R"(10.0.0.3 - - [10/Oct/2023:13:55:36 +0000] "GET / HTTP/1.1" 200 500 "-" "-")";

  const char* pattern =
      R"((?P<remote_addr>\S+) - (?P<remote_user>\S+) \[(?P<timestamp>[^\]]+)\] "(?P<request_method>\S+) (?P<request_uri>\S+) (?P<request_version>\S+)" (?P<response_status>\d+) (?P<body_bytes_sent>\d+).*)";

  RegexMapper mapper;
  auto compile_result = mapper.compile(pattern);
  ASSERT_TRUE(compile_result.has_value());

  auto mappings = makeRegexMappings({
      {"request_version", "request_version"},
  });

  auto result = mapper.extract(log_line, pattern, mappings);
  ASSERT_TRUE(result.has_value());
  const auto& fields = *result;

  // 捕获组包含完整的 "HTTP/1.1"
  EXPECT_EQ(fields.at("request_version"), "HTTP/1.1");

  // 手动 strip 验证
  std::string version = fields.at("request_version");
  if (version.starts_with("HTTP/")) {
    version = version.substr(5);
  }
  EXPECT_EQ(version, "1.1");
}

TEST(RegexMapperTest, MapRegexHttp2Version) {
  const char* log_line =
      R"(10.0.0.4 - - [10/Oct/2023:13:55:36 +0000] "GET / HTTP/2.0" 200 300 "-" "-")";

  const char* pattern =
      R"((?P<remote_addr>\S+) - (?P<remote_user>\S+) \[(?P<timestamp>[^\]]+)\] "(?P<request_method>\S+) (?P<request_uri>\S+) (?P<request_version>\S+)" (?P<response_status>\d+) (?P<body_bytes_sent>\d+).*)";

  RegexMapper mapper;
  auto compile_result = mapper.compile(pattern);
  ASSERT_TRUE(compile_result.has_value());

  auto mappings = makeRegexMappings({
      {"request_version", "request_version"},
  });

  auto result = mapper.extract(log_line, pattern, mappings);
  ASSERT_TRUE(result.has_value());

  std::string version = (*result).at("request_version");
  EXPECT_EQ(version, "HTTP/2.0");
  if (version.starts_with("HTTP/")) {
    version = version.substr(5);
  }
  EXPECT_EQ(version, "2.0");
}

// ============================================================================
// 8. MapRegexWithRequiredCaptureGroup — 必填捕获组缺失
// ============================================================================

TEST(RegexMapperTest, MapRegexWithRequiredGroupMissing) {
  const char* log_line = R"(simple line without enough groups)";

  const char* pattern = R"((?P<first>\S+)\s+(?P<second>\S+))";

  RegexMapper mapper;
  auto compile_result = mapper.compile(pattern);
  ASSERT_TRUE(compile_result.has_value());

  std::vector<FieldMapping> mappings;
  // first 存在
  FieldMapping fm1;
  fm1.source = "first";
  fm1.target = "t1";
  fm1.type = FieldType::String;
  mappings.push_back(fm1);

  // required_group 不在 pattern 的捕获组中
  FieldMapping fm2;
  fm2.source = "nonexistent";
  fm2.target = "t2";
  fm2.type = FieldType::String;
  fm2.required = true;
  mappings.push_back(fm2);

  auto result = mapper.extract(log_line, pattern, mappings);
  EXPECT_FALSE(result.has_value());
}

// ============================================================================
// 9. RegexMapper parseTimestamp 多格式测试
// ============================================================================

TEST(RegexMapperTest, ParseTimestampNginxFormat) {
  RegexMapper mapper;
  auto ts = mapper.parseTimestamp("10/Oct/2023:13:55:36 +0000", {});
  EXPECT_GT(ts, 0);
}

TEST(RegexMapperTest, ParseTimestampIso8601) {
  RegexMapper mapper;
  auto ts = mapper.parseTimestamp("2023-10-10T13:55:36Z", {});
  EXPECT_GT(ts, 0);
}

TEST(RegexMapperTest, ParseTimestampUnixSeconds) {
  RegexMapper mapper;
  auto ts = mapper.parseTimestamp("1696946136", {});
  EXPECT_GT(ts, 0);
}

TEST(RegexMapperTest, ParseTimestampUnixMilliseconds) {
  RegexMapper mapper;
  auto ts = mapper.parseTimestamp("1696946136000", {});
  EXPECT_GT(ts, 0);
}

// P2-1 回归测试: ISO 8601 无毫秒但有时区偏移
TEST(RegexMapperTest, ParseTimestampIso8601WithOffset) {
  RegexMapper mapper;
  auto ts = mapper.parseTimestamp("2024-01-15T10:30:00+08:00", {});
  EXPECT_GT(ts, 0);
  // +08:00 → UTC 02:30:00, 应比 Z 后缀版本早 8 小时
  auto ts_utc = mapper.parseTimestamp("2024-01-15T10:30:00Z", {});
  EXPECT_EQ(ts, ts_utc - 8 * 3600 * 1000)
      << "Timestamp with +08:00 offset should be 8h earlier than UTC";
}

// P2-1 回归测试: ISO 8601 无毫秒但有时区偏移 (负偏移)
TEST(RegexMapperTest, ParseTimestampIso8601WithNegativeOffset) {
  RegexMapper mapper;
  auto ts = mapper.parseTimestamp("2024-01-15T10:30:00-05:00", {});
  EXPECT_GT(ts, 0);
  auto ts_utc = mapper.parseTimestamp("2024-01-15T10:30:00Z", {});
  EXPECT_EQ(ts, ts_utc + 5 * 3600 * 1000)
      << "Timestamp with -05:00 offset should be 5h later than UTC";
}
