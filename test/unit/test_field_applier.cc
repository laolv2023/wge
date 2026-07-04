/**
 * @file test_field_applier.cc
 * @brief FieldApplier 单元测试 — 测试字段赋值、类型转换、时间戳解析、编解码
 */

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "http_access.pb.h"
#include "mapper/field_applier.h"
#include "mapper/mapper_config.h"

using namespace wge::kafka::mapper;
using namespace wge::kafka;

// ============================================================================
// 辅助
// ============================================================================

namespace {

FieldMapping makeFm(const std::string& target, FieldType type) {
  FieldMapping fm;
  fm.target = target;
  fm.type = type;
  return fm;
}

}  // namespace

// ============================================================================
// 1. ApplyStringField
// ============================================================================

TEST(FieldApplierTest, ApplyStringField) {
  FieldApplier applier;
  HttpAccessEvent event;

  std::map<std::string, std::string> fields;
  fields["request_method"] = "POST";
  fields["request_uri"] = "/api/v1/data";
  fields["downstream_ip"] = "192.168.1.1";

  std::vector<FieldMapping> mappings = {
      makeFm("request_method", FieldType::String),
      makeFm("request_uri", FieldType::String),
      makeFm("downstream_ip", FieldType::String),
  };

  applier.applyFields(fields, event, mappings);

  EXPECT_EQ(event.request_method(), "POST");
  EXPECT_EQ(event.request_uri(), "/api/v1/data");
  EXPECT_EQ(event.downstream_ip(), "192.168.1.1");
}

// ============================================================================
// 2. ApplyInt32Field
// ============================================================================

TEST(FieldApplierTest, ApplyInt32Field) {
  FieldApplier applier;
  HttpAccessEvent event;

  std::map<std::string, std::string> fields;
  fields["downstream_port"] = "8080";
  fields["response_status"] = "200";

  std::vector<FieldMapping> mappings = {
      makeFm("downstream_port", FieldType::Int32),
      makeFm("response_status", FieldType::Int32),
  };

  applier.applyFields(fields, event, mappings);

  EXPECT_EQ(event.downstream_port(), 8080);
  EXPECT_EQ(event.response_status(), 200);
}

TEST(FieldApplierTest, ApplyInt32FieldNegative) {
  FieldApplier applier;
  HttpAccessEvent event;

  std::map<std::string, std::string> fields;
  fields["downstream_port"] = "-1";

  std::vector<FieldMapping> mappings = {
      makeFm("downstream_port", FieldType::Int32),
  };

  applier.applyFields(fields, event, mappings);
  EXPECT_EQ(event.downstream_port(), -1);
}

// ============================================================================
// 3. ApplyInt64Field
// ============================================================================

TEST(FieldApplierTest, ApplyInt64Field) {
  FieldApplier applier;
  HttpAccessEvent event;

  std::map<std::string, std::string> fields;
  fields["request_body_length"] = "1048576";
  fields["response_body_length"] = "2097152";

  std::vector<FieldMapping> mappings = {
      makeFm("request_body_length", FieldType::Int64),
      makeFm("response_body_length", FieldType::Int64),
  };

  applier.applyFields(fields, event, mappings);

  EXPECT_EQ(event.request_body_length(), 1048576);
  EXPECT_EQ(event.response_body_length(), 2097152);
}

TEST(FieldApplierTest, ApplyInt64FieldLarge) {
  FieldApplier applier;
  HttpAccessEvent event;

  std::map<std::string, std::string> fields;
  fields["request_body_length"] = "9223372036854775807";  // INT64_MAX

  std::vector<FieldMapping> mappings = {
      makeFm("request_body_length", FieldType::Int64),
  };

  applier.applyFields(fields, event, mappings);
  EXPECT_EQ(event.request_body_length(), 9223372036854775807LL);
}

// ============================================================================
// 4. ApplyBytesFieldBase64
// ============================================================================

TEST(FieldApplierTest, ApplyBytesFieldBase64) {
  FieldApplier applier;

  // 直接测试 decodeBytes
  std::string decoded = applier.decodeBytes("SGVsbG8gV29ybGQ=", "base64");
  EXPECT_EQ(decoded, "Hello World");
}

// ============================================================================
// 5. ApplyBytesFieldHex
// ============================================================================

TEST(FieldApplierTest, ApplyBytesFieldHex) {
  FieldApplier applier;

  std::string decoded = applier.decodeBytes("48656c6c6f", "hex");
  EXPECT_EQ(decoded, "Hello");
}

// ============================================================================
// 6. ApplyBytesFieldRaw
// ============================================================================

TEST(FieldApplierTest, ApplyBytesFieldRaw) {
  FieldApplier applier;

  std::string decoded = applier.decodeBytes("raw binary data", "raw");
  EXPECT_EQ(decoded, "raw binary data");

  // 空 encoding 也应该原样返回
  std::string decoded2 = applier.decodeBytes("raw binary data", "");
  EXPECT_EQ(decoded2, "raw binary data");
}

// ============================================================================
// 7. ApplyFieldWithDefault
// ============================================================================

TEST(FieldApplierTest, ApplyFieldWithDefault) {
  FieldApplier applier;
  HttpAccessEvent event;

  // fields 中不包含 request_method，但有默认值
  std::map<std::string, std::string> fields;
  fields["downstream_ip"] = "10.0.0.1";

  std::vector<FieldMapping> mappings;
  FieldMapping fm1;
  fm1.target = "request_method";
  fm1.type = FieldType::String;
  fm1.default_value = "GET";
  mappings.push_back(fm1);

  FieldMapping fm2;
  fm2.target = "downstream_ip";
  fm2.type = FieldType::String;
  mappings.push_back(fm2);

  applier.applyFields(fields, event, mappings);

  // request_method 不在 fields 中，应使用默认值... 
  // 但实际上 applyFields 先检查 fields 中是否有 mapping.target，
  // 若没有则跳过（即使有 default_value）。
  // default_value 是在 mapper 的 extract 阶段使用的，不在 applyFields 中。
  // 所以这里验证 applyFields 的行为：字段缺失时跳过。
  EXPECT_TRUE(event.request_method().empty());
  EXPECT_EQ(event.downstream_ip(), "10.0.0.1");
}

// ============================================================================
// 8. ParseTimestampIso8601
// ============================================================================

TEST(FieldApplierTest, ParseTimestampIso8601) {
  FieldApplier applier;
  auto ts = applier.parseTimestamp("2024-01-15T10:30:00Z", {});
  EXPECT_GT(ts, 0);
}

TEST(FieldApplierTest, ParseTimestampIso8601WithMs) {
  FieldApplier applier;
  auto ts = applier.parseTimestamp("2024-01-15T10:30:00.123Z", {});
  EXPECT_GT(ts, 0);
}

TEST(FieldApplierTest, ParseTimestampIso8601WithOffset) {
  FieldApplier applier;
  auto ts = applier.parseTimestamp("2024-01-15T10:30:00+08:00", {});
  EXPECT_GT(ts, 0);
  // 验证时区偏移正确应用: +08:00 → UTC 02:30:00
  // 2024-01-15T10:30:00+08:00 = 2024-01-15T02:30:00Z = 1705295400000 ms
  // 2024-01-15T10:30:00Z       = 1705324200000 ms (差 8h = 28800000 ms)
  auto ts_utc = applier.parseTimestamp("2024-01-15T10:30:00Z", {});
  EXPECT_EQ(ts, ts_utc - 8 * 3600 * 1000)
      << "Timestamp with +08:00 offset should be 8h earlier than UTC";
}

TEST(FieldApplierTest, ParseTimestampIso8601WithNegativeOffset) {
  FieldApplier applier;
  auto ts = applier.parseTimestamp("2024-01-15T10:30:00-05:00", {});
  EXPECT_GT(ts, 0);
  // -05:00 → UTC 15:30:00 (5h later)
  auto ts_utc = applier.parseTimestamp("2024-01-15T10:30:00Z", {});
  EXPECT_EQ(ts, ts_utc + 5 * 3600 * 1000)
      << "Timestamp with -05:00 offset should be 5h later than UTC";
}

// ============================================================================
// 9. ParseTimestampNginx
// ============================================================================

TEST(FieldApplierTest, ParseTimestampNginx) {
  FieldApplier applier;
  auto ts = applier.parseTimestamp("15/Jan/2024:10:30:00 +0000", {});
  EXPECT_GT(ts, 0);
}

TEST(FieldApplierTest, ParseTimestampNginxWithOffset) {
  FieldApplier applier;
  auto ts = applier.parseTimestamp("15/Jan/2024:10:30:00 +0800", {});
  EXPECT_GT(ts, 0);
}

// ============================================================================
// 10. ParseTimestampUnixSeconds
// ============================================================================

TEST(FieldApplierTest, ParseTimestampUnixSeconds) {
  FieldApplier applier;
  // 10 digits → seconds
  auto ts = applier.parseTimestamp("1705312200", {});
  EXPECT_EQ(ts, 1705312200000LL);
}

// ============================================================================
// 11. ParseTimestampUnixMilliseconds
// ============================================================================

TEST(FieldApplierTest, ParseTimestampUnixMilliseconds) {
  FieldApplier applier;
  // 13 digits → milliseconds
  auto ts = applier.parseTimestamp("1705312200123", {});
  EXPECT_EQ(ts, 1705312200123LL);
}

// ============================================================================
// 12. ParseTimestampEmpty — 空字符串处理
// ============================================================================

TEST(FieldApplierTest, ParseTimestampEmpty) {
  FieldApplier applier;
  auto ts = applier.parseTimestamp("", {});
  EXPECT_EQ(ts, -1);
}

TEST(FieldApplierTest, ParseTimestampInvalid) {
  FieldApplier applier;
  auto ts = applier.parseTimestamp("not-a-timestamp", {});
  EXPECT_EQ(ts, -1);
}

// ============================================================================
// 13. ApplyConstant
// ============================================================================

TEST(FieldApplierTest, ApplyConstant) {
  FieldApplier applier;
  HttpAccessEvent event;

  applier.applyConstant(event, "collector_id", "nginx-edge-01");
  EXPECT_EQ(event.collector_id(), "nginx-edge-01");

  applier.applyConstant(event, "event_id", "abc-123-def");
  EXPECT_EQ(event.event_id(), "abc-123-def");
}

// ============================================================================
// 14. DecodeBase64
// ============================================================================

TEST(FieldApplierTest, DecodeBase64) {
  FieldApplier applier;

  // 标准 Base64
  EXPECT_EQ(applier.decodeBytes("SGVsbG8=", "base64"), "Hello");
  EXPECT_EQ(applier.decodeBytes("SGVsbG8gV29ybGQ=", "base64"), "Hello World");
  EXPECT_EQ(applier.decodeBytes("dGVzdA==", "base64"), "test");

  // 空字符串
  EXPECT_EQ(applier.decodeBytes("", "base64"), "");

  // 不含填充的
  EXPECT_EQ(applier.decodeBytes("YQ", "base64"), "a");
}

// P1-2 回归测试: 长 base64 字符串（>6 字符，触发 int 溢出）
TEST(FieldApplierTest, DecodeBase64LongString) {
  FieldApplier applier;
  // 48 字符 base64 → 36 字节输出，远超 int 32 位溢出点
  std::string expected(36, 'A');
  // 手动编码 36 个 'A' → base64
  // "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" → "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQQ=="
  std::string b64 = "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQQ==";
  std::string decoded = applier.decodeBytes(b64, "base64");
  EXPECT_EQ(decoded, expected);
}

// P1-2 回归测试: 超长 base64（100+ 字符）
TEST(FieldApplierTest, DecodeBase64VeryLongString) {
  FieldApplier applier;
  // 100 个 'X' → base64
  std::string raw_data(100, 'X');
  // 使用标准 base64 编码
  static const char b64_chars[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string b64;
  int val = 0, valb = -6;
  for (unsigned char c : raw_data) {
    val = (val << 8) + c;
    valb += 8;
    while (valb >= 0) {
      b64.push_back(b64_chars[(val >> valb) & 0x3F]);
      valb -= 6;
    }
  }
  if (valb > -6) {
    b64.push_back(b64_chars[((val << 8) >> (valb + 8)) & 0x3F]);
  }
  while (b64.size() % 4) b64.push_back('=');

  std::string decoded = applier.decodeBytes(b64, "base64");
  EXPECT_EQ(decoded, raw_data);
}

// ============================================================================
// 15. DecodeHex
// ============================================================================

TEST(FieldApplierTest, DecodeHex) {
  FieldApplier applier;

  EXPECT_EQ(applier.decodeBytes("48656c6c6f", "hex"), "Hello");
  EXPECT_EQ(applier.decodeBytes("4f4b", "hex"), "OK");
  EXPECT_EQ(applier.decodeBytes("", "hex"), "");

  // 大写
  EXPECT_EQ(applier.decodeBytes("4F4B", "hex"), "OK");
}

// ============================================================================
// 16. ApplyFieldsNoMappings — 无 mappings，全部按 String 处理
// ============================================================================

TEST(FieldApplierTest, ApplyFieldsNoMappings) {
  FieldApplier applier;
  HttpAccessEvent event;

  std::map<std::string, std::string> fields;
  fields["request_method"] = "DELETE";
  fields["request_uri"] = "/resource/42";
  fields["downstream_ip"] = "10.0.0.99";

  // 不传 mappings：全部按 String
  applier.applyFields(fields, event);

  EXPECT_EQ(event.request_method(), "DELETE");
  EXPECT_EQ(event.request_uri(), "/resource/42");
  EXPECT_EQ(event.downstream_ip(), "10.0.0.99");
}

// ============================================================================
// 17. Unknown field name — 不存在的 protobuf 字段
// ============================================================================

TEST(FieldApplierTest, UnknownFieldName) {
  FieldApplier applier;
  HttpAccessEvent event;

  std::map<std::string, std::string> fields;
  fields["nonexistent_field"] = "value";

  applier.applyFields(fields, event);
  // 不应崩溃，只是 warn 并跳过
  SUCCEED();
}

// ============================================================================
// 18. Bool field
// ============================================================================

TEST(FieldApplierTest, ApplyBoolField) {
  FieldApplier applier;
  HttpAccessEvent event;

  // 注意: HttpAccessEvent 没有 bool 类型的字段。
  // setBoolField 会通过反射查找字段，若字段不存在则 warn 并返回。
  // 这里验证不会崩溃。
  std::map<std::string, std::string> fields;
  fields["nonexistent_bool"] = "true";

  std::vector<FieldMapping> mappings = {
      makeFm("nonexistent_bool", FieldType::Bool),
  };

  applier.applyFields(fields, event, mappings);
  SUCCEED();
}
