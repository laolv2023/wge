# WGE-Kafka 异步 WAF 检测系统 — 生产级设计方案

> 版本: v1.4 | 日期: 2026-05-27 | 作者: 小a & 老A

---

## 1. 需求摘要

| # | 需求 | 约束 |
|---|------|------|
| 1 | HTTP 完整请求+响应数据写入 Kafka 指定 Topic | Kafka 参数、Topic 可配置 |
| 2 | WGE 周期性消费该 Topic 数据进行检测 | 消费间隔可配置 |
| 3 | 检测到异常时告警输出到另一 Kafka Topic | 告警 Topic 可配置 |
| 4 | 生产级方案 | 可观测、容错、可扩展 |

---

## 2. 系统全局架构

### 2.1 架构总览

```
                          ┌──────────────────────────────────────────────────────┐
                          │               Kafka Cluster                         │
                          │                                                      │
                          │  ┌────────────────┐         ┌──────────────────┐     │
  ┌───────────┐           │  │  http-access   │         │  wge-alert       │     │
  │  Web Server│  produce │  │  (input topic) │         │  (output topic)  │     │
  │  / Agent   │ ─────────▶│  │                │         │                  │     │
  │  / SIEM    │           │  │  Partition: N  │         │  Partition: M   │     │
  └───────────┘           │  └───────┬────────┘         └───────▲──────────┘     │
                          │          │                          │                │
                          └──────────┼──────────────────────────┼────────────────┘
                                     │ consume                  │ produce
                                     │ (poll interval)          │
                          ┌──────────┼──────────────────────────┼────────────────┐
                          │          ▼                          │                │
                          │  ┌──────────────────────────────────────────┐        │
                          │  │         WGE Detector Service            │        │
                          │  │                                          │        │
                          │  │  ┌─────────────┐   ┌────────────────┐  │        │
                          │  │  │   Kafka      │   │  WGE Engine    │  │        │
                          │  │  │   Consumer   │   │  (singleton)   │  │        │
                          │  │  │   Group       │   │                │  │        │
                          │  │  └──────┬───────┘   └───────┬────────┘  │        │
                          │  │         │                    │            │        │
                          │  │         ▼                    ▼            │        │
                          │  │  ┌──────────────────────────────────┐   │        │
                          │  │  │        Worker Thread Pool        │   │        │
                          │  │  │  ┌─────┐ ┌─────┐ ┌─────┐       │   │        │
                          │  │  │  │ T-0 │ │ T-1 │ │ T-N │       │   │        │
                          │  │  │  └──┬──┘ └──┬──┘ └──┬──┘       │   │        │
                          │  │  └─────┼────────┼────────┼──────────┘   │        │
                          │  │        │        │        │               │        │
                          │  │        ▼        ▼        ▼               │        │
                          │  │  ┌──────────────────────────────────┐   │        │
                          │  │  │       Alert Aggregator           │   │        │
                          │  │  │  (batch, dedup, enrich)          │   │        │
                          │  │  └──────────────┬───────────────────┘   │        │
                          │  │                 │                         │        │
                          │  │  ┌──────────────▼───────────────────┐   │        │
                          │  │  │       Alert Kafka Producer        │───┘        │
                          │  │  └──────────────────────────────────┘            │
                          │  │                                                   │
                          │  │  ┌────────────┐ ┌──────────────┐                  │
                          │  │  │ Config Mgr │ │ Metrics/Health│                  │
                          │  │  └────────────┘ └──────────────┘                  │
                          │  └──────────────────────────────────────────────────┘
                          │
                          └──────────────────────────────────────────────────────
```

### 2.2 核心设计原则

| 原则 | 说明 |
|------|------|
| **异步解耦** | 数据采集与检测完全异步，Kafka 做缓冲，削峰填谷 |
| **零拷贝注入** | WGE `HttpExtractor` 回调机制，HTTP 数据零拷贝映射到 Transaction |
| **无锁并发** | 每个 worker 线程持有独立 Transaction，WGE Engine 只读共享 |
| **背压控制** | Consumer 拉取速率受 worker pool 处理能力反馈控制 |
| **精确一次告警** | 告警输出使用 Kafka 事务（Idempotent Producer + 事务性发送） |
| **可观测性** | Prometheus metrics 暴露全链路指标 |

---

## 3. 数据协议设计

### 3.1 HTTP 访问数据（输入 Topic）

采用 **Protobuf** 序列化，兼顾性能与 Schema 演化能力。

```protobuf
syntax = "proto3";
package wge.kafka;

// Kafka input topic: http-access
message HttpAccessEvent {
  // ---- 元信息 ----
  string  event_id       = 1;   // UUID v7, 全局唯一
  int64   timestamp_ms   = 2;   // 事件采集时间 (epoch ms)
  string  collector_id   = 3;   // 采集源标识 (主机名/实例名)

  // ---- 连接信息 ----
  string  downstream_ip   = 10;
  int32   downstream_port = 11;
  string  upstream_ip     = 12;
  int32   upstream_port   = 13;

  // ---- 请求 ----
  string  request_method  = 20;
  string  request_uri     = 21;   // 完整 URI (含 query string)
  string  request_version  = 22;  // "1.1"
  repeated Header request_headers = 23;
  bytes   request_body    = 24;   // 原始 body bytes
  int64   request_body_length = 25;

  // ---- 响应 ----
  int32   response_status  = 30;
  string  response_version = 31;
  repeated Header response_headers = 32;
  bytes   response_body    = 33;
  int64   response_body_length = 34;
}

message Header {
  string key   = 1;
  string value = 2;
}
```

> **为什么用 Protobuf 而非 JSON？**
> - WAF 场景 body 可能很大（Multipart/JSON API），Protobuf 编码体积约 JSON 的 1/3
> - Schema 演化兼容（Confluent Schema Registry 支持）
> - C++ 原生代码生成，零反射开销

### 3.3 原始日志 → HttpAccessEvent 映射配置

Kafka 输入 Topic 中的原始日志格式不可控（Nginx combined log、JSON、自定义格式等），需要**配置驱动的映射层**完成转换。

#### 3.3.1 支持的输入格式

| Parser 类型 | 适用场景 | 说明 |
|-------------|----------|------|
| `protobuf` | 上游已序列化为 `HttpAccessEvent` | 零转换，直通 |
| `json` | 上游输出结构化 JSON | JSONPath 字段提取 + 类型转换 |
| `regex` | Nginx/Apache 文本日志 | 正则分组提取字段 |
| `grok` | 兼容 Logstash/Elastic 生态 | Grok pattern 组合 |

#### 3.3.2 映射配置文件 (`log_mapping.yaml`)

```yaml
# log_mapping.yaml — 原始日志 → HttpAccessEvent 映射配置

# ===== 全局设置 =====
mapping:
  # 输入格式: protobuf | json | regex | grok
  format: "json"

  # 原始消息编码: utf8 | base64 | hex
  encoding: "utf8"

  # 当原始日志无法解析时的处理: skip | dlq
  on_parse_error: "dlq"

  # event_id 生成策略:
  #   auto    — 自动生成 UUID v7
  #   extract — 从原始日志中提取
  event_id_policy: "auto"

  # timestamp 生成策略:
  #   auto    — 使用消费时间
  #   extract — 从原始日志中提取 (需指定格式)
  timestamp_policy: "extract"

# ===== JSON 格式映射 =====
json:
  # 字段映射: source 为原始 JSON 中的路径 (支持点号分隔的嵌套路径)
  #           target 为 HttpAccessEvent Protobuf 字段名
  #           type 为目标类型: string | int32 | int64 | bytes | bool
  #           default 为缺失时的默认值 (可选)
  fields:
    - source: "request.method"
      target: "request_method"
      type: "string"
      default: "GET"

    - source: "request.uri"
      target: "request_uri"
      type: "string"
      default: "/"

    - source: "request.version"
      target: "request_version"
      type: "string"
      default: "1.1"

    - source: "request.body"
      target: "request_body"
      type: "bytes"            # base64 字符串自动解码为 bytes

    - source: "request.body.length"
      target: "request_body_length"
      type: "int64"
      default: 0

    - source: "response.status"
      target: "response_status"
      type: "int32"
      default: 0

    - source: "response.version"
      target: "response_version"
      type: "string"
      default: "1.1"

    - source: "response.body"
      target: "response_body"
      type: "bytes"

    - source: "response.body.length"
      target: "response_body_length"
      type: "int64"
      default: 0

    - source: "connection.source_ip"
      target: "downstream_ip"
      type: "string"
      default: "0.0.0.0"

    - source: "connection.source_port"
      target: "downstream_port"
      type: "int32"
      default: 0

    - source: "connection.dest_ip"
      target: "upstream_ip"
      type: "string"
      default: "0.0.0.0"

    - source: "connection.dest_port"
      target: "upstream_port"
      type: "int32"
      default: 0

    - source: "timestamp"
      target: "timestamp_ms"
      type: "int64"

    - source: "collector"
      target: "collector_id"
      type: "string"
      default: "unknown"

  # Header 提取: 从原始 JSON 中提取 HTTP Headers
  # 模式: embedded (headers 已是 key-value 数组) | prefix (headers 共享前缀)
  header_mode: "embedded"

  headers:
    # embedded 模式: 原始 JSON 中 headers 已是结构化数组
    request:
      source: "request.headers"        # JSON 路径, 值为 [{"key":"Content-Type","value":"text/html"}, ...]
      key_field: "key"
      value_field: "value"
    response:
      source: "response.headers"
      key_field: "key"
      value_field: "value"

  # prefix 模式 (备选): headers 散落在字段前缀下
  #   request_prefix: "request.header."
  #   response_prefix: "response.header."

# ===== Regex 格式映射 (Nginx combined log 示例) =====
# 当 mapping.format 设为 "regex" 时使用此段
regex:
  # 正则表达式, 命名分组对应 HttpAccessEvent 字段
  # 注意: request_version 捕获的是 "HTTP/1.1" 格式, 映射时需 strip "HTTP/" 前缀
  pattern: '^(?P<downstream_ip>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] "(?P<request_method>\S+) (?P<request_uri>\S+) (?P<request_version>[^"]+)" (?P<response_status>\d+) (?P<response_body_length>\d+) "(?P<referer>[^"]*)" "(?P<user_agent>[^"]*)"'

  # 分组名 → Protobuf 字段映射 (仅列出需要映射的)
  fields:
    - source: "downstream_ip"
      target: "downstream_ip"
      type: "string"

    - source: "request_method"
      target: "request_method"
      type: "string"

    - source: "request_uri"
      target: "request_uri"
      type: "string"

    - source: "request_version"
      target: "request_version"
      type: "string"

    - source: "response_status"
      target: "response_status"
      type: "int32"

    - source: "response_body_length"
      target: "response_body_length"
      type: "int64"

    - source: "user_agent"
      target: "request_headers"
      type: "header"            # 特殊类型: 自动包装为 Header{"User-Agent", value}

  # timestamp 解析格式
  timestamp_format: "%d/%b/%Y:%H:%M:%S %z"

  # Regex 模式下 Header 的特殊映射规则
  header_mappings:
    - source: "user_agent"
      header_name: "User-Agent"
      direction: "request"      # request | response
    - source: "referer"
      header_name: "Referer"
      direction: "request"

# ===== Grok 格式映射 (兼容 Logstash) =====
# 当 mapping.format 设为 "grok" 时使用此段
grok:
  # Grok pattern (使用 %{PATTERN:name} 语法)
  pattern: '%{IP:downstream_ip} %{WORD:ident} %{USER:auth} \[%{HTTPDATE:timestamp}\] "%{WORD:request_method} %{URIPATHPARAM:request_uri} HTTP/%{NUMBER:request_version}" %{INT:response_status} %{INT:response_body_length}'

  # 内置 Grok pattern 定义 (可扩展)
  patterns_dir: "/etc/wge/grok_patterns/"

  fields:
    - source: "downstream_ip"
      target: "downstream_ip"
      type: "string"
    # ... 同 regex 的 fields 格式

  timestamp_format: "%d/%b/%Y:%H:%M:%S %z"

# ===== 类型转换规则 =====
conversions:
  # timestamp 字符串 → epoch ms 的格式定义
  timestamp_formats:
    - "%Y-%m-%dT%H:%M:%S%.3fZ"        # ISO 8601 (e.g., 2026-05-27T10:30:00.000Z)
    - "%d/%b/%Y:%H:%M:%S %z"          # Nginx (e.g., 27/May/2026:10:30:00 +0800)
    - "%Y-%m-%d %H:%M:%S"             # 简单格式
    - "%s"                             # Unix epoch seconds
    - "%s%3N"                          # Unix epoch milliseconds

  # bytes 字段的编码来源: base64 | hex | raw
  bytes_encoding:
    request_body: "base64"            # JSON 中 body 通常是 base64 编码
    response_body: "base64"

# ===== 常量注入 (每条事件都附加的固定值) =====
constants:
  collector_id: "nginx-prod-01"        # 可被字段映射覆盖
  downstream_port: 443                 # 如果原始日志不含端口号
```

#### 3.3.3 映射引擎 C++ 实现

```cpp
// mapper/mapper.h

// ---- 字段映射规则 ----
struct FieldMapping {
    std::string source;            // 原始字段路径 / 正则分组名
    std::string target;            // Protobuf 字段名
    enum class Type : uint8_t {
        String, Int32, Int64, Bytes, Bool, Header
    } type = Type::String;
    std::string default_value;     // 缺失时的默认值
    std::string header_name;       // type=Header 时的 header key
    std::string header_direction;  // "request" | "response"
};

// ---- Header 提取配置 ----
struct HeaderMapping {
    std::string source;            // JSON 路径 (embedded 模式)
    std::string key_field;         // key 字段名
    std::string value_field;       // value 字段名
};

struct HeaderConfig {
    enum class Mode : uint8_t { Embedded, Prefix } mode = Mode::Embedded;
    HeaderMapping request;
    HeaderMapping response;
    std::string request_prefix;    // Prefix 模式下的前缀
    std::string response_prefix;
};

// ---- 映射配置 ----
struct MapperConfig {
    enum class Format : uint8_t { Protobuf, Json, Regex, Grok } format;
    std::string encoding{"utf8"};
    std::string on_parse_error{"dlq"};    // skip | dlq
    std::string event_id_policy{"auto"};   // auto | extract
    std::string timestamp_policy{"extract"};

    // JSON 配置
    std::vector<FieldMapping> json_fields;
    HeaderConfig json_headers;

    // Regex 配置
    std::string regex_pattern;
    std::vector<FieldMapping> regex_fields;
    std::string regex_timestamp_format;

    // Grok 配置
    std::string grok_pattern;
    std::string grok_patterns_dir;
    std::vector<FieldMapping> grok_fields;

    // Header 特殊映射 (regex/grok 模式)
    std::vector<FieldMapping> header_mappings;

    // 类型转换
    std::vector<std::string> timestamp_formats;
    std::map<std::string, std::string> bytes_encoding;  // field → base64/hex/raw

    // 常量注入
    std::map<std::string, std::string> constants;
};

// ---- 映射引擎 ----
class LogMapper {
public:
    explicit LogMapper(const MapperConfig& config);

    // 核心方法: 原始 Kafka 消息 → HttpAccessEvent
    // 成功返回解析后的事件, 失败返回错误信息
    std::expected<std::shared_ptr<HttpAccessEvent>, std::string>
    map(std::string_view raw_payload) const;

private:
    // 按 format 分发
    std::expected<std::shared_ptr<HttpAccessEvent>, std::string>
    mapProtobuf(std::string_view raw) const;

    std::expected<std::shared_ptr<HttpAccessEvent>, std::string>
    mapJson(std::string_view raw) const;

    std::expected<std::shared_ptr<HttpAccessEvent>, std::string>
    mapRegex(std::string_view raw) const;

    std::expected<std::shared_ptr<HttpAccessEvent>, std::string>
    mapGrok(std::string_view raw) const;

    // 通用: 将提取的 field_map → Protobuf 字段赋值
    void applyFields(
        const std::map<std::string, std::string>& extracted,
        HttpAccessEvent& event) const;

    // 通用: timestamp 字符串 → epoch ms
    int64_t parseTimestamp(const std::string& ts_str) const;

    // 通用: bytes 解码
    std::string decodeBytes(const std::string& raw, const std::string& encoding) const;

    MapperConfig config_;
    // Regex 编译缓存
    mutable std::unique_ptr<re2::RE2> compiled_regex_;
    // Grok pattern 编译缓存
    mutable std::unique_ptr<GrokCompiler> compiled_grok_;
};
```

#### 3.3.4 JSON 映射核心逻辑

```cpp
// mapper/json_mapper.cc

std::expected<std::shared_ptr<HttpAccessEvent>, std::string>
LogMapper::mapJson(std::string_view raw) const {
    // 1. JSON 解析
    auto json = simdjson::dom::parser().parse(
        reinterpret_cast<const uint8_t*>(raw.data()), raw.size());
    if (json.error()) {
        return std::unexpected("JSON parse error: " +
                               std::string(simdjson::error_message(json.error())));
    }

    // 2. 提取字段到 string map
    std::map<std::string, std::string> extracted;
    for (const auto& field : config_.json_fields) {
        // 支持点号分隔的嵌套路径: "request.method" → json["request"]["method"]
        std::string value = extractJsonPath(json.value_unsafe(), field.source);
        if (value.empty() && !field.default_value.empty()) {
            value = field.default_value;
        }
        if (!value.empty()) {
            extracted[field.source] = std::move(value);
        }
    }

    // 3. 构建 HttpAccessEvent
    auto event = std::make_shared<HttpAccessEvent>();
    applyFields(extracted, *event);

    // 4. 提取 Headers
    if (config_.json_headers.mode == HeaderConfig::Mode::Embedded) {
        extractEmbeddedHeaders(json.value_unsafe(),
                               config_.json_headers.request,
                               *event->mutable_request_headers());
        extractEmbeddedHeaders(json.value_unsafe(),
                               config_.json_headers.response,
                               *event->mutable_response_headers());
    } else {
        extractPrefixHeaders(json.value_unsafe(),
                             config_.json_headers.request_prefix,
                             *event->mutable_request_headers());
        extractPrefixHeaders(json.value_unsafe(),
                             config_.json_headers.response_prefix,
                             *event->mutable_response_headers());
    }

    // 5. 注入常量
    for (const auto& [key, value] : config_.constants) {
        applyConstant(*event, key, value);
    }

    // 6. 生成 event_id
    if (config_.event_id_policy == "auto") {
        event->set_event_id(generateUUIDv7());
    }

    // 7. 处理 timestamp
    if (config_.timestamp_policy == "auto") {
        event->set_timestamp_ms(currentEpochMs());
    }

    return event;
}

void LogMapper::applyFields(
    const std::map<std::string, std::string>& extracted,
    HttpAccessEvent& event) const {

    // 按配置中的字段列表逐一赋值
    const auto& fields = (config_.format == MapperConfig::Format::Json)
                         ? config_.json_fields
                         : (config_.format == MapperConfig::Format::Regex)
                           ? config_.regex_fields
                           : config_.grok_fields;

    for (const auto& field : fields) {
        auto it = extracted.find(field.source);
        if (it == extracted.end()) continue;

        const std::string& value = it->second;

        // 根据类型分发
        switch (field.type) {
        case FieldMapping::Type::String:
            setStringField(event, field.target, value);
            break;
        case FieldMapping::Type::Int32:
            setInt32Field(event, field.target, value);
            break;
        case FieldMapping::Type::Int64:
            setInt64Field(event, field.target, value);
            break;
        case FieldMapping::Type::Bytes: {
            auto encoding = config_.bytes_encoding.count(field.target)
                            ? config_.bytes_encoding.at(field.target) : "raw";
            setBytesField(event, field.target, decodeBytes(value, encoding));
            break;
        }
        case FieldMapping::Type::Bool:
            setBoolField(event, field.target, value);
            break;
        case FieldMapping::Type::Header: {
            auto* headers = (field.header_direction == "request")
                            ? event.mutable_request_headers()
                            : event.mutable_response_headers();
            auto* h = headers->Add();
            h->set_key(field.header_name);
            h->set_value(value);
            break;
        }
        }
    }
}
```

#### 3.3.5 Regex 映射核心逻辑

```cpp
// mapper/regex_mapper.cc

std::expected<std::shared_ptr<HttpAccessEvent>, std::string>
LogMapper::mapRegex(std::string_view raw) const {
    // 1. 正则匹配
    if (!compiled_regex_ || !compiled_regex_->ok()) {
        return std::unexpected("Regex not compiled or invalid");
    }

    // 2. 提取命名分组
    int num_groups = compiled_regex_->NumberOfCapturingGroups();
    std::vector<re2::StringPiece> groups(num_groups);
    std::vector<std::string> group_names(num_groups);

    // 获取分组名
    for (int i = 0; i < num_groups; ++i) {
        group_names[i] = compiled_regex_->group_name(i + 1);
    }

    if (!re2::RE2::FullMatchN(raw, *compiled_regex_,
                               groups.data(), num_groups)) {
        return std::unexpected("Regex match failed");
    }

    // 3. 分组名 → string map
    std::map<std::string, std::string> extracted;
    for (int i = 0; i < num_groups; ++i) {
        if (!group_names[i].empty() && !groups[i].empty()) {
            extracted[group_names[i]] = std::string(groups[i]);
        }
    }

    // 4. 构建 HttpAccessEvent (复用通用 applyFields)
    auto event = std::make_shared<HttpAccessEvent>();
    applyFields(extracted, *event);

    // 5. 处理 Header 特殊映射
    for (const auto& hm : config_.header_mappings) {
        auto it = extracted.find(hm.source);
        if (it != extracted.end()) {
            auto* headers = (hm.header_direction == "request")
                            ? event->mutable_request_headers()
                            : event->mutable_response_headers();
            auto* h = headers->Add();
            h->set_key(hm.header_name);
            h->set_value(it->second);
        }
    }

    // 6. Timestamp 解析
    if (config_.timestamp_policy == "extract") {
        auto ts_it = extracted.find("timestamp");
        if (ts_it != extracted.end()) {
            event->set_timestamp_ms(parseTimestamp(ts_it->second));
        }
    }

    // 7. 注入常量 + event_id
    for (const auto& [key, value] : config_.constants) {
        applyConstant(*event, key, value);
    }
    if (config_.event_id_policy == "auto") {
        event->set_event_id(generateUUIDv7());
    }

    return event;
}
```

#### 3.3.6 映射流程集成到 Consumer

```
Kafka Raw Message
       │
       ▼
┌──────────────────┐
│  LogMapper.map() │
│                  │
│  format=protobuf │ ──▶ Protobuf::Parse()  ──▶ HttpAccessEvent
│  format=json     │ ──▶ simdjson::parse()   ──▶ field extraction ──▶ HttpAccessEvent
│  format=regex    │ ──▶ RE2::FullMatchN()   ──▶ named groups   ──▶ HttpAccessEvent
│  format=grok     │ ──▶ GrokCompiler::match()──▶ named groups   ──▶ HttpAccessEvent
│                  │
│  失败 ──▶ on_parse_error=dlq  ──▶ 投递 DLQ Topic
│        ──▶ on_parse_error=skip ──▶ 跳过, 记录错误
└──────────────────┘
       │
       ▼
  shared_ptr<HttpAccessEvent>  ──▶  Worker Pool  ──▶  WGE Detection
```

Consumer 线程中的调用：

```cpp
// 在 Consumer 的 on_batch 回调中
// 注意: RdKafka::Message* 生命周期仅限回调内, 必须在回调返回前完成 mapping
std::vector<std::shared_ptr<HttpAccessEvent>> batch;
batch.reserve(messages.size());

for (const auto* msg : messages) {
    std::string_view payload(reinterpret_cast<const char*>(msg->payload()),
                              msg->len());
    auto result = mapper_.map(payload);
    if (!result.has_value()) {
        spdlog::warn("log mapping failed: {}", result.error());
        if (config_.on_parse_error == "dlq") {
            dlq_producer_->send(msg);        // 原始消息进 DLQ
        }
        metrics_.events_dropped_->Increment();
        continue;
    }
    batch.push_back(std::move(result.value()));
}

// 批量入队 (背压: 队列满时阻塞等待)
int enqueued = worker_pool_.submitBatch(std::move(batch));
spdlog::debug("batch: {} mapped, {} enqueued", messages.size(), enqueued);
```

### 3.2 告警数据（输出 Topic）

```protobuf
syntax = "proto3";
package wge.kafka;

// Kafka output topic: wge-alert
message WgeAlertEvent {
  string  alert_id        = 1;   // UUID v7
  int64   timestamp_ms    = 2;   // 检测时间

  // ---- 关联 ----
  string  event_id        = 10;  // 关联原始 HttpAccessEvent.event_id
  string  collector_id    = 11;  // 采集源

  // ---- 检测结果 ----
  bool    intervened      = 20;  // 是否触发拦截动作
  string  disruptive_action = 21; // DENY/DROP/REDIRECT/ALLOW 等
  int32   response_code   = 22;  // 规则设定的 HTTP 状态码
  string  redirect_url    = 23;  // REDIRECT 目标 URL

  // ---- 匹配规则 ----
  repeated MatchedRule matched_rules = 30;

  // ---- 原始请求摘要 (用于告警展示，不含 body) ----
  string  request_method  = 40;
  string  request_uri     = 41;
  string  downstream_ip   = 42;
  string  upstream_ip     = 43;
}

message MatchedRule {
  uint64  rule_id         = 1;   // SecRule id
  string  rule_msg        = 2;
  string  rule_severity   = 3;  // CRITICAL/ERROR/WARNING/NOTICE
  string  rule_ver        = 4;  // 规则集版本
  repeated string rule_tags = 5;

  // 匹配变量
  string  matched_var_name   = 10;
  string  matched_var_value   = 11;  // 变换后的值
  string  matched_var_original = 12; // 原始值

  // 匹配操作
  string  operator_name   = 20;  // @rx / @detectSQLi / @xss 等
  string  operator_param  = 21;  // 正则表达式等
}
```

---

## 4. 配置体系设计

### 4.1 配置文件格式 (YAML)

```yaml
# wge-detector.yaml

# ===== Kafka 配置 =====
kafka:
  # Consumer 配置
  consumer:
    bootstrap_servers: "kafka-1:9092,kafka-2:9092,kafka-3:9092"
    group_id: "wge-detector-group"
    topic: "http-access"                    # [可配置] 输入 Topic
    dlq_topic: "http-access-dlq"            # [可配置] 死信队列 Topic
    partition_assignment_strategy: "cooperative-sticky"
    auto_offset_reset: "latest"             # earliest | latest (生产默认 latest, 避免重启回放)
    enable_auto_commit: false               # 手动提交，确保 exactly-once
    session_timeout_ms: 30000
    max_poll_records: 500                   # 单次 poll 最大记录数
    max_poll_interval_ms: 300000
    fetch_max_bytes: 1048576               # 1MB per fetch
    security_protocol: "SASL_PLAINTEXT"     # SASL_SSL | SASL_PLAINTEXT | PLAINTEXT
    sasl_mechanism: "SCRAM-SHA-512"
    sasl_username: "${KAFKA_SASL_USERNAME}"
    sasl_password: "${KAFKA_SASL_PASSWORD}"

  # Producer 配置 (告警输出)
  producer:
    bootstrap_servers: "kafka-1:9092,kafka-2:9092,kafka-3:9092"
    topic: "wge-alert"                      # [可配置] 告警 Topic
    acks: "all"                             # 确保所有 ISR 确认
    enable_idempotence: true               # 幂等 Producer
    compression_type: "lz4"
    batch_size: 65536
    linger_ms: 20
    retries: 3
    max_in_flight_requests_per_connection: 5  # enable_idempotence 下可安全设 5
    transactional_id: "wge-alert-txn-${INSTANCE_ID}"  # 事务性 Producer, 多实例时 ID 必须唯一
    security_protocol: "SASL_PLAINTEXT"
    sasl_mechanism: "SCRAM-SHA-512"
    sasl_username: "${KAFKA_SASL_USERNAME}"
    sasl_password: "${KAFKA_SASL_PASSWORD}"

# ===== WGE 引擎配置 =====
wge:
  rule_files:
    - "/etc/wge/rules/crs-setup.conf"
    - "/etc/wge/rules/crs/rules/*.conf"
  log_level: "warn"                        # trace|debug|info|warn|error|critical|off
  log_file: "/var/log/wge/engine.log"
  property_store_json: "/etc/wge/property_store.json"  # 可选

# ===== 日志映射配置 =====
mapping:
  config_file: "/etc/wge/log_mapping.yaml" # 映射规则配置文件

# ===== 检测调度配置 =====
detector:
  poll_interval_ms: 100                    # [可配置] Kafka 消费轮询间隔 (ms)
  worker_threads: 8                        # WGE 检测工作线程数
  batch_size: 100                          # 单批次处理事件数上限
  alert_batch_size: 50                     # 告警批量发送大小
  alert_linger_ms: 50                      # 告警批量发送等待时间

  # 背压与限流
  max_pending_tasks: 2000                   # Worker 线程池最大待处理任务
  task_timeout_ms: 5000                     # 单任务检测超时 (超时放弃进 DLQ)
  slow_consumer_threshold_ms: 5000         # 处理延迟告警阈值

  # 健康检查
  health_check_port: 9100
  readiness_threshold_lag: 10000           # Consumer lag > 此值则标记 not-ready

# ===== 可观测性 =====
observability:
  prometheus:
    enabled: true
    port: 9101
    path: "/metrics"
  opentelemetry:
    enabled: false
    endpoint: "http://otel-collector:4317"
```

### 4.2 配置热更新

支持运行时通过 `SIGHUP` 或 HTTP API 触发配置重载：

| 配置项 | 是否支持热加载 | 说明 |
|--------|---------------|------|
| `wge.rule_files` | ✅ | 调用 `Engine::loadFromFile()` + `Engine::init()` |
| `wge.property_store_json` | ✅ | 调用 `Engine::updatePropertyStore()` |
| `kafka.consumer.max_poll_records` | ✅ | 下一轮 poll 生效 |
| `detector.worker_threads` | ❌ | 需重启 |
| `detector.poll_interval_ms` | ✅ | 下一轮 poll 生效 |
| Kafka 连接参数 | ❌ | 需重启 |

---

## 5. 核心模块详细设计

### 5.1 进程模型

```
┌─────────────────────────────────────────────────────────────┐
│  wge-detector (单进程, 多线程)                               │
│                                                              │
│  Main Thread ────────────────────────────────────────────     │
│  │  • Signal handler (SIGTERM/SIGINT/SIGHUP)                │
│  │  • Config loading & hot-reload                            │
│  │  • WGE Engine init (主线程约束)                           │
│  │  • Health / Metrics HTTP server                           │
│  │                                                           │
│  Consumer Thread ───────────────────────────────────────     │
│  │  • Kafka RdKafka consumer poll loop                       │
│  │  • Deserialization (Protobuf → HttpAccessEvent)          │
│  │  • Push to bounded task queue                             │
│  │  • Manual offset commit (after processing ack)           │
│  │                                                           │
│  Worker Thread Pool (N threads) ────────────────────────     │
│  │  • Pop task from queue                                    │
│  │  • Create WGE Transaction per task                       │
│  │  • Map HttpAccessEvent → HttpExtractor (shared_ptr)      │
│  │  • Run 5-phase detection (per-task timeout)              │
│  │  • Collect intervention + matched rules via callback     │
│  │  • Push alert to alert queue                              │
│  │                                                           │
│  Alert Producer Thread ─────────────────────────────────     │
│     • Batch collect alerts from alert queue                  │
│     • Serialize WgeAlertEvent (Protobuf)                     │
│     • Kafka transactional produce                            │
│     • send_offsets_to_transaction() + commit_transaction()  │
│                                                              │
│  ─── 优雅关闭序列 (SIGTERM) ───                              │
│  1. Consumer 停止 poll, 不再拉取新消息                       │
│  2. Worker pool 等待所有 in-flight 任务完成 (最长 task_timeout)│
│  3. Alert Producer flush 所有待发送告警                       │
│  4. 提交最后一个事务 (含最终 offset)                          │
│  5. 关闭 Kafka 连接, 退出                                    │
└─────────────────────────────────────────────────────────────┘
```

### 5.2 Kafka Consumer 模块

```cpp
// kafka_consumer.h
class KafkaConsumer {
public:
    struct Config {
        std::string bootstrap_servers;
        std::string group_id;
        std::string topic;
        std::string dlq_topic;
        int32_t     max_poll_records{500};
        int32_t     poll_interval_ms{100};
        int64_t     session_timeout_ms{30000};
        bool        enable_auto_commit{false};
        // SASL/SSL params...
    };

    explicit KafkaConsumer(const Config& cfg);
    ~KafkaConsumer();

    // 启动消费循环 (阻塞, 在独立线程运行)
    void start(std::function<void(std::vector<RdKafka::Message*>)> on_batch);

    // 优雅停止
    void stop();

    // 获取 Consumer Group 元数据 (CTP 模式下 Producer 需要此信息)
    RdKafka::ConsumerGroupMetadata* groupMetadata() const;

    // 监控指标
    int64_t consumerLag() const;
    int64_t committedOffset() const;

private:
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
    std::atomic<bool> running_{false};
};
```

**关键设计决策：**

| 决策 | 理由 |
|------|------|
| **手动 offset 提交** | 确保消息处理 + 告警发送都完成后才提交，避免丢告警 |
| **批量 poll** | `max_poll_records=500`，减少网络往返，提高吞吐 |
| **cooperative-sticky 分配** | 避免 Rebalance 时全量 revoke，减少消费中断 |
| **Consumer 单线程** | RdKafka 不是线程安全的 Consumer，单线程 poll + 批量分发 |

### 5.3 WGE Worker 模块

```cpp
// detector/result.h — 检测结果结构体
struct MatchedRuleInfo {
    int         rule_id{0};
    std::string rule_msg;
    std::string severity;           // EMERGENCY/ALERT/CRITICAL/ERROR/WARNING/NOTICE/INFO/DEBUG
    std::string rule_ver;
    std::vector<std::string> rule_tags;

    std::string matched_var_name;
    std::string matched_var_value;    // 变换后的值
    std::string matched_var_original; // 原始值

    std::string operator_name;        // @rx / @detectSQLi / @xss 等
    std::string operator_param;        // 正则表达式等
};

struct AlertResult {
    std::string event_id;
    int64_t     timestamp_ms{0};
    bool        intervened{false};

    // Disruptive action: 从 LogCallback 收集的 Rule::detail_->disruptive_ 推断
    std::string disruptive_action;     // DENY/DROP/REDIRECT/ALLOW/ALLOW_PHASE/ALLOW_REQUEST/BLOCK
    int         response_code{403};     // 从 Rule::detail_->status_ 获取, 默认 "403"
    std::string redirect_url;          // REDIRECT 时的目标 URL (Rule::detail_->redirect_)

    std::vector<MatchedRuleInfo> matched_rules;
};

// wge_worker.h
class WgeWorker {
public:
    struct Config {
        int worker_threads{8};
        int max_pending_tasks{2000};
        int task_timeout_ms{5000};
    };

    WgeWorker(const Wge::Engine& engine, AlertProducer& alert_producer,
              const Config& cfg);

    // 批量提交检测任务 (由 Consumer 线程调用, 线程安全)
    // 使用有界队列, 满时阻塞等待 (背压控制)
    int submitBatch(std::vector<std::shared_ptr<HttpAccessEvent>>&& events);

    // 启动/停止 worker pool
    void start();
    void stop();

    // 监控
    size_t pendingCount() const;
    size_t processedCount() const;
    double avgLatencyMs() const;

private:
    void workerLoop(int thread_id);

    const Wge::Engine& engine_;
    AlertProducer& alert_producer_;       // Worker 直接调 Producer 发告警, 不维护独立队列
    Config cfg_;

    // 有界任务队列
    std::mutex queue_mutex_;
    std::condition_variable queue_not_empty_;
    std::condition_variable queue_not_full_;
    std::deque<std::shared_ptr<HttpAccessEvent>> task_queue_;

    // Worker 检测完成后，直接通过 AlertProducer 发送告警 (线程安全)
    // 不在 WgeWorker 内维护告警队列，避免队列不一致

    std::vector<std::thread> workers_;
    std::atomic<bool> running_{false};

    // 指标
    std::atomic<uint64_t> processed_{0};
    std::atomic<uint64_t> total_latency_us_{0};
};
```

> **设计变更说明：**
> - **`std::future` → 有界队列 + condvar**：`std::future<AlertResult>` 每条消息一次堆分配，万级 QPS 下开销显著。改为批量入队 `submitBatch()`，Worker 批量出队处理
> - **`boost::lockfree::queue` → mutex + deque**：`shared_ptr<HttpAccessEvent>` / `shared_ptr<WgeAlertEvent>` 含引用计数，非 trivially copyable，不能直接放入 lockfree queue。改用 mutex + deque，在 batch 场景下锁竞争极低（Consumer 一次入队 N 条，Worker 一次出队 N 条）

### 5.4 HTTP 数据映射 (HttpExtractor 回调适配)

这是 **最关键** 的集成点。WGE 的 `processRequestHeaders()` 和 `processResponseHeaders()` 不通过 setter 注入 HttpExtractor，而是**直接接收回调参数**（`HeaderFind`、`HeaderTraversal`、`size_t header_count`）。适配器需提供这些独立参数。

> **WGE 实际 API 签名**（transaction.h / transaction.cc）：
> ```cpp
> bool processRequestHeaders(
>     HeaderFind request_header_find,
>     HeaderTraversal request_header_traversal,
>     size_t request_header_count,
>     LogCallback log_callback = nullptr,
>     void* log_user_data = nullptr,
>     AdditionalCondCallback additional_cond = nullptr,
>     void* additional_cond_user_data = nullptr);
>
> bool processResponseHeaders(
>     std::string_view status_code,
>     std::string_view protocol,
>     HeaderFind response_header_find,
>     HeaderTraversal response_header_traversal,
>     size_t response_header_count,
>     LogCallback log_callback = nullptr,
>     void* log_user_data = nullptr,
>     AdditionalCondCallback additional_cond = nullptr,
>     void* additional_cond_user_data = nullptr);
> ```

```cpp
// http_extractor_adapter.h
class HttpExtractorAdapter {
public:
    // 从 Protobuf HttpAccessEvent 构建回调参数
    // 使用 shared_ptr 持有 event, 保证 Lambda 回调期间 event 存活
    explicit HttpExtractorAdapter(std::shared_ptr<HttpAccessEvent> event)
        : event_(std::move(event)) {
        // 预构建 Header 索引: O(n) 构建, O(1) 查找
        // WAF 规则频繁查找 Content-Type / Authorization 等 header
        for (const auto& h : event_->request_headers()) {
            request_header_index_[toLower(h.key())].emplace_back(h.value());
        }
        for (const auto& h : event_->response_headers()) {
            response_header_index_[toLower(h.key())].emplace_back(h.value());
        }
    }

    // ===== Request Header 回调 (传给 processRequestHeaders) =====

    Wge::HeaderFind requestHeaderFind() const {
        // 捕获 this 指针, 回调期间 adapter 必须存活
        return [this](const std::string& lower_key)
            -> std::vector<std::string_view> {
            auto it = request_header_index_.find(lower_key);
            if (it == request_header_index_.end()) return {};
            std::vector<std::string_view> views;
            views.reserve(it->second.size());
            for (const auto& v : it->second) {
                views.emplace_back(v);
            }
            return views;
        };
    }

    Wge::HeaderTraversal requestHeaderTraversal() const {
        return [this](auto callback) {
            for (const auto& [key, values] : request_header_index_) {
                for (const auto& value : values) {
                    if (!callback(key, std::string_view(value))) {
                        return;  // callback 返回 false 停止遍历
                    }
                }
            }
        };
    }

    size_t requestHeaderCount() const {
        return event_->request_headers_size();
    }

    // ===== Response Header 回调 (传给 processResponseHeaders) =====

    Wge::HeaderFind responseHeaderFind() const {
        return [this](const std::string& lower_key)
            -> std::vector<std::string_view> {
            auto it = response_header_index_.find(lower_key);
            if (it == response_header_index_.end()) return {};
            std::vector<std::string_view> views;
            views.reserve(it->second.size());
            for (const auto& v : it->second) {
                views.emplace_back(v);
            }
            return views;
        };
    }

    Wge::HeaderTraversal responseHeaderTraversal() const {
        return [this](auto callback) {
            for (const auto& [key, values] : response_header_index_) {
                for (const auto& value : values) {
                    if (!callback(key, std::string_view(value))) {
                        return;
                    }
                }
            }
        };
    }

    size_t responseHeaderCount() const {
        return event_->response_headers_size();
    }

    // ===== Response Line 信息 =====

    std::string_view responseStatusCode() const {
        // WGE 接收 string_view 格式的 status code
        // 缓存到 string_pool_ 保证生命周期
        if (cached_status_.empty() && event_->response_status() != 0) {
            cached_status_ = std::to_string(event_->response_status());
        }
        return cached_status_;
    }

    std::string_view responseProtocol() const {
        // 构造 "HTTP/1.1" 格式, 缓存到 string_pool_
        if (cached_protocol_.empty()) {
            cached_protocol_ = "HTTP/" + event_->response_version();
        }
        return cached_protocol_;
    }

    // ===== 生命周期保证 =====
    // event_ 通过 shared_ptr 持有, string_view 回调指向的内存:
    //   - protobuf string 字段: 由 event_ 生命周期保证
    //   - header_index_ 中的 string: 由 event_ 中的 protobuf 字段保证
    //   - cached_status_/cached_protocol_: 由 adapter 自身持有, adapter 生命周期必须覆盖 Transaction 全程

private:
    std::shared_ptr<HttpAccessEvent> event_;

    // Header 索引: lower_key → values (O(1) 查找)
    // 注意: unordered_map 在构造函数中一次性构建完毕后, 后续仅被 Lambda 回调读取
    //       不存在写操作, 因此迭代器稳定, string_view 不会因 rehash 失效
    std::unordered_map<std::string, std::vector<std::string>> request_header_index_;
    std::unordered_map<std::string, std::vector<std::string>> response_header_index_;

    // 缓存 response line 字符串 (需跨回调存活)
    std::string cached_status_;
    std::string cached_protocol_;
};
```

> **设计决策变更：为什么不再用 `fromEvent()` 返回 `HttpExtractor`？**
> - WGE 没有 `setHttpExtractor()` 方法，`HttpExtractor` 的字段是在 `processRequestHeaders()` / `processResponseHeaders()` 内部赋值的
> - 适配器需作为**独立对象存活**（持有索引、缓存字符串、shared_ptr），在 detect() 函数内与 Transaction 同生命周期
> - Header 查找从 O(n) 线性扫描 → O(1) `unordered_map` 查找，WAF 规则频繁查找 Content-Type 等特定 header

### 5.5 Transaction 执行流程

按 WGE 实际 API 重写。核心变更：
- **不调用** `setHttpExtractor()` —— 不存在此方法
- **不调用** `onRuleMatch()` —— 不存在此方法，用 `LogCallback` 函数指针 + `void* user_data` 收集匹配信息
- **processUri** 使用三参数版本：`processUri(uri, method, version)`
- **processRequestHeaders / processResponseHeaders** 直接传入 HeaderFind/HeaderTraversal/header_count
- **processResponseHeaders** 额外需 `status_code` 和 `protocol` 参数
- **intervention 判断**：各 `process*()` 返回 `bool`，`false` = 拦截（DENY/DROP）
- **Disruptive action / status code**：从 `LogCallback` 中收到的 `Rule` 对象的 `detail_` 字段获取

```cpp
// detector_core.cc — Worker 线程中的核心检测逻辑

// ===== LogCallback 用户数据结构 =====
// LogCallback 是 C 函数指针: void(*)(const Rule&, void*)
// 不能捕获 Lambda 上下文，通过 void* 传入用户数据
struct LogCallbackUserData {
    std::vector<MatchedRuleInfo>* matched_rules;
    bool* has_disruptive;
    Rule::Disruptive* last_disruptive;   // 最近一条 Disruptive Action
    std::string* last_status_code;       // 最近一条规则设定的 HTTP status
    std::string* last_redirect;          // redirect 目标 URL
    const Transaction* transaction;       // 用于获取 matched_variables_
};

// ===== LogCallback 实现 =====
// 注意: WGE 的 LogCallback 类型为 void(*)(const Rule&, void*)
//       必须定义为命名空间内的自由函数 (非 Lambda, 非 extern "C")
void wgeLogCallback(const Wge::Rule& rule, void* user_data) {
    auto* ud = static_cast<LogCallbackUserData*>(user_data);

    MatchedRuleInfo info;
    info.rule_id = rule.detail_->id_;
    info.rule_msg = std::string(rule.detail_->msg_);
    info.severity = severityToString(rule.detail_->severity_);
    info.rule_ver = std::string(rule.detail_->ver_);
    for (const auto& tag : rule.detail_->tags_) {
        info.rule_tags.emplace_back(tag);
    }
    info.operator_name = rule.detail_->operator_->name();
    info.operator_param = rule.detail_->operator_->param();

    // 从 Transaction 获取当前匹配变量
    // 注意: WGE 的 matched_variables_ 是 private 成员, 需要 WGE 提供 public getter
    //       或通过 friend class 声明访问。此处假设已有 getMatchedVariablesForRule() 接口
    if (ud->transaction) {
        const auto& [chain_idx, matched_vars] =
            ud->transaction->getCurrentMatchedVariables();
        for (const auto& mv : matched_vars) {
            info.matched_var_name = mv.variable_->name();
            info.matched_var_value = std::string(mv.transformed_value_.view);
            info.matched_var_original = std::string(mv.original_value_.view);
        }
    }

    ud->matched_rules->push_back(std::move(info));

    // 记录 Disruptive Action
    if (rule.detail_->disruptive_ != Rule::Disruptive::PASS &&
        rule.detail_->disruptive_ != Rule::Disruptive::BLOCK) {
        *ud->has_disruptive = true;
        *ud->last_disruptive = rule.detail_->disruptive_;
        *ud->last_status_code = std::string(rule.detail_->status_);
        if (rule.detail_->disruptive_ == Rule::Disruptive::REDIRECT) {
            *ud->last_redirect = std::string(rule.detail_->redirect_);
        }
    }
}

// ===== 核心检测函数 =====
AlertResult detect(const Wge::Engine& engine, std::shared_ptr<HttpAccessEvent> event) {
    // 1. 创建 Transaction (返回 unique_ptr, 归当前 Worker 线程所有, 不可跨线程)
    auto tx = engine.makeTransaction();

    // 2. 构建适配器 (生命周期覆盖整个 detect 函数)
    HttpExtractorAdapter adapter(event);

    // 3. 准备 LogCallback 用户数据
    std::vector<MatchedRuleInfo> matched_rules;
    bool has_disruptive = false;
    Rule::Disruptive last_disruptive = Rule::Disruptive::PASS;
    std::string last_status_code;
    std::string last_redirect;

    LogCallbackUserData log_user_data{
        &matched_rules,
        &has_disruptive,
        &last_disruptive,
        &last_status_code,
        &last_redirect,
        tx.get()                    // Transaction 指针, 用于 LogCallback 中提取 matched_var
    };

    // 4. Phase 0: 连接信息
    //    WGE processConnection 使用 short 类型存储端口 (最大 32767)
    //    若端口 > 32767, 需在数据源侧调整或 accept 截断
    tx->processConnection(
        event->downstream_ip(),  static_cast<short>(event->downstream_port()),
        event->upstream_ip(),    static_cast<short>(event->upstream_port())
    );

    // 5. Phase 0: URI 解析 — 使用三参数版本
    //    processUri(uri, method, version): WGE 内部会构造 "METHOD URI HTTP/VERSION"
    //    注意: version 不含 "HTTP/" 前缀, WGE 会自动拼接
    tx->processUri(
        event->request_uri(),
        event->request_method(),
        event->request_version()
    );

    // 6. Phase 1: 请求头检测
    //    WGE 实际 API: processRequestHeaders(HeaderFind, HeaderTraversal, count, LogCallback, user_data, ...)
    //    返回 bool: true = 安全/允许, false = 拦截(DENY/DROP)
    bool safe = tx->processRequestHeaders(
        adapter.requestHeaderFind(),
        adapter.requestHeaderTraversal(),
        adapter.requestHeaderCount(),
        wgeLogCallback,
        &log_user_data
    );

    // 7. Phase 2: 请求体检测
    if (!event->request_body().empty()) {
        bool body_safe = tx->processRequestBody(std::string_view(
            event->request_body().data(),
            event->request_body().size()
        ));
        safe = safe && body_safe;
    }

    // 8. Phase 3-4: 响应检测 (仅在有响应数据时)
    bool has_response = event->response_status() != 0;

    if (has_response) {
        // processResponseHeaders 需要 status_code + protocol + Header 回调
        bool resp_header_safe = tx->processResponseHeaders(
            adapter.responseStatusCode(),       // string_view, 如 "403"
            adapter.responseProtocol(),         // string_view, 如 "HTTP/1.1"
            adapter.responseHeaderFind(),
            adapter.responseHeaderTraversal(),
            adapter.responseHeaderCount(),
            wgeLogCallback,
            &log_user_data
        );
        safe = safe && resp_header_safe;

        if (!event->response_body().empty()) {
            bool resp_body_safe = tx->processResponseBody(std::string_view(
                event->response_body().data(),
                event->response_body().size()
            ));
            safe = safe && resp_body_safe;
        }
    }

    // 9. 收集结果
    AlertResult result;
    result.event_id = event->event_id();
    result.timestamp_ms = event->timestamp_ms();
    result.intervened = !safe;   // safe=false 表示被拦截

    if (result.intervened) {
        // Disruptive action 从 LogCallback 中收集的 Rule 信息推断
        switch (last_disruptive) {
        case Rule::Disruptive::DENY:
            result.disruptive_action = "DENY"; break;
        case Rule::Disruptive::DROP:
            result.disruptive_action = "DROP"; break;
        case Rule::Disruptive::REDIRECT:
            result.disruptive_action = "REDIRECT";
            result.redirect_url = last_redirect;
            break;
        case Rule::Disruptive::ALLOW:
            result.disruptive_action = "ALLOW"; break;
        case Rule::Disruptive::ALLOW_PHASE:
            result.disruptive_action = "ALLOW_PHASE"; break;
        case Rule::Disruptive::ALLOW_REQUEST:
            result.disruptive_action = "ALLOW_REQUEST"; break;
        default:
            result.disruptive_action = "BLOCK"; break;
        }

        // HTTP status code: 从规则 detail_->status_ 获取 (默认 "403")
        result.response_code = last_status_code.empty() ? 403 : std::stoi(last_status_code);
        result.matched_rules = std::move(matched_rules);
    }

    return result;
}
```

> **WGE API 对接要点总结：**
>
> | 设计中原假设 | WGE 实际 API | 修正 |
> |-------------|-------------|------|
> | `tx->setHttpExtractor()` | 不存在 | Header 回调直接作为 `processRequestHeaders/ResponseHeaders()` 参数传入 |
> | `tx->onRuleMatch()` | 不存在 | `LogCallback` 函数指针 + `void* user_data`，通过 `process*()` 参数传入 |
> | `tx->intervention()` | 不存在 | 各 `process*()` 返回 `bool`，`false` = 拦截 |
> | `tx->disruptiveAction()` | 不存在 | 从 `LogCallback` 收到的 `Rule::detail_->disruptive_` 枚举推断 |
> | `tx->responseCode()` | 不存在 | 从 `Rule::detail_->status_` 获取 (string_view, 默认 "403") |
> | `tx->processUri(uri)` | 两个重载 | 使用三参数版本 `processUri(uri, method, version)` |
> | `processResponseHeaders()` | 需要 status + protocol | 传入 `responseStatusCode()` + `responseProtocol()` |
> | LogCallback 是 `std::function` | 实际是 C 函数指针 | 定义自由函数 `wgeLogCallback`，通过 `void*` 传入用户数据 |

### 5.6 Alert Producer 模块

```cpp
// alert_producer.h
class AlertProducer {
public:
    struct Config {
        std::string bootstrap_servers;
        std::string topic;
        std::string transactional_id;
        int32_t     batch_size{50};
        int32_t     linger_ms{50};
        bool        enable_idempotence{true};
        // ...
    };

    AlertProducer(const Config& cfg);
    ~AlertProducer();

    // 发送告警 (线程安全, 异步)
    void sendAlert(WgeAlertEvent&& alert);

    // 批量刷新 (在独立线程中调用)
    void flushLoop();

    // 初始化事务 (启动时调用一次)
    void initTransactions();

    // 优雅关闭
    void close();

private:
    // RdKafka 事务 API 全部在 Producer 上直接调用:
    //   producer_->begin_transaction()
    //   producer_->send_offsets_to_transaction(offsets, group_metadata, timeout)
    //   producer_->commit_transaction(timeout)
    //   producer_->abort_transaction(timeout)
    // 不存在独立的 RdKafka::Transaction 对象

    std::unique_ptr<RdKafka::Producer> producer_;

    // 有界告警缓冲区 (shared_ptr 保证可放入队列)
    std::mutex alert_queue_mutex_;
    std::condition_variable alert_queue_cv_;
    std::deque<std::shared_ptr<WgeAlertEvent>> alert_queue_;
    std::atomic<bool> running_{false};
};
```

**事务性发送流程：**

```
┌─────────────────────────────────────────────────────────────┐
│ Alert Producer Thread                                        │
│                                                              │
│  while (running) {                                           │
│      producer_->begin_transaction();                         │
│                                                              │
│      alerts = drainQueue(max_batch_size);                    │
│      for (alert : alerts) {                                  │
│          producer_->produce(topic, partition, key, payload); │
│      }                                                       │
│                                                              │
│      // 将 Consumer Offset 发送到事务 (CTP 模式核心)         │
│      // 由事务协调器原子提交: 告警 + offset                   │
│      producer_->send_offsets_to_transaction(                 │
│          processed_offsets,         // 已处理的 offset 列表   │
│          consumer_group_metadata,   // Consumer Group 元数据  │
│          timeout                                           │
│      );                                                      │
│                                                              │
│      producer_->commit_transaction(timeout);                 │
│      // 原子效果: 告警可被消费 + offset 已提交                │
│      // 若 commit 失败: abort_transaction → 重试             │
│                                                              │
│      sleep(linger_ms);                                       │
│  }                                                           │
└─────────────────────────────────────────────────────────────┘
```

> **CTP 模式关键点**：`send_offsets_to_transaction()` 将 offset 提交给 Kafka 事务协调器，而非直接调 `consumer->commit()`。事务 commit 成功后，offset 自动生效；事务 abort 后，offset 不生效，下次重新消费。这保证了 **告警输出 + Offset 提交** 的原子性。

---

## 6. Offset 管理策略（精确一次语义）

### 6.1 Consume-Transform-Produce 模式

```
Consumer Poll ──▶ Worker Pool ──▶ Alert Producer ──▶ Kafka Transaction Coordinator
     │                                                  │
     │◄────── send_offsets_to_transaction() ───────────┘
     │        (offset 由事务协调器管理, 非直接 commit)
```

| 步骤 | 操作 | 失败处理 |
|------|------|----------|
| 1 | Consumer poll 获取消息批次 | N/A |
| 2 | 反序列化为 `HttpAccessEvent` | 格式错误 → 死信队列 (DLQ) |
| 3 | Worker 线程 WGE 检测 | 超时 → 跳过此消息, 记录错误 |
| 4 | Alert Producer 事务性发送 | 发送失败 → `abort_transaction()` + 重试 3 次 |
| 5 | `send_offsets_to_transaction()` | 将已处理 offset 发给事务协调器，事务回滚则 offset 不生效 |
| 6 | `commit_transaction()` | 失败 → `abort_transaction()` → 重试整个事务 |

### 6.2 Offset 存储位置

- **Kafka 事务协调器** (CTP 模式): `send_offsets_to_transaction()` 将 offset 信息发送给事务协调器，事务 commit 后自动生效，无需本地维护 offset map
- **Kafka 内部 Topic** (`__consumer_offsets`): 事务 commit 后由事务协调器写入

---

## 7. 容错与生产级保障

### 7.1 故障场景与应对

| 故障场景 | 影响 | 应对策略 |
|----------|------|----------|
| **Kafka Broker 宕机** | Consumer/Producer 连接中断 | RdKafka 自动重连, `reconnect.backoff.ms` 指数退避 |
| **Kafka Rebalance** | 消费暂停 | cooperative-sticky 策略最小化影响; Rebalance 完成后自动恢复 |
| **Worker 线程 OOM** | 单线程崩溃 | 捕获 `std::bad_alloc`, 丢弃当前消息, 记录错误 |
| **WGE 检测超时** | 单消息卡住 | Worker 线程内设 per-task 超时 (默认 5s, 可配置), 超时后放弃该任务进 DLQ |
| **Alert Producer 发送失败** | 告警丢失 | 事务重试 3 次; 最终失败进本地 WAL 文件, 后台线程补发 |
| **整个进程崩溃** | 数据重复/丢失 | Offset 未提交 → 重复消费 (at-least-once, WGE 幂等检测) |
| **规则加载失败** | 引擎不可用 | 启动时校验; 热加载失败保留旧规则, 不中断服务 |
| **Consumer Lag 堆积** | 检测延迟增大 | 自适应限流: lag > threshold → 增大 batch / 减少非关键 body 检测 |

### 7.2 死信队列 (DLQ)

无法处理的消息投递到 DLQ Topic `http-access-dlq`：

```protobuf
message DeadLetterEvent {
  string  original_topic    = 1;
  int32   original_partition = 2;
  int64   original_offset   = 3;
  bytes   original_payload  = 4;  // 原始消息
  string  error_message     = 5;
  int64   timestamp_ms      = 6;
}
```

### 7.3 WAL (Write-Ahead Log) 告警兜底

```
Alert 发送 Kafka 失败
    │
    ├── 重试 3 次 (间隔 100ms / 500ms / 2s)
    │
    ├── 仍然失败 → 写入本地 WAL 文件
    │   /var/lib/wge/wal/alert-20260527-001.log
    │
    └── 后台 WAL-Relay 线程
        定期扫描 WAL 文件 → 重新发送到 Kafka
        发送成功 → 删除 WAL 条目
```

### 7.4 多实例水平扩展

基于 Kafka Consumer Group 机制实现天然的水平扩展能力，无需外部编排：

```
┌─────────────────────────────────────────────────┐
│              多实例部署 (物理机 / VM)             │
│                                                  │
│  ┌──────────────┐  ┌──────────────┐             │
│  │ wge-detector │  │ wge-detector │  × N       │
│  │  (Instance 0)│  │  (Instance 1)│             │
│  │  Consumer:   │  │  Consumer:   │             │
│  │  Partition 0 │  │  Partition 1 │             │
│  │  Partition 2 │  │  Partition 3 │             │
│  └──────┬───────┘  └──────┬───────┘             │
│         │                  │                     │
│  ┌──────▼──────────────────▼──────┐             │
│  │     Kafka Consumer Group      │             │
│  │  group.id = wge-detector     │             │
│  │  (自动 Rebalance 分配分区)    │             │
│  └───────────────────────────────┘             │
└─────────────────────────────────────────────────┘
```

**扩展要点：**

| 项 | 说明 |
|----|------|
| 实例数 ≤ 分区数 | 同一 Consumer Group 内，实例数不超过 Topic 分区数，超出则空闲 |
| 无状态设计 | 进程无本地持久状态（除 WAL），任意实例可随时启停 |
| 优雅关闭 | SIGTERM → 停止 poll → 等待 worker 处理完成 → flush 告警 → leave group → 退出 |
| 配置统一 | 所有实例共用同一份 `wge-detector.yaml`，通过配置管理工具同步分发 |

---

## 8. 可观测性

### 8.1 Prometheus Metrics

| 指标 | 类型 | 说明 |
|------|------|------|
| `wge_events_consumed_total` | Counter | 消费的事件总数 |
| `wge_events_processed_total` | Counter | 处理完成的事件总数 |
| `wge_events_dropped_total` | Counter | 丢弃/进入 DLQ 的事件数 |
| `wge_alerts_produced_total` | Counter | 产生的告警总数 |
| `wge_detection_duration_seconds` | Histogram | 单次 WGE 检测耗时 (含 bucket: 0.01, 0.05, 0.1, 0.5, 1, 5) |
| `wge_consumer_lag` | Gauge | Consumer Lag (messages) |
| `wge_consumer_lag_seconds` | Gauge | Consumer 时间延迟 |
| `wge_worker_pool_pending` | Gauge | 待处理任务队列深度 |
| `wge_worker_pool_active` | Gauge | 活跃 Worker 线程数 |
| `wge_rules_loaded` | Gauge | 已加载规则数 |
| `wge_rule_evaluations_total` | Counter | 规则评估次数 (by phase) |
| `wge_rule_matches_total` | Counter | 规则匹配次数 (by rule_id) |
| `wge_kafka_produce_errors_total` | Counter | Kafka 发送失败次数 |
| `wge_wal_pending` | Gauge | WAL 中待补发告警数 |

### 8.2 告警规则 (Prometheus)

```yaml
groups:
  - name: wge-detector
    rules:
      - alert: WGEConsumerLagHigh
        expr: wge_consumer_lag > 100000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "WGE consumer lag 超过 100K 消息"

      - alert: WGEDetectionSlow
        expr: histogram_quantile(0.99, wge_detection_duration_seconds) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "WGE P99 检测延迟超过 100ms"

      - alert: WGEAlertProduceFailures
        expr: rate(wge_kafka_produce_errors_total[5m]) > 0
        labels:
          severity: critical
        annotations:
          summary: "WGE 告警发送到 Kafka 失败"

      - alert: WGEDropRateHigh
        expr: rate(wge_events_dropped_total[5m]) > 0.01 * rate(wge_events_consumed_total[5m])
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "WGE 事件丢弃率超过 1%"
```

---

## 9. 性能估算

### 9.1 单节点吞吐

| 参数 | 值 |
|------|------|
| Worker 线程数 | 8 |
| WGE 基准 QPS (CRS v4.3.0, 8 threads) | ~17,560 |
| 含 Kafka 开销 (反序列化/序列化/网络) | 实际 QPS 降至 ~10,000 - 12,000 |
| Consumer Lag 处理速度 | ~12,000 events/s per node |

> **注**：WGE 基准 17560 QPS 是 8 worker 线程的**总吞吐**，非单线程。Kafka 开销（Protobuf 编解码 + 网络 I/O + offset 管理）预估带来 30-40% 性能折损。若需更高吞吐，水平扩展多实例。

### 9.2 延迟

| 环节 | 延迟 |
|------|------|
| Kafka 消费 (poll) | ~5ms |
| 日志映射 (JSON/Regex → Protobuf) | ~0.2ms |
| Protobuf 反序列化 | ~0.1ms |
| WGE 5-phase 检测 | ~0.057ms |
| Alert 序列化+发送 | ~2ms |
| **端到端 (从消费到告警发出)** | **~7ms (P50), ~15ms (P99)** |
| 含 poll_interval 等待 | +100ms (可配置) |

> **注**：若输入格式为 `protobuf`（直通模式），日志映射环节延迟可忽略（<0.01ms），端到端更快。JSON 映射使用 simdjson（~2GB/s 解析速度），Regex 映射使用 re2，均为 C++ 最高性能方案。

---

## 10. 项目结构

```
wge-kafka-detector/
├── CMakeLists.txt
├── cmake/
│   └── FindRdKafka.cmake
├── proto/
│   ├── http_access.proto          # 输入协议
│   └── wge_alert.proto            # 输出协议
├── src/
│   ├── main.cc                    # 入口: signal, config, 启动
│   ├── config/
│   │   ├── config.h               # 配置结构体
│   │   └── config_loader.cc        # YAML 解析 + 热加载
│   ├── kafka/
│   │   ├── consumer.h/.cc         # Kafka Consumer 封装
│   │   ├── producer.h/.cc         # Kafka Alert Producer 封装
│   │   └── dlq.h/.cc              # 死信队列处理
│   ├── detector/
│   │   ├── detector_service.h/.cc # 主服务: 编排 consumer/worker/producer
│   │   ├── worker_pool.h/.cc     # WGE 检测线程池
│   │   ├── http_extractor_adapter.h/.cc  # 零拷贝适配器
│   │   └── alert_builder.h/.cc   # 从 Transaction 构建 Alert
│   ├── mapper/
│   │   ├── mapper.h/.cc          # 映射引擎: 原始日志 → HttpAccessEvent
│   │   ├── json_mapper.h/.cc     # JSON 格式解析 + 字段提取
│   │   ├── regex_mapper.h/.cc    # Regex/Grok 格式解析
│   │   ├── field_applier.h/.cc   # 通用字段赋值 (类型转换/bytes解码)
│   │   └── mapper_config.h/.cc   # 映射配置加载 (log_mapping.yaml)
│   ├── wal/
│   │   ├── wal_writer.h/.cc       # WAL 写入
│   │   └── wal_relay.h/.cc        # WAL 补发
│   └── metrics/
│       ├── prometheus_server.h/.cc # Prometheus HTTP server
│       └── metrics.h               # 全局指标定义
├── config/
│   ├── wge-detector.yaml          # 默认配置
│   └── log_mapping.yaml           # 日志映射配置
├── deploy/
│   └── Dockerfile
├── scripts/
│   ├── generate-proto.sh          # Protobuf 代码生成
│   └── integration-test.sh        # 集成测试
└── test/
    ├── unit/
    │   ├── test_extractor_adapter.cc
    │   ├── test_alert_builder.cc
    │   ├── test_offset_manager.cc
    │   ├── test_json_mapper.cc
    │   └── test_regex_mapper.cc
    └── integration/
        └── test_kafka_e2e.cc
```

### 10.1 依赖清单 (vcpkg.json)

```json
{
  "name": "wge-kafka-detector",
  "version": "0.1.0",
  "dependencies": [
    "wge",
    "librdkafka",
    "protobuf",
    "simdjson",
    "re2",
    "yaml-cpp",
    "prometheus-cpp",
    "spdlog",
    "gtest"
  ]
}
```

---

## 11. 关键设计决策总结

| # | 决策 | 备选方案 | 选择理由 |
|---|------|----------|----------|
| 1 | **Protobuf 序列化** | JSON / Avro | 体积小 3x、C++ 原生、Schema 演化、零反射 |
| 2 | **手动 Offset 提交** | 自动提交 | 精确一次语义，避免丢告警 |
| 3 | **Consume-Transform-Produce 事务** | 至少一次 | 端到端精确一次：`send_offsets_to_transaction()` 保证告警+Offset 原子提交 |
| 4 | **HttpExtractorAdapter 独立对象** | setHttpExtractor (不存在) | WGE 无 setter，Header 回调直接作为 process*() 参数传入；O(1) 索引查找 |
| 5 | **单进程多线程** | 多进程 (Fork) | WGE Engine 主线程约束; 共享内存避免 IPC |
| 6 | **mutex + deque 替代 lockfree queue** | boost::lockfree::queue | shared_ptr 非 trivially copyable; batch 场景下 mutex 竞争极低 |
| 7 | **WAL 本地兜底** | 仅 Kafka 重试 | 极端情况 Kafka 不可用时保证告警不丢 |
| 8 | **Consumer 单线程 poll** | 多 Consumer 实例 | RdKafka 线程安全约束; 批量分发解耦 |
| 9 | **配置驱动日志映射** | 硬编码 Parser | `log_mapping.yaml` 支持 JSON/Regex/Grok 多格式，零代码扩展新数据源 |
| 10 | **LogCallback C 函数指针 + void*** | onRuleMatch Lambda (不存在) | WGE 实际 API 约束；自由函数 + user_data 结构体传递匹配信息 |
| 11 | **process*() 返回 bool 判断拦截** | intervention() (不存在) | WGE 实际 API 约束；bool 累积判断，Disruptive 从 Rule::detail_ 推断 |

---

## 12. 风险与缓解

| 风险 | 可能性 | 影响 | 缓解 |
|------|--------|------|------|
| **Kafka 全集群不可用** | 低 | 严重 | WAL 兜底 + 本地文件告警; 部署多 Kafka 集群容灾 |
| **WGE 规则热加载导致 Crash** | 中 | 高 | 热加载前规则语法校验; 失败回滚旧规则; Engine 仅主线程操作 |
| **单条大 Body 消息导致 OOM** | 中 | 中 | `request_body_limit` 配置上限; Worker 异常捕获; per-task 超时 |
| **Consumer Lag 雪崩** | 中 | 高 | 自适应限流; Lag 告警; 扩容 Worker |
| **Protobuf Schema 不兼容** | 低 | 中 | Schema Registry 强制兼容检查 |
| **RdKafka 与 WGE 内存冲突** | 低 | 严重 | 各模块独立内存池; ASAN/Valgrind 集成测试 |
| **WGE `processConnection` 端口 short 溢出** | 低 | 低 | 数据源侧确保端口 ≤ 32767; 若需高端口，向 WGE 上游提 PR 改用 unsigned short |

---

## 13. 对 WGE 上游的 API 依赖

集成所需但 WGE 当前未暴露的接口，需通过 **PR 或 friend class** 解决：

| # | 需要的接口 | WGE 当前状态 | 优先级 |
|---|-----------|-------------|--------|
| 1 | `Transaction::getCurrentMatchedVariables()` — 获取当前规则匹配的变量列表 | `matched_variables_` 为 private 成员，无 public getter | 🔴 必须 |
| 2 | `Rule::detail_->disruptive_` 通过 public getter 访问 | `detail_` 为 `unique_ptr<Detail>`，`Detail` 是 public struct，访问 path 为 `rule.detail_->disruptive_`，可访问但非标准 API | 🟡 建议封装 |
| 3 | `processRequestBody` / `processResponseBody` 的 LogCallback 参数 | 当前片段未显示这些方法是否支持 LogCallback | 🟡 需确认 |
| 4 | `processResponseHeaders` / `processResponseBody` 方法是否完整实现 | transaction.cc 仅展示了 `processRequestHeaders` 的实现 | 🟡 需确认 |

> **应急方案**：若 #1 短期无法合入 WGE 上游，可通过 Transaction 子类 `WgeTransaction` + `friend class` 直接访问 `matched_variables_`，代价是耦合约 WGE 内部实现。`detail_` 已是 public 成员，可安全访问。

---

*文档结束。此方案可直接进入详细设计和编码阶段。*
