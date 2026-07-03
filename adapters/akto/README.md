# Akto API 日志适配器

## 概述

本适配器将 **Akto API Security** 平台的 Kafka 输出转换为 WGE-Kafka Detector 可识别的 `HttpAccessEvent` 格式。

- **输入 Topic**: `akto.api.logs`
- **输出 Topic**: `wge.alert`

## Akto 日志格式的特殊性

与标准 HTTP 访问日志不同，Akto 的 JSON 输出有 **3 处特殊处理**：

| 问题 | Akto 格式 | WGE 期望 | 处理方式 |
|------|----------|---------|---------|
| Headers 是 JSON 字符串 | `"requestHeaders": "{\"Accept\":\"...\",\"Host\":\"...\"}"` | `"requestHeaders": [{"key":"Accept","value":"..."}, ...]` | AktoPreprocessor 二次解析并展开 |
| 时间是秒级时间戳 | `"time": "1779867214"` | `1779867214000` (毫秒) | ×1000 |
| HTTP 版本有前缀 | `"type": "HTTP/1.1"` | `"1.1"` | strip `HTTP/` |

## 文件清单

```
adapters/akto/
├── README.md                  # 本文档
├── wge-detector.yaml          # 检测器配置 (指向 akto.api.logs → wge.alert)
├── log_mapping.yaml           # 日志映射配置
├── akto_preprocessor.h        # 预处理器头文件
├── akto_preprocessor.cc       # 预处理器实现
└── test_akto_adapter.cc       # 集成测试 (10 用例)
```

## 快速开始

### 1. 安装

```bash
# 从源码构建 (含 Akto 适配器)
cd wge-kafka-detector
./scripts/build.sh --release

# 安装
sudo ./scripts/install.sh
```

### 2. 配置

```bash
# 复制 Akto 专用配置
sudo cp adapters/akto/wge-detector.yaml /etc/wge/wge-detector.yaml
sudo cp adapters/akto/log_mapping.yaml /etc/wge/log_mapping.yaml

# 编辑 Kafka 连接信息
sudo vim /etc/wge/wge-detector.yaml
# 修改: kafka.consumer.bootstrap_servers
# 修改: kafka.producer.bootstrap_servers
# 修改: SASL 用户名/密码
```

### 3. 启动

```bash
sudo systemctl start wge-detector
sudo systemctl status wge-detector
sudo journalctl -u wge-detector -f
```

### 4. 验证

```bash
# 检查消费 lag
curl -s http://localhost:9101/metrics | grep wge_consumer_lag

# 检查告警输出
curl -s http://localhost:9101/metrics | grep wge_alerts_produced
```

## 数据处理流程

```
Akto API Security
      │
      ▼ produce
┌──────────────────┐
│  akto.api.logs   │  Kafka Topic
│  (原始 JSON)     │
└────────┬─────────┘
         │ consume
         ▼
┌──────────────────────────┐
│  AktoPreprocessor        │
│  ├─ 解析 JSON 字符串     │
│  ├─ 展开 requestHeaders  │  "{\"Accept\":\"...\"}" → [{"key":"Accept","value":"..."}]
│  ├─ 展开 responseHeaders │
│  ├─ time 秒→毫秒         │  1779867214 → 1779867214000
│  └─ type HTTP/1.1→1.1    │
└──────────┬───────────────┘
           ▼
┌──────────────────────────┐
│  JsonMapper              │
│  ├─ 字段映射             │  path→request_uri, method→request_method, ...
│  ├─ Header 提取          │  embedded 模式
│  └─ 常量注入             │  downstream_port=0, upstream_port=0
└──────────┬───────────────┘
           ▼
┌──────────────────────────┐
│  HttpAccessEvent         │  Protobuf
└──────────┬───────────────┘
           ▼
┌──────────────────────────┐
│  WGE Engine              │  CRS v4.3.0 规则检测
└──────────┬───────────────┘
           ▼
┌──────────────────┐
│  wge.alert       │  Kafka Topic
│  (告警输出)      │
└──────────────────┘
```

## 字段映射对照表

| Akto 字段 | WGE 字段 | 类型 | 说明 |
|-----------|---------|------|------|
| `path` | `request_uri` | string | 请求路径 |
| `method` | `request_method` | string | HTTP 方法 |
| `requestHeaders` | `request_headers` | repeated Header | 需预处理器展开 |
| `requestPayload` | `request_body` | bytes | 原始字符串 |
| `responseHeaders` | `response_headers` | repeated Header | 需预处理器展开 |
| `responsePayload` | `response_body` | bytes | 原始字符串 |
| `statusCode` | `response_status` | int32 | 字符串转整数 |
| `type` | `request_version` | string | 需预处理 strip HTTP/ |
| `ip` | `downstream_ip` | string | 客户端 IP |
| `destIp` | `upstream_ip` | string | 目标 IP |
| `time` | `timestamp_ms` | int64 | 秒×1000 |
| `akto_account_id` | `collector_id` | string | 采集源标识 |
| (constant) | `downstream_port` | int32 | Akto 不提供, 设 0 |
| (constant) | `upstream_port` | int32 | Akto 不提供, 设 0 |

## 测试

```bash
# 编译并运行 Akto 适配器测试
cd wge-kafka-detector
g++ -std=c++23 -I src -I adapters/akto \
    adapters/akto/test_akto_adapter.cc \
    adapters/akto/akto_preprocessor.cc \
    src/mapper/mapper.cc src/mapper/json_mapper.cc \
    src/mapper/mapper_config.cc src/mapper/field_applier.cc \
    src/config/config_loader.cc \
    -lsimdjson -lre2 -lyaml-cpp -lprotobuf -lspdlog \
    -lgtest -lgtest_main -lpthread \
    -o /tmp/test_akto && /tmp/test_akto
```

测试覆盖：
- ✅ 预处理器展开 requestHeaders (含 Cookie 中的 JWT token)
- ✅ 预处理器展开 responseHeaders (含 OpenRASP X-Request-ID)
- ✅ 时间戳秒→毫秒转换
- ✅ HTTP 版本号前缀去除
- ✅ 完整管道：原始 JSON → Event (request_uri/method/headers/body 全验证)
- ✅ Request headers 逐字段验证 (Host/Content-Type/Cookie/User-Agent/Referer)
- ✅ Response headers 逐字段验证 (X-Protected-By/X-Request-ID/Transfer-Encoding)
- ✅ 空 headers 边界处理
- ✅ 中文 payload 完整保留
- ✅ 50 条批量处理

## 注意事项

1. **端口号缺失**: Akto 日志不提供 TCP 端口号, `downstream_port` / `upstream_port` 均设为 0。WGE 检测规则通常不依赖端口号, 影响可忽略。

2. **Headers 中的特殊字符**: requestHeaders 中的 Cookie 可能包含 JWT token 和 URL 编码字符。预处理器通过 simdjson 正确解析, 不会截断。

3. **连接方向**: Akto 的 `direction` 字段为 "REQUEST" (镜像模式)。当前适配器忽略此字段, 直接回放式检测。

4. **Kafka 消息顺序**: Akto 日志可能包含同一请求的多次记录 (REQUEST + RESPONSE)。建议上游按 `request_id` 去重后再写入 Kafka, 或将 `direction` 字段加入分区键。
