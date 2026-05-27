# WGE-Kafka Detector — 安装使用文档

## 1. 系统要求

### 基础环境

| 项目 | 最低要求 | 推荐配置 |
|------|---------|---------|
| **操作系统** | Ubuntu 22.04+ / Debian 12+ / CentOS 9+ / RHEL 9+ | Ubuntu 24.04 LTS |
| **CPU 架构** | x86_64 (amd64) | x86_64, ≥ 4 核 |
| **内存** | 2 GB | 8 GB+（取决于 worker 线程数和 WGE 规则数量） |
| **磁盘** | 10 GB | 50 GB+ SSD（WAL 和日志存储） |
| **网络** | 可访问 Kafka 集群 | 低延迟内网 |

### 编译器

| 编译器 | 最低版本 |
|--------|---------|
| GCC | ≥ 13.1 |
| Clang | ≥ 17.0 |
| CMake | ≥ 3.28 |

### 运行时依赖

| 组件 | 用途 |
|------|------|
| librdkafka ≥ 2.3 | Kafka 客户端 |
| libprotobuf ≥ 3.21 | Protobuf 序列化 |
| WGE SDK | 安全检测引擎（企业许可） |
| simdjson | JSON 日志解析 |
| re2 | 正则表达式匹配 |

---

## 2. 依赖列表和安装命令

### Ubuntu / Debian

```bash
# 系统基础工具
sudo apt update
sudo apt install -y \
    build-essential \
    gcc-13 g++-13 \
    cmake \
    pkg-config \
    git \
    curl \
    ca-certificates

# 设置 GCC 13 为默认编译器
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-13 100
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-13 100
sudo update-alternatives --install /usr/bin/cpp cpp /usr/bin/cpp-13 100

# 安装 CMake ≥ 3.28（如果系统自带版本过低）
wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | \
    gpg --dearmor - | \
    sudo tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null
echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ jammy main' | \
    sudo tee /etc/apt/sources.list.d/kitware.list
sudo apt update
sudo apt install -y cmake

# vcpkg
git clone https://github.com/microsoft/vcpkg.git /opt/vcpkg
cd /opt/vcpkg
./bootstrap-vcpkg.sh

# 安装所有 C++ 依赖
/opt/vcpkg/vcpkg install \
    librdkafka \
    protobuf \
    simdjson \
    re2 \
    yaml-cpp \
    spdlog \
    prometheus-cpp \
    gtest
```

### CentOS / RHEL

```bash
# 启用 EPEL 和 CodeReady Builder
sudo dnf install -y epel-release
sudo dnf config-manager --set-enabled crb  # RHEL 9
# 或
sudo dnf config-manager --set-enabled powertools  # CentOS 8

# 安装 GCC 13 (从 gcc-toolset)
sudo dnf install -y gcc-toolset-13-gcc gcc-toolset-13-gcc-c++
source /opt/rh/gcc-toolset-13/enable  # 或写入 ~/.bashrc

# 安装 CMake
sudo dnf install -y cmake

# vcpkg
git clone https://github.com/microsoft/vcpkg.git /opt/vcpkg
cd /opt/vcpkg
./bootstrap-vcpkg.sh

# 安装依赖
/opt/vcpkg/vcpkg install \
    librdkafka \
    protobuf \
    simdjson \
    re2 \
    yaml-cpp \
    spdlog \
    prometheus-cpp \
    gtest
```

### 验证依赖

```bash
# 检查关键头文件
ls /opt/vcpkg/installed/x64-linux/include/librdkafka/rdkafkacpp.h
ls /opt/vcpkg/installed/x64-linux/include/google/protobuf/message.h
ls /opt/vcpkg/installed/x64-linux/include/simdjson.h
ls /opt/vcpkg/installed/x64-linux/include/re2/re2.h
ls /opt/vcpkg/installed/x64-linux/include/spdlog/spdlog.h

# 检查 WGE SDK
ls $WGE_SDK_PATH/include/wge/engine.h
```

---

## 3. 从源码构建的完整步骤

```bash
# 1. 克隆仓库
git clone https://github.com/your-org/wge-kafka-detector.git
cd wge-kafka-detector

# 2. 设置环境变量
export VCPKG_ROOT="/opt/vcpkg"
export CMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"

# 3. 配置 CMake（Release 构建）
cmake -B build -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_TOOLCHAIN_FILE="$CMAKE_TOOLCHAIN_FILE" \
    -DWGE_DETECTOR_LOG_LEVEL=4 \
    -DWGE_DETECTOR_ENABLE_TESTS=OFF

# 4. 编译
cmake --build build -j$(nproc)

# 5. 安装
sudo cmake --install build --prefix /usr/local

# 6. 验证安装
/usr/local/bin/wge-detector --version
# 输出:
# wge-kafka-detector version 0.1.0
#   Build: May 20 2026 08:15:00
#   C++ Standard: 202302L
```

### Debug 构建（开发/调试用）

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_TOOLCHAIN_FILE="$CMAKE_TOOLCHAIN_FILE" \
    -DWGE_DETECTOR_ENABLE_TESTS=ON
cmake --build build -j$(nproc)
```

---

## 4. 配置文件准备

### 4.1 主配置文件：`wge-detector.yaml`

存放位置：`/etc/wge-detector/config.yaml`

```yaml
# =============================================================================
# WGE-Kafka Detector 主配置文件
# =============================================================================

# 应用基本信息
app_version: "0.1.0"
instance_name: "wge-detector-01"

# ---------------------------------------------------------------------------
# Kafka 配置
# ---------------------------------------------------------------------------
kafka:
  consumer:
    bootstrap_servers: "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092"
    group_id: "wge-kafka-detector"
    topic: "http-access"
    auto_offset_reset: "latest"
    enable_auto_commit: false
    session_timeout_ms: 30000
    max_poll_records: 500
    heartbeat_interval_ms: 10000
    max_poll_interval_ms: 300000

  producer:
    bootstrap_servers: "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092"
    topic: "wge-alert"
    dlq_topic: "http-access-dlq"
    acks: -1              # all ISR
    compression_type: "lz4"
    linger_ms: 5.0
    batch_size: 1048576
    retries: 3

# ---------------------------------------------------------------------------
# WGE 引擎配置
# ---------------------------------------------------------------------------
wge:
  rule_files:
    - "/etc/wge/rules/modsecurity_crs_41_sql_injection.json"
    - "/etc/wge/rules/modsecurity_crs_41_xss.json"
    - "/etc/wge/rules/modsecurity_crs_42_tight_security.json"
    - "/etc/wge/rules/custom_business_rules.json"
  engine_config_path: "/etc/wge/engine.json"
  rule_update_interval_sec: 60
  engine_init_timeout_ms: 30000
  strict_mode: false       # true: 规则加载失败则启动失败

# ---------------------------------------------------------------------------
# 日志映射配置
# ---------------------------------------------------------------------------
mapping:
  log_mapping_path: "/etc/wge-detector/log_mapping.yaml"

# ---------------------------------------------------------------------------
# 检测器运行时配置
# ---------------------------------------------------------------------------
detector:
  poll_interval_ms: 100
  worker_threads: 0        # 0 = 自动检测 CPU 核心数
  batch_size: 64
  max_pending_tasks: 1024
  task_timeout_ms: 5000
  health_check_port: 8080
  graceful_shutdown_timeout_ms: 30000
  enable_dlq: true
  wal_dir: "/var/lib/wge-detector/wal"
  wal_segment_max_size: 268435456   # 256 MB

# ---------------------------------------------------------------------------
# 可观测性配置
# ---------------------------------------------------------------------------
observability:
  prometheus_enabled: true
  prometheus_port: 9090
  prometheus_path: "/metrics"
  otel_enabled: false
  otel_endpoint: "http://localhost:4317"
  log_level: "info"
  log_format: "json"       # json 或 text
  log_file_path: "/var/log/wge-detector/detector.log"
```

### 4.2 日志映射配置：`log_mapping.yaml`

#### JSON 格式示例

```yaml
format: json

# 字段映射
field_mappings:
  - source: "request.id"
    target: "event_id"
    type: String
    required: true
  - source: "request.timestamp"
    target: "timestamp_ms"
    type: Int64
    required: true
  - source: "request.method"
    target: "request_method"
    type: String
  - source: "request.uri"
    target: "request_uri"
    type: String
  - source: "request.version"
    target: "request_version"
    type: String
  - source: "connection.downstream_ip"
    target: "downstream_ip"
    type: String
  - source: "connection.upstream_ip"
    target: "upstream_ip"
    type: String
  - source: "response.status"
    target: "response_status"
    type: Int32

# Header 提取（Embedded：从嵌套子对象提取）
request_headers:
  strategy: Embedded
  embedded_path: "request.headers"
  is_request: true

response_headers:
  strategy: Embedded
  embedded_path: "response.headers"
  is_request: false

# 常量字段
constant_fields:
  - target: "collector_id"
    value: "${HOSTNAME}"          # 支持环境变量
```

对应的 JSON 日志格式：

```json
{
  "request": {
    "id": "abc123",
    "timestamp": 1700000000000,
    "method": "POST",
    "uri": "/api/login",
    "version": "1.1",
    "headers": {
      "Content-Type": "application/json",
      "User-Agent": "Mozilla/5.0"
    }
  },
  "connection": {
    "downstream_ip": "192.168.1.100",
    "upstream_ip": "10.0.0.10"
  },
  "response": {
    "status": 200,
    "headers": {
      "Server": "nginx/1.24"
    }
  }
}
```

#### Regex 格式示例

```yaml
format: regex

regex_pattern: >-
  (?P<remote_addr>\S+) - (?P<remote_user>\S+) \[(?P<timestamp>[^\]]+)\]
  "(?P<request_method>\S+) (?P<request_uri>\S+) (?P<request_version>\S+)"
  (?P<response_status>\d+) (?P<body_bytes_sent>\d+)
  "(?P<http_referer>[^"]*)" "(?P<http_user_agent>[^"]*)"

field_mappings:
  - source: "remote_addr"
    target: "downstream_ip"
    type: String
  - source: "request_method"
    target: "request_method"
    type: String
  - source: "request_uri"
    target: "request_uri"
    type: String
  - source: "request_version"
    target: "request_version"
    type: String
  - source: "response_status"
    target: "response_status"
    type: Int32
  - source: "body_bytes_sent"
    target: "response_body_length"
    type: Int64

# Header 提取（Prefix：从前缀字段提取）
request_headers:
  strategy: Prefix
  prefix: "http_"
  is_request: true
  normalize_keys: true    # http_content_type → Content-Type

# 时间戳解析
timestamp_config:
  source_field: "timestamp"
  formats:
    - "02/Jan/2006:15:04:05 -0700"   # Nginx 格式
    - "2006-01-02T15:04:05Z07:00"    # ISO 8601
  timezone: "UTC"

constant_fields:
  - target: "collector_id"
    value: "nginx-edge-01"
```

#### Grok 格式示例

```yaml
format: grok

grok_pattern: >-
  %{IP:downstream_ip} - %{USER:remote_user} \[%{HTTPDATE:timestamp}\]
  "%{WORD:request_method} %{URIPATHPARAM:request_uri} HTTP/%{NUMBER:request_version}"
  %{INT:response_status} %{INT:body_bytes_sent}

field_mappings:
  - source: "downstream_ip"
    target: "downstream_ip"
    type: String
  - source: "request_method"
    target: "request_method"
    type: String
  - source: "request_uri"
    target: "request_uri"
    type: String
  - source: "response_status"
    target: "response_status"
    type: Int32
```

### 4.3 配置文件位置约定

| 文件 | 推荐路径 | 说明 |
|------|---------|------|
| 主配置 | `/etc/wge-detector/config.yaml` | 通过 `--config` 参数指定 |
| 日志映射 | `/etc/wge-detector/log_mapping.yaml` | 主配置中 `mapping.log_mapping_path` 引用 |
| WGE 规则 | `/etc/wge/rules/*.json` | 主配置中 `wge.rule_files` 列表 |
| WGE 引擎配置 | `/etc/wge/engine.json` | 主配置中 `wge.engine_config_path` 引用 |

---

## 5. Kafka 集群连接配置

### 5.1 PLAINTEXT 模式（不加密，仅开发环境）

```yaml
kafka:
  consumer:
    bootstrap_servers: "localhost:9092"
    topic: "http-access"
    group_id: "wge-kafka-detector"
    # 不设置 security_protocol，默认 PLAINTEXT

  producer:
    bootstrap_servers: "localhost:9092"
    topic: "wge-alert"
```

### 5.2 SASL/PLAINTEXT 模式

```yaml
kafka:
  consumer:
    bootstrap_servers: "kafka-broker-1:9093,kafka-broker-2:9093"
    topic: "http-access"
    group_id: "wge-kafka-detector"
    extra_properties:
      - key: "security.protocol"
        value: "SASL_PLAINTEXT"
      - key: "sasl.mechanisms"
        value: "SCRAM-SHA-256"
      - key: "sasl.username"
        value: "${KAFKA_USERNAME}"        # 使用环境变量
      - key: "sasl.password"
        value: "${KAFKA_PASSWORD}"

  producer:
    bootstrap_servers: "kafka-broker-1:9093,kafka-broker-2:9093"
    topic: "wge-alert"
    extra_properties:
      - key: "security.protocol"
        value: "SASL_PLAINTEXT"
      - key: "sasl.mechanisms"
        value: "SCRAM-SHA-256"
      - key: "sasl.username"
        value: "${KAFKA_USERNAME}"
      - key: "sasl.password"
        value: "${KAFKA_PASSWORD}"
```

### 5.3 SASL/SSL 模式（生产环境推荐）

```yaml
kafka:
  consumer:
    bootstrap_servers: "kafka-broker-1:9094,kafka-broker-2:9094,kafka-broker-3:9094"
    topic: "http-access"
    group_id: "wge-kafka-detector"
    extra_properties:
      - key: "security.protocol"
        value: "SASL_SSL"
      - key: "sasl.mechanisms"
        value: "SCRAM-SHA-512"
      - key: "sasl.username"
        value: "${KAFKA_USERNAME}"
      - key: "sasl.password"
        value: "${KAFKA_PASSWORD}"
      - key: "ssl.ca.location"
        value: "/etc/ssl/certs/kafka-ca.pem"
      - key: "ssl.certificate.location"
        value: "/etc/ssl/certs/detector-client.pem"
      - key: "ssl.key.location"
        value: "/etc/ssl/private/detector-client.key"
      - key: "ssl.key.password"
        value: "${SSL_KEY_PASSWORD}"
      - key: "enable.ssl.certificate.verification"
        value: "true"
      - key: "ssl.endpoint.identification.algorithm"
        value: "HTTPS"

  producer:
    bootstrap_servers: "kafka-broker-1:9094,kafka-broker-2:9094,kafka-broker-3:9094"
    topic: "wge-alert"
    transactional_id: "wge-detector-01"   # 启用事务（Exactly-once）
    extra_properties:
      - key: "security.protocol"
        value: "SASL_SSL"
      - key: "sasl.mechanisms"
        value: "SCRAM-SHA-512"
      - key: "sasl.username"
        value: "${KAFKA_USERNAME}"
      - key: "sasl.password"
        value: "${KAFKA_PASSWORD}"
      - key: "ssl.ca.location"
        value: "/etc/ssl/certs/kafka-ca.pem"
      - key: "ssl.certificate.location"
        value: "/etc/ssl/certs/detector-client.pem"
      - key: "ssl.key.location"
        value: "/etc/ssl/private/detector-client.key"
```

---

## 6. 日志映射配置详解

### 6.1 三种格式对比

| 特性 | JSON | Regex | Grok |
|------|------|-------|------|
| 性能 | 高（simdjson） | 高（RE2） | 中（先转 Regex） |
| 灵活性 | 支持任意嵌套路径 | 需完整匹配行格式 | 语义化模式 |
| Header 提取 | Embedded / Prefix | Prefix | Prefix |
| 类型转换 | String/Int32/Int64/Bytes/Bool/Header | 同 JSON | 同 JSON |
| 适用场景 | 结构化 JSON API 日志 | Nginx/Apache 标准日志 | Logstash 风格日志 |

### 6.2 字段类型说明

| type | 说明 | 示例 source → target |
|------|------|---------------------|
| `String` | 直接字符串赋值 | `"request.method"` → `"request_method"` |
| `Int32` | 字符串转 int32 | `"response.status"` → `"response_status"` |
| `Int64` | 字符串转 int64 | `"timestamp"` → `"timestamp_ms"` |
| `Bytes` | Base64/Hex 解码为 bytes | `"request.body_base64"` → `"request_body"` |
| `Bool` | 字符串转 bool | `"tls_enabled"` → is_tls |
| `Header` | 添加为 repeated Header | `"content-type"` → `request_headers` |

### 6.3 Header 提取策略

**Embedded 策略** — Headers 在 JSON 子对象中：

```yaml
request_headers:
  strategy: Embedded
  embedded_path: "request.headers"
  is_request: true
```

对应 JSON：
```json
{
  "request": {
    "headers": {
      "Content-Type": "application/json",
      "Authorization": "Bearer token123"
    }
  }
}
```

**Prefix 策略** — Headers 以字段名前缀散布：

```yaml
request_headers:
  strategy: Prefix
  prefix: "http_"
  is_request: true
  normalize_keys: true
```

对应扁平化字段：
```
http_content_type → Content-Type (normalized)
http_user_agent   → User-Agent
http_cookie       → Cookie
```

### 6.4 时间戳格式

系统按以下优先级尝试解析：

1. **ISO 8601**: `2006-01-02T15:04:05Z07:00`
2. **Nginx**: `02/Jan/2006:15:04:05 -0700`
3. **Simple**: `2006-01-02 15:04:05`
4. **Unix epoch seconds**: `1700000000`
5. **Unix epoch milliseconds**: `1700000000000`

可在 `timestamp_config.formats` 中自定义格式列表。

---

## 7. 部署模式说明

### 7.1 单节点部署

```
┌──────────────────────────────────────┐
│           Single Node                 │
│  ┌────────────────────────────────┐  │
│  │     wge-kafka-detector         │  │
│  │  Consumer → Mapper → Detector  │  │
│  │      → Producer → WAL         │  │
│  └────────────────────────────────┘  │
│              │                        │
│     ┌────────┴────────┐              │
│     ▼                 ▼              │
│  Kafka Input      Kafka Output       │
│  (http-access)    (wge-alert)        │
└──────────────────────────────────────┘
```

**启动命令**：

```bash
wge-detector --config /etc/wge-detector/config.yaml
```

**适用场景**：开发/测试环境、低流量生产环境（< 10k QPS）。

### 7.2 多节点水平扩展

```
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│   Node 1         │   │   Node 2         │   │   Node 3         │
│   wge-detector   │   │   wge-detector   │   │   wge-detector   │
│   instance: 01   │   │   instance: 02   │   │   instance: 03   │
└────────┬─────────┘   └────────┬─────────┘   └────────┬─────────┘
         │                      │                      │
         └──────────────────────┼──────────────────────┘
                                │
                     ┌──────────┴──────────┐
                     │   Kafka Cluster     │
                     │   3+ Brokers        │
                     │   Partitioning      │
                     └─────────────────────┘
```

**关键配置差异**：

| 配置项 | Node 1 | Node 2 | Node 3 |
|--------|--------|--------|--------|
| `instance_name` | `wge-detector-01` | `wge-detector-02` | `wge-detector-03` |
| `kafka.consumer.group_id` | `wge-kafka-detector`（相同） | `wge-kafka-detector`（相同） | `wge-kafka-detector`（相同） |
| `kafka.producer.transactional_id` | `wge-detector-01` | `wge-detector-02` | `wge-detector-03` |

**扩展原理**：

- 所有节点使用相同的 Consumer Group，Kafka 自动进行分区分配
- 每个节点处理分配到的分区，互不干扰
- 新增节点自动触发 rebalance，无需重启现有节点

### 7.3 systemd 服务文件

```ini
# /etc/systemd/system/wge-detector.service
[Unit]
Description=WGE-Kafka Detector - Security Detection Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=wge-detector
Group=wge-detector
ExecStart=/usr/local/bin/wge-detector --config /etc/wge-detector/config.yaml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=10
LimitNOFILE=262144
LimitNPROC=4096

# 安全加固
NoNewPrivileges=yes
ProtectSystem=full
ProtectHome=yes
ReadWritePaths=/var/lib/wge-detector/wal /var/log/wge-detector
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
```

```bash
# 启动服务
sudo systemctl daemon-reload
sudo systemctl enable wge-detector
sudo systemctl start wge-detector

# 查看状态
sudo systemctl status wge-detector

# 热重载配置
sudo systemctl reload wge-detector

# 查看日志
sudo journalctl -u wge-detector -f
```

---

## 8. 健康检查和监控接入

### 8.1 健康检查端点

系统在 `detector.health_check_port`（默认 8080）上提供 HTTP 端点：

```bash
# 存活检查
curl http://localhost:8080/healthz
# 返回: 200 OK

# 就绪检查
curl http://localhost:8080/readyz
# 返回: 200 OK (Consumer 已连接且 WorkerPool 已启动)
```

### 8.2 Prometheus Metrics

```bash
# 查看 Prometheus 指标
curl http://localhost:9090/metrics
```

**可用指标**：

| 指标名称 | 类型 | 标签 | 说明 |
|---------|------|------|------|
| `wge_detector_events_consumed` | Counter | `instance` | 已消费的事件总数 |
| `wge_detector_events_processed` | Counter | `instance` | 处理成功的事件数 |
| `wge_detector_events_dropped` | Counter | `instance` | 丢弃的事件数 |
| `wge_detector_alerts_produced` | Counter | `instance` | 生成告警数 |
| `wge_detector_kafka_produce_errors` | Counter | `instance` | Kafka 发送失败数 |
| `wge_detector_rule_evaluations` | Counter | `instance` | 规则评估次数 |
| `wge_detector_rule_matches` | Counter | `instance` | 规则匹配次数 |
| `wge_detector_consumer_lag` | Gauge | `instance` | 消费延迟 |
| `wge_detector_worker_pool_pending` | Gauge | `instance` | 待处理任务数 |
| `wge_detector_worker_pool_active` | Gauge | `instance` | 活跃 Worker 数 |
| `wge_detector_rules_loaded` | Gauge | `instance` | 已加载规则数 |
| `wge_detector_wal_pending` | Gauge | `instance` | WAL 待写入数 |
| `wge_detector_detection_duration_us` | Histogram | `instance` | 检测耗时分布 (us) |

### 8.3 Prometheus 抓取配置

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'wge-detector'
    scrape_interval: 15s
    static_configs:
      - targets:
          - 'detector-node-1:9090'
          - 'detector-node-2:9090'
          - 'detector-node-3:9090'
        labels:
          service: 'wge-kafka-detector'
```

### 8.4 关键告警规则

```yaml
# Prometheus 告警规则示例
groups:
  - name: wge-detector
    rules:
      - alert: HighConsumerLag
        expr: wge_detector_consumer_lag > 10000
        for: 5m
        annotations:
          summary: "Consumer lag 过高 ({{ $value }})"

      - alert: HighDropRate
        expr: rate(wge_detector_events_dropped[5m]) / rate(wge_detector_events_consumed[5m]) > 0.05
        for: 5m
        annotations:
          summary: "事件丢弃率超过 5%"

      - alert: DetectionSlowdown
        expr: histogram_quantile(0.99, rate(wge_detector_detection_duration_us_bucket[5m])) > 100000
        for: 10m
        annotations:
          summary: "P99 检测耗时超过 100ms"
```

---

## 9. 信号处理

### 9.1 SIGTERM / SIGINT — 优雅停止

```bash
# 方式 1: systemd
sudo systemctl stop wge-detector

# 方式 2: 直接发送信号
kill -TERM $(pidof wge-detector)
# 或 Ctrl+C（前台运行时）
```

**停止序列**：

1. `KafkaConsumer::stop()` — 停止消费新消息
2. `WgeWorkerPool::stop()` — 等待所有 in-flight 任务完成
3. 排空队列中的剩余任务（swap-then-process）
4. `AlertProducer::close()` — 提交最后事务，清空发送队列
5. `DeadLetterQueue::close()` — 清空 DLQ
6. 输出最终统计（events_consumed / alerts_produced / events_dropped）

超时控制：`detector.graceful_shutdown_timeout_ms`（默认 30000ms）。

### 9.2 SIGHUP — 热重载配置

```bash
# 方式 1: systemd
sudo systemctl reload wge-detector

# 方式 2: 直接发送信号
kill -HUP $(pidof wge-detector)
```

**重载行为**：

1. 重新读取 `--config` 指定的 YAML 文件
2. 新配置与当前配置合并（`ConfigLoader::reload`）
3. 日志级别立即更新（通过 spdlog 动态调整）
4. 原配置中未出现在新文件中的字段保持不变

**注意**：Kafka 连接参数、WGE 规则文件等更改需重启才能生效。

### 9.3 SIGPIPE — 自动忽略

`SIGPIPE` 信号被显式忽略（`SIG_IGN`），防止写已关闭的 socket 导致进程退出。

---

## 10. 日志文件路径和轮转

### 10.1 应用日志

由 spdlog 管理：

- **控制台输出**：stdout（始终启用）
- **文件输出**：由 `observability.log_file_path` 配置（如 `/var/log/wge-detector/detector.log`）

```yaml
observability:
  log_format: "json"
  log_file_path: "/var/log/wge-detector/detector.log"
```

JSON 格式输出示例：

```json
{"time":"2026-05-20T08:15:30.123+0800","level":"INFO","logger":"wge-detector","msg":"DetectorService started","thread":1}
```

### 10.2 日志轮转

使用 logrotate 管理文件日志：

```bash
# /etc/logrotate.d/wge-detector
/var/log/wge-detector/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
    maxsize 100M
}
```

### 10.3 WAL 文件

WAL 文件位于 `detector.wal_dir`（默认 `/var/lib/wge-detector/wal/`）：

```
/var/lib/wge-detector/wal/
├── alert-20260520-08.log
├── alert-20260520-09.log
└── alert-20260520-10.log
```

- 按**小时**自动轮转
- 单文件最大大小由 `detector.wal_segment_max_size` 控制（默认 256MB）
- WalRelay 补发成功后会清理对应条目

### 10.4 磁盘空间监控

```bash
# 监控 WAL 目录大小
du -sh /var/lib/wge-detector/wal/

# 查看当前小时文件
ls -lh /var/lib/wge-detector/wal/alert-$(date +%Y%m%d-%H).log
```

---

## 11. 升级和回滚步骤

### 11.1 滚动升级（多节点）

```bash
# 1. 在节点 1 上
sudo systemctl stop wge-detector

# 2. 更新二进制
sudo cp /tmp/wge-detector-new /usr/local/bin/wge-detector
sudo chmod +x /usr/local/bin/wge-detector

# 3. 更新配置（如有变更）
sudo cp /tmp/config.yaml /etc/wge-detector/config.yaml

# 4. 启动并验证
sudo systemctl start wge-detector
sleep 5
sudo systemctl status wge-detector
curl http://localhost:9090/metrics | head

# 5. 检查日志
sudo journalctl -u wge-detector --since "1 min ago" -n 20

# 6. 确认节点 1 正常后，对其他节点重复步骤 1-5
```

### 11.2 灰度验证

升级第一个节点后建议观察至少 **15 分钟**，重点关注：

- Consumer lag 是否稳定或下降
- `events_dropped` 速率是否为零
- `detection_duration_us` P99 是否在预期范围内
- 是否有 `kafka_produce_errors`

### 11.3 回滚

```bash
# 1. 停止服务
sudo systemctl stop wge-detector

# 2. 恢复旧版本二进制
sudo cp /backup/wge-detector-old /usr/local/bin/wge-detector

# 3. 恢复旧配置
sudo cp /backup/config.yaml.old /etc/wge-detector/config.yaml

# 4. 启动
sudo systemctl start wge-detector

# 5. 验证
sudo systemctl status wge-detector
```

### 11.4 升级检查清单

- [ ] 阅读 Release Notes 中的 Breaking Changes
- [ ] 在测试环境验证新版本
- [ ] 备份当前二进制和配置
- [ ] 检查 Protobuf schema 兼容性（forward/backward）
- [ ] 确认 Kafka topic partition 数量未变
- [ ] 准备回滚方案
- [ ] 通知下游消费者（wge-alert topic 的消费者）

### 11.5 备份当前部署

```bash
# 创建部署备份
BACKUP_DIR="/backup/wge-detector-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$BACKUP_DIR"

cp /usr/local/bin/wge-detector "$BACKUP_DIR/"
cp -r /etc/wge-detector/ "$BACKUP_DIR/config/"
cp -r /etc/wge/rules/ "$BACKUP_DIR/rules/"

echo "Backup created at: $BACKUP_DIR"
```
