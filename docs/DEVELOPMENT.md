# WGE-Kafka Detector — 开发文档

## 1. 项目概述

WGE-Kafka Detector 是一个高性能的 **Kafka-WGE 安全检测桥梁服务**，基于 C++23 开发。它从 Kafka 消费 HTTP 访问日志，通过 [WGE (Web Gateway Engine)](https://www.chaitin.cn/zh/waf) 安全检测引擎进行攻击检测，将检测结果（告警）发送回 Kafka。

### 核心能力

- **多格式日志映射**：支持 JSON、Regex、Grok、Protobuf 四种输入格式，可灵活配置字段映射
- **高性能检测**：Thread-per-core 工作线程模型，有界队列背压控制，协作式超时机制
- **Exactly-once 语义**：基于 Kafka 事务的 CTP (Consume-Transform-Produce) 模式
- **WAL 预写日志**：告警发送前先写 WAL，崩溃后可重放补发
- **死信队列 (DLQ)**：处理失败的消息路由到 DLQ，保留原始上下文
- **Prometheus 指标**：内置 Metrics 汇总，支持 Prometheus 文本格式导出
- **优雅关闭与热重载**：SIGTERM 优雅关闭，SIGHUP 热重载配置

### 技术栈

| 组件 | 版本/说明 |
|------|----------|
| C++ 标准 | C++23 (`CMAKE_CXX_STANDARD 23`) |
| 构建系统 | CMake ≥ 3.28 |
| 包管理器 | vcpkg |
| WGE 引擎 | 企业安全检测引擎 |
| Kafka 客户端 | librdkafka (RdKafka) |
| JSON 解析 | simdjson（超高性能 SIMD JSON 解析器） |
| 正则匹配 | RE2（Google 线性时间正则引擎） |
| 序列化 | Protobuf (proto3) |
| 配置解析 | yaml-cpp |
| 日志 | spdlog |
| 监控指标 | prometheus-cpp（可选） |
| 测试框架 | Google Test |

---

## 2. 开发环境搭建

### 2.1 系统要求

- **操作系统**：Ubuntu 22.04+ / Debian 12+ / CentOS 9+ / macOS 13+
- **编译器**：GCC ≥ 13.1 或 Clang ≥ 17（必须支持 C++23）
- **CMake**：≥ 3.28
- **内存**：开发环境建议 ≥ 8GB

### 2.2 安装 GCC 13+

```bash
# Ubuntu 22.04
sudo apt update
sudo apt install -y gcc-13 g++-13
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-13 100
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-13 100

# 验证
gcc --version   # 应显示 13.x
g++ --version
```

### 2.3 安装 CMake

```bash
# 从 Kitware APT 源安装最新版
wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | sudo tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null
echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ jammy main' | sudo tee /etc/apt/sources.list.d/kitware.list
sudo apt update
sudo apt install -y cmake

# 验证
cmake --version   # 应显示 ≥ 3.28
```

### 2.4 安装 vcpkg 及依赖

```bash
# 克隆 vcpkg
git clone https://github.com/microsoft/vcpkg.git ~/vcpkg
cd ~/vcpkg
./bootstrap-vcpkg.sh

# 设置环境变量（建议写入 ~/.bashrc）
export VCPKG_ROOT="$HOME/vcpkg"
export CMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"

# 安装项目依赖
vcpkg install \
    librdkafka \
    protobuf \
    simdjson \
    re2 \
    yaml-cpp \
    spdlog \
    prometheus-cpp \
    gtest
```

### 2.5 一键安装脚本

```bash
# 在项目根目录下
cd /path/to/wge-kafka-detector

# 安装所有依赖（需要先安装 vcpkg）
xargs -a <(echo "librdkafka protobuf simdjson re2 yaml-cpp spdlog prometheus-cpp gtest") \
    vcpkg install

# 验证 WGE SDK 可用性
ls $VCPKG_ROOT/installed/x64-linux/include/wge/engine.h
```

---

## 3. 项目结构详解

```
wge-kafka-detector/
├── CMakeLists.txt              # 顶层构建文件（C++23, 依赖声明, 目标定义）
├── proto/
│   ├── http_access.proto       # Kafka 输入消息定义 (HttpAccessEvent)
│   ├── wge_alert.proto         # Kafka 输出消息定义 (WgeAlertEvent, DeadLetterEvent)
│   └── (生成的 C++ 代码)        # protobuf 编译产物，输出到 build 目录
├── config/                     # 运行时配置（YAML 文件存放目录，git 中可放示例）
├── scripts/                    # 运维脚本（部署、启动、健康检查等）
├── cmake/                      # CMake 模块（Find*.cmake 等，当前为扩展预留）
├── docs/                       # 项目文档
├── build/                      # CMake 构建输出目录（gitignore）
├── src/
│   ├── main.cc                 # 程序入口：参数解析 → 初始化 → 事件循环 → 优雅关闭
│   ├── config/
│   │   ├── config.h            # 配置结构体定义 (AppConfig, KafkaConfig, WgeConfig 等)
│   │   ├── config_loader.h     # YAML 配置加载器声明 (loadFromFile, reload)
│   │   └── config_loader.cc    # 配置加载实现（yaml-cpp 解析, 环境变量替换, 字段验证）
│   ├── mapper/
│   │   ├── mapper_config.h     # 映射配置定义 (Format 枚举, FieldMapping, MapperConfig)
│   │   ├── mapper_config.cc    # 映射配置解析和序列化
│   │   ├── mapper.h            # LogMapper 主类声明（策略模式分发到各映射器）
│   │   ├── mapper.cc           # LogMapper 实现（Pimpl 惯用法）
│   │   ├── json_mapper.h       # JSON 日志映射器（基于 simdjson）
│   │   ├── json_mapper.cc      # JSON 解析实现（on-demand 解析, 嵌套路径访问）
│   │   ├── regex_mapper.h      # Regex/Grok 日志映射器（基于 RE2）
│   │   ├── regex_mapper.cc     # 正则提取实现（命名捕获组 → 字段映射）
│   │   ├── field_applier.h     # 字段应用器声明（提取字段 → Protobuf 赋值）
│   │   └── field_applier.cc    # 字段写入实现（String/Int32/Int64/Bytes/Bool/Header）
│   ├── kafka/
│   │   ├── consumer.h          # KafkaConsumer 声明（批量 poll, 优雅停止, lag 查询）
│   │   ├── consumer.cc         # 消费者实现（RdKafka::KafkaConsumer 封装, 独立 poll 线程）
│   │   ├── producer.h          # AlertProducer 声明（事务性批量发送, CTP 模式）
│   │   ├── producer.cc         # 生产者实现（事务循环, send_offsets_to_transaction）
│   │   ├── dlq.h               # DeadLetterQueue 声明（死信队列, 非事务 producer）
│   │   └── dlq.cc              # DLQ 实现（sendRaw 包装原始消息）
│   ├── detector/
│   │   ├── result.h            # 检测结果数据结构 (MatchedRuleInfo, AlertResult)
│   │   ├── worker_pool.h       # WgeWorkerPool 声明（固定线程池, 有界阻塞队列）
│   │   ├── worker_pool.cc      # Worker 池实现（detect() 流程, 协作式超时, swap-drain）
│   │   ├── http_extractor_adapter.h   # HttpAccessEvent → WGE 回调适配器声明
│   │   ├── http_extractor_adapter.cc  # 适配器实现（HeaderFind, HeaderTraversal, 索引构建）
│   │   ├── alert_builder.h     # AlertBuilder 声明（AlertResult → WgeAlertEvent）
│   │   ├── alert_builder.cc    # 告警构建实现（UUID v7, 字段合并）
│   │   ├── detector_service.h  # DetectorService 声明（顶层编排, 生命周期管理）
│   │   └── detector_service.cc # 编排服务实现（start/stop 序列, onConsumerBatch）
│   ├── wal/
│   │   ├── wal_writer.h        # WalWriter 声明（告警写 WAL, 按小时轮转）
│   │   ├── wal_writer.cc       # WAL 写入实现（JSON 序列化, 文件轮转, fsync）
│   │   ├── wal_relay.h         # WalRelay 声明（后台补发, 文件扫描）
│   │   └── wal_relay.cc        # 补发实现（逐行读取, 反序列化, 重发 → 删除）
│   └── metrics/
│       ├── metrics.h           # Metrics 单例声明（atomic 计数器/Gauge/Histogram）
│       └── metrics.cc          # Metrics 实现（Prometheus 文本格式导出）
└── test/
    ├── unit/
    │   ├── test_json_mapper.cc      # JSON 映射器单元测试
    │   ├── test_regex_mapper.cc     # Regex 映射器单元测试
    │   ├── test_field_applier.cc    # 字段应用器单元测试
    │   ├── test_extractor_adapter.cc # HTTP 提取适配器单元测试
    │   ├── test_alert_builder.cc    # 告警构建器单元测试
    │   ├── test_offset_manager.cc   # Offset 管理逻辑单元测试（模拟 CTP offset 追踪）
    │   ├── test_config_loader.cc    # 配置加载器单元测试
    │   └── test_worker_pool.cc      # Worker 池单元测试
    └── integration/
        ├── test_nginx_injection.cc      # Nginx 日志注入测试（20 正常 + 15 攻击）
        ├── test_offset_state_machine.cc  # Offset 状态机集成测试（11 种场景）
        ├── test_wal_roundtrip.cc         # WAL 往返测试（8 种场景）
        ├── test_concurrent_worker.cc     # 并发压力测试（10 种场景）
        └── test_e2e_pipeline.cc          # 端到端管道测试（11 种格式/场景）
```

### 各目录职责速查

| 目录 | 职责 |
|------|------|
| `src/config/` | 配置结构体定义和 YAML 加载 |
| `src/mapper/` | 原始日志 → HttpAccessEvent 的映射转换 |
| `src/kafka/` | Kafka 消费者/生产者/DLQ 的封装 |
| `src/detector/` | WGE 检测核心：线程池、适配器、告警构建、编排服务 |
| `src/wal/` | WAL 预写日志的写入和中继补发 |
| `src/metrics/` | Prometheus 兼容的指标汇总 |
| `test/unit/` | 各模块独立的单元测试 |
| `test/integration/` | 跨模块集成测试、注入测试、状态机测试 |

---

## 4. 代码风格规范

### 4.1 命名约定

| 元素 | 约定 | 示例 |
|------|------|------|
| 命名空间 | `snake_case` | `wge::kafka::detector` |
| 类/结构体 | `PascalCase` | `KafkaConsumer`, `DetectorService` |
| 函数/方法 | `camelCase` | `submitBatch()`, `consumerLag()` |
| 变量/成员 | `snake_case` | `bootstrap_servers`, `running_` |
| 常量 | `kPascalCase` 或 `UPPER_SNAKE` | `kNumBuckets`, `WGE_DETECTOR_VERSION` |
| 枚举值 | `PascalCase` | `Format::Json`, `FieldType::Int32` |
| 头文件保护 | `#pragma once` | （统一使用，不使用 `#ifndef` 宏） |
| 文件名 | `snake_case` | `config_loader.h`, `worker_pool.cc` |

### 4.2 注释规范

全部使用 **Doxygen 风格**（`///` 或 `/** */`）：

```cpp
/**
 * @brief 简要说明
 *
 * 详细说明...
 *
 * @param config 配置参数
 * @return std::expected<Result, Error> 返回值说明
 * @throws std::runtime_error 可能的异常
 * @note 注意事项
 */
[[nodiscard]] std::expected<Result, std::string> someFunction(const Config& config);
```

- 每个公开的类、方法、结构体必须有 `@brief`
- 关键设计决策使用 `## 设计要点` 格式在文件头注释中说明
- 每个 `.h` 和 `.cc` 文件头部必须包含 `@file` 注释块

### 4.3 错误处理模式

**统一使用 `std::expected<T, std::string>`** 作为返回值：

```cpp
// ✅ 推荐：显式错误传播
[[nodiscard]] std::expected<AppConfig, std::string> loadFromFile(const std::string& path);

// 调用方
auto result = loadFromFile(path);
if (!result) {
    SPDLOG_ERROR("Failed: {}", result.error());
    return 1;
}
auto cfg = std::move(*result);
```

**不使用异常做正常控制流**，异常仅用于构造函数、资源分配失败等不可恢复场景：

```cpp
// ✅ 构造函数中使用异常
explicit KafkaConsumer(const ConsumerConfig& config) {
    // ... rdkafka 创建失败时抛 std::runtime_error
}
```

**`[[nodiscard]]` 标注所有不应当忽略返回值的函数**。

### 4.4 其他约定

- **不可拷贝不可移动**：持有线程/资源的类统一禁用拷贝和移动（`= delete`）
- **RAII 自动化**：析构函数自动调用 `stop()`/`close()`，异常内部捕获不传播
- **内存序**：atomic 变量读取统一 `memory_order_acquire`，写入 `memory_order_release`，计数器用 `memory_order_relaxed`
- **规则零/五**：资源管理类明确声明或删除五大特殊成员函数

---

## 5. 模块架构详解

### 5.1 数据流概览

```
Kafka → Consumer.poll → LogMapper.map → WorkerPool.submitBatch
                                               ↓
                                     detect() → AlertBuilder.build
                                               ↓
                                     AlertProducer.sendAlert → Kafka (wge-alert)
                                               ↓
                                     WalWriter.write → WAL 文件
                                               ↓
                                     WalRelay → 补发失败告警
                                               ↓
                                     DeadLetterQueue → Kafka (http-access-dlq)
```

### 5.2 各模块职责

#### Config (配置层)
- `AppConfig` 聚合所有子系统配置，所有字段有生产级默认值
- `ConfigLoader::loadFromFile()` 解析 YAML，支持 `${VAR_NAME}` 和 `${VAR_NAME:-default}` 环境变量替换
- `ConfigLoader::reload()` 支持热重载（SIGHUP 触发），新配置与 base 合并

#### Mapper (日志映射层)
- **策略模式**：`LogMapper` 根据 `Format` 枚举分发到具体映射器
- `JsonMapper`（基于 simdjson）— 解析 JSON 日志，支持嵌套路径
- `RegexMapper`（基于 RE2）— 解析正则/Grok 日志，支持命名捕获组
- `FieldApplier` — 将提取的字段值写入 `HttpAccessEvent` Protobuf，支持 6 种类型转换
- `ConstantField` — 为事件注入常量（如 `collector_id`）
- `HeaderExtractionConfig` — Embedded/Prefix 两种 Header 提取策略
- `TimestampConfig` — 支持 5 种时间格式自动解析

#### Kafka (Kafka 客户端层)
- **KafkaConsumer**：独立 poll 线程，批量回调，支持 consumer lag 和 committed offset 查询
- **AlertProducer**：事务性批量发送（CTP 模式），内部队列 + flush 线程
- **DeadLetterQueue**：非事务性独立 producer，包装失败消息

#### Detector (检测核心层)
- **DetectorService**：顶层编排，start/stop 序列，onConsumerBatch 回调
- **WgeWorkerPool**：固定线程池 + 有界阻塞队列，协作式超时，swap-then-process drain
- **HttpExtractorAdapter**：构建 header 索引（O(1) 查找），提供 WGE 回调接口
- **AlertBuilder**：AlertResult → WgeAlertEvent，UUID v7 生成
- **AlertResult / MatchedRuleInfo**：检测结果数据模型

#### WAL (预写日志层)
- **WalWriter**：告警先写 WAL 再发 Kafka，按小时轮转（`alert-YYYYMMDD-HH.log`）
- **WalRelay**：后台线程定期扫描，逐行补发失败告警

#### Metrics (指标层)
- 进程内 atomic 计数器/Gauge/Histogram
- `toPrometheusText()` 输出 Prometheus 兼容格式
- 支持单例访问 `Metrics::instance()`

---

## 6. 线程模型

项目使用 **四类线程**，通过原子变量和有界队列进行协调：

```
┌─────────────────────────────────────────────────┐
│ Main Thread                                      │
│ - 事件循环（500ms 间隔）                          │
│ - SIGHUP → ConfigLoader::reload()                │
│ - 更新 consumer_lag gauge                        │
│ - 检测 g_shutdown_requested → detector.stop()    │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│ Consumer Thread (1 个)                           │
│ - KafkaConsumer::poll loop                       │
│ - 累积至 max_poll_records 或 poll 返回空         │
│ - 通过 onConsumerBatch 回调投递整个批次           │
│ - 回调中: 解析 → 提交 WorkerPool → 失败入 DLQ    │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│ Worker Threads (N 个, N = CPU 核心数)            │
│ - WgeWorkerPool::workerLoop()                    │
│ - 从有界阻塞队列消费 HttpAccessEvent              │
│ - detect():                                       │
│   1. HttpExtractorAdapter 构建 header 索引        │
│   2. WGE Transaction::processConnection()        │
│   3. processRequestURI()                         │
│   4. processRequestHeaders()                     │
│   5. processRequestBody()                        │
│   6. processResponseHeaders()                    │
│   7. 收集 matched_variables → AlertResult        │
│   8. AlertBuilder::build() → sendAlert()         │
│ - 协作式超时: 每个步骤间检查 timed_out()          │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│ AlertProducer Thread (1 个)                      │
│ - flushLoop: 从内部队列消费告警                   │
│ - 事务流程:                                       │
│   1. begin_transaction()                         │
│   2. produce() 批量消息                           │
│   3. send_offsets_to_transaction()               │
│   4. commit_transaction()                        │
└─────────────────────────────────────────────────┘
```

### 线程间通信

| 通信方向 | 机制 |
|---------|------|
| Consumer → WorkerPool | `submitBatch()` → 有界阻塞队列 (`std::deque` + `condition_variable`) |
| Worker → AlertProducer | `sendAlert()` → 内部队列 (`std::deque` + `mutex` + `condition_variable`) |
| Worker → WalWriter | 直接调用 `write()`（内部 mutex 保护文件写） |
| 关闭信号 → 所有线程 | `atomic<bool>` 标志 + `condition_variable::notify_all()` |

### 背压机制

- WorkerPool 有界队列满 → `submitBatch()` 阻塞 → Consumer 线程阻塞 → `poll()` 不再调用 → Kafka 暂停拉取
- AlertProducer 队列满 → `sendAlert()` 阻塞 → Worker 线程阻塞 → WorkerPool 队列堆积 → 向上游传导

---

## 7. WGE API 对接说明

### 7.1 检测流程

```cpp
// 1. 为每个 HttpAccessEvent 构建适配器
HttpExtractorAdapter adapter(*event);

// 2. 创建 WGE Transaction
auto tx = engine.makeTransaction();

// 3. 设置日志回调（收集 matched_variables）
std::vector<MatchedRuleInfo> matched_rules;
auto logCallback = [&matched_rules](const wge::Rule& rule, 
                                     const wge::Detail& detail,
                                     std::string_view var_name,
                                     std::string_view var_value,
                                     std::string_view var_original) {
    MatchedRuleInfo info;
    info.rule_id = rule.id();
    info.rule_msg = rule.msg();
    info.severity = rule.severity();
    info.matched_var_name = std::string(var_name);
    info.matched_var_value = std::string(var_value);
    info.matched_var_original = std::string(var_original);
    matched_rules.push_back(std::move(info));
};

// 4. 执行检测流程
tx->processConnection(adapter.downstreamIp(), adapter.downstreamPort(),
                       adapter.upstreamIp(), adapter.upstreamPort(),
                       logCallback, &result, nullptr, nullptr);

tx->processRequestURI(adapter.requestUri(), logCallback, &result,
                       nullptr, nullptr);

tx->processRequestHeaders(
    adapter.requestHeaderFind(),      // HeaderFind 回调
    adapter.requestHeaderTraversal(),  // HeaderTraversal 回调
    adapter.requestHeaderCount(),      // Header 总数
    logCallback, &result, nullptr, nullptr);

tx->processRequestBody(adapter.requestBody(), logCallback, &result,
                        nullptr, nullptr);

tx->processResponseHeaders(
    adapter.responseHeaderFind(),
    adapter.responseHeaderTraversal(),
    adapter.responseHeaderCount(),
    logCallback, &result, nullptr, nullptr);

tx->processResponseBody(adapter.responseBody(), logCallback, &result,
                         nullptr, nullptr);
```

### 7.2 LogCallback 签名

```cpp
// WGE 引擎在匹配规则时通过此回调通知调用方
using LogCallback = std::function<void(
    const wge::Rule& rule,           // 匹配的规则
    const wge::Detail& detail,       // 规则细节
    std::string_view var_name,       // 匹配变量名（如 REQUEST_URI）
    std::string_view var_value,      // 变换后的值
    std::string_view var_original    // 原始值
)>;
```

### 7.3 processRequestHeaders 签名

```cpp
void processRequestHeaders(
    HeaderFind find,          // bool(string_view key, string_view& value)
    HeaderTraversal traverse, // void(function<void(string_view, string_view)>)
    size_t count,             // Header 总数
    LogCallback cb,
    void* result,
    void* request_context,
    void* response_context
);
```

### 7.4 matched_variables 访问

每条规则匹配时，WGE 引擎通过 `LogCallback` 回调传递匹配变量。所有匹配变量在单次 `detect()` 调用中累积到 `std::vector<MatchedRuleInfo>`，最终构建为 `AlertResult`。

---

## 8. 构建和调试命令

### 8.1 基本构建

```bash
# 在项目根目录下
cd wge-kafka-detector

# 配置（Debug 模式，启用 AddressSanitizer 和 UndefinedBehaviorSanitizer）
cmake -B build -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"

# 构建
cmake --build build -j$(nproc)

# 安装
cmake --install build --prefix /usr/local
```

### 8.2 Release 构建

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" \
    -DWGE_DETECTOR_ENABLE_TESTS=OFF \
    -DWGE_DETECTOR_LOG_LEVEL=4   # WARNING 级别

cmake --build build -j$(nproc)
```

### 8.3 运行测试

```bash
# 配置并构建测试
cmake -B build -DCMAKE_BUILD_TYPE=Debug \
    -DWGE_DETECTOR_ENABLE_TESTS=ON \
    -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
cmake --build build -j$(nproc)

# 运行所有测试
cd build && ctest --output-on-failure

# 运行特定测试
ctest -R test_json_mapper --output-on-failure

# 直接运行测试二进制（获得详细输出）
./build/wge_detector_test --gtest_filter="*JsonMapper*"
```

### 8.4 调试

```bash
# 使用 GDB
gdb --args ./build/wge-detector --config config/wge-detector.yaml

# 使用 AddressSanitizer（Debug 构建自动启用）
cmake -B build -DCMAKE_BUILD_TYPE=Debug ...
./build/wge-detector --config config/wge-detector.yaml
# 内存错误时 ASan 会打印详细堆栈

# 使用 helgrind 检测线程竞争
valgrind --tool=helgrind ./build/wge_detector_test
```

### 8.5 CMake 选项

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `WGE_DETECTOR_ENABLE_TESTS` | ON | 是否构建单元测试 |
| `WGE_DETECTOR_ENABLE_INTEGRATION_TESTS` | OFF | 是否构建集成测试 |
| `WGE_DETECTOR_LOG_LEVEL` | 3 | 活跃日志级别 (1=trace..7=off) |
| `CMAKE_BUILD_TYPE` | — | Debug / Release / RelWithDebInfo |

---

## 9. 添加新 Mapper 格式的指南

### 步骤

1. **在 `mapper_config.h` 中添加新 Format 枚举值**：

```cpp
enum class Format : uint8_t {
    Protobuf = 0,
    Json = 1,
    Regex = 2,
    Grok = 3,
    Csv = 4,     // 新增 CSV 格式
};
```

2. **在 `formatToString()` 和 `parseFormat()` 中添加对应转换**。

3. **创建新的 Mapper 类**（如 `src/mapper/csv_mapper.h` 和 `.cc`）：

```cpp
// csv_mapper.h
#pragma once
#include <expected>
#include <string>
#include <string_view>
#include <map>

namespace wge::kafka::mapper {

class CsvMapper {
public:
    [[nodiscard]] std::expected<std::map<std::string, std::string>, std::string>
    extract(std::string_view raw_payload,
            const std::vector<std::string>& columns,
            const std::vector<FieldMapping>& mappings) const;
};

}  // namespace wge::kafka::mapper
```

4. **在 `CORE_SOURCES` 中添加新文件**（`CMakeLists.txt`）。

5. **在 `LogMapper::map()` 中添加分发逻辑**：

```cpp
case Format::Csv:
    return impl_->csvMapper.extract(raw_payload, config_.csv_columns, 
                                     config_.field_mappings);
```

6. **在 `MapperConfig` 中添加 CSV 特有配置字段**（如 `csv_columns`, `csv_delimiter`）。

7. **编写单元测试**（`test/unit/test_csv_mapper.cc`）和集成测试。

8. **更新 `log_mapping.yaml` 文档和示例**。

---

## 10. 常见问题和 FAQ

### Q: 编译时找不到 wge 库？

确保 WGE SDK 已通过 vcpkg 安装，且 `CMAKE_TOOLCHAIN_FILE` 正确指向 vcpkg.cmake。检查 `$VCPKG_ROOT/installed/x64-linux/include/wge/engine.h` 是否存在。

### Q: 如何切换 Protobuf 版本？

在 vcpkg 中指定版本：`vcpkg install protobuf:protobuf@3.21.12`，或在 manifest 模式下使用 `vcpkg.json` 固定版本。

### Q: Worker 线程数如何设置？

- `detector.worker_threads: 0`（默认）：自动检测 `std::thread::hardware_concurrency()`
- `detector.worker_threads: 8`：固定 8 个线程
- 建议设置为 CPU 核心数，避免过度订阅

### Q: 为什么使用 `std::expected` 而不是异常？

在数据处理管道中，解析失败是**预期内**的情况（如格式错误的日志），不应使用异常做控制流。`std::expected` 提供零开销的错误传播路径。

### Q: Kafka rebalance 时会不会丢消息？

不会。项目使用 CTP（Consume-Transform-Produce）模式：告警处理和 offset 提交在同一个 Kafka 事务中完成。若 rebalance 发生在事务提交前，消息会被重新消费处理。

### Q: 如何验证 Exactly-once 语义？

检查以下配置：
- `kafka.producer.enable_idempotence: true`（默认）
- `kafka.producer.transactional_id` 非空（启用事务）
- `kafka.consumer.enable_auto_commit: false`（默认）

### Q: WAL 文件占用过多磁盘空间怎么办？

- 设置 `detector.wal_segment_max_size` 限制单文件大小（默认 256MB）
- WalRelay 补发成功后会自动清理对应条目
- 可配置定期清理脚本删除超过 N 天的 WAL 文件

### Q: 如何查看实时 Metrics？

```bash
curl http://localhost:9090/metrics
```

返回 Prometheus 文本格式的指标数据，包含计数器、Gauge 和 Histogram。

### Q: 配置热重载支持哪些字段？

当前支持：日志级别（`observability.log_level`）。其他字段（如 Kafka 连接参数、规则文件路径等）的重载在 `ConfigLoader::reload()` 中通过 base merge 机制支持，但运行时切换可能导致连接中断。
