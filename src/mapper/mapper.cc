/**
 * @file mapper.cc
 * @brief LogMapper 实现 — 日志格式映射引擎
 *
 * ## 模块职责
 * LogMapper 是整个数据管道的入口映射层，负责将原始 Kafka 消息（JSON/Regex/Grok/Protobuf）
 * 解析并转换为统一的 HttpAccessEvent protobuf 结构，供下游 WGE 检测引擎使用。
 *
 * ## 核心流程
 * ```
 * raw_payload (Kafka message bytes)
 *   │
 *   ├─ [Format::Json] ──→ JsonMapper::extract()      → extracted_fields (map)
 *   ├─ [Format::Regex] ─→ RegexMapper::extract()     → extracted_fields (map)
 *   ├─ [Format::Grok] ──→ RegexMapper::extractGrok() → extracted_fields (map)
 *   └─ [Format::Protobuf] ──→ 直接 ParseFromArray()
 *   │
 *   ├─ FieldApplier::applyFields() → 将字段写入 HttpAccessEvent
 *   ├─ 时间戳解析 (Regex/Grok 格式)
 *   └─ 常量字段注入
 *   │
 *   └─→ HttpAccessEvent (标准 protobuf)
 * ```
 *
 * ## 设计模式
 * - **Pimpl (Pointer to Implementation)**: 使用 Impl 结构体隐藏映射器依赖，
 *   减小头文件包含范围，加快编译速度
 * - **策略模式**: 根据 Format 枚举选择不同的提取策略（JsonMapper / RegexMapper / 直接反序列化）
 * - **std::expected 错误处理**: 所有映射操作返回 expected<T, string>，
 *   调用方可选择处理或传播错误
 */

#include "mapper/mapper.h"

#include <climits>
#include <stdexcept>

#include "http_access.pb.h"
#include "akto_message.pb.h"
#include "mapper/field_applier.h"
#include "mapper/json_mapper.h"
#include "mapper/regex_mapper.h"
#include "spdlog/spdlog.h"

namespace wge::kafka::mapper {

// ============================================================================
// Pimpl 实现结构体
// ============================================================================

struct LogMapper::Impl {
    MapperConfig config;  // 映射配置（格式类型、字段映射规则等）

    // 具体映射器按需持有（根据 format 决定初始化哪个）
    std::unique_ptr<JsonMapper> json_mapper;      ///< JSON 格式提取器
    std::unique_ptr<RegexMapper> regex_mapper;    ///< Regex/Grok 格式提取器
    std::unique_ptr<FieldApplier> field_applier;  ///< 字段写入器（始终需要）

    explicit Impl(const MapperConfig& cfg)
        : config(cfg)
        , field_applier(std::make_unique<FieldApplier>()) {}  // field_applier 总是初始化
};

// ============================================================================
// 构造与析构
// ============================================================================

LogMapper::LogMapper(const MapperConfig& config)
    : config_(config)
    , impl_(std::make_unique<Impl>(config)) {
    // 根据配置的日志格式预初始化对应的映射器
    // 在构造阶段完成，避免每次 map() 都做分支判断
    switch (config_.format) {
        case Format::Json:
            impl_->json_mapper = std::make_unique<JsonMapper>();
            spdlog::debug("LogMapper: initialized JsonMapper");
            break;

        case Format::Regex:
            impl_->regex_mapper = std::make_unique<RegexMapper>();
            // 预编译正则模式：构造时编译失败则直接抛异常，快速失败
            if (!config_.regex_pattern.empty()) {
                auto compile_result =
                    impl_->regex_mapper->compile(config_.regex_pattern);
                if (!compile_result) {
                    throw std::runtime_error(
                        std::string("LogMapper: failed to compile regex '") +
                        config_.regex_pattern +
                        "': " + compile_result.error());
                } else {
                    spdlog::debug("LogMapper: compiled regex '{}'",
                                  config_.regex_pattern);
                }
            }
            break;

        case Format::Grok:
            // Grok 最终转换为 Regex 后再匹配，复用 RegexMapper
            impl_->regex_mapper = std::make_unique<RegexMapper>();
            spdlog::debug("LogMapper: initialized RegexMapper for Grok");
            break;

        case Format::Protobuf:
            // Protobuf 无需额外映射器，直接反序列化即可
            spdlog::debug("LogMapper: using direct Protobuf deserialization");
            break;

        case Format::AktoProtobuf:
            // AktoProtobuf 无需额外映射器，直接反序列化 Akto HttpResponseParam 即可
            spdlog::debug("LogMapper: using direct AktoProtobuf deserialization");
            break;
    }
}

LogMapper::~LogMapper() = default;

// ============================================================================
// map
// ============================================================================

std::expected<std::shared_ptr<HttpAccessEvent>, std::string> LogMapper::map(
    std::string_view raw_payload) const {
    // 空载荷快速拒绝
    if (raw_payload.empty()) {
        return std::unexpected(std::string("Empty payload"));
    }

    auto event = std::make_shared<HttpAccessEvent>();

    // ===== 步骤 1: 按格式分发，提取原始字段到 map =====
    std::map<std::string, std::string> extracted_fields;

    switch (config_.format) {
        case Format::Json: {
            if (!impl_->json_mapper) {
                return std::unexpected(
                    std::string("JsonMapper not initialized"));
            }
            // JSON: 一次性解析 + 按 field_mappings 提取字段
            auto fields_result = impl_->json_mapper->extract(
                raw_payload, config_.field_mappings);
            if (!fields_result) {
                return std::unexpected(
                    std::string("JsonMapper::extract failed: ") +
                    fields_result.error());
            }
            extracted_fields = std::move(*fields_result);

            // JSON 格式额外支持内嵌 headers 提取
            impl_->json_mapper->extractHeaders(
                raw_payload, config_.request_headers, config_.response_headers,
                *event);
            break;
        }

        case Format::Regex: {
            if (!impl_->regex_mapper) {
                return std::unexpected(
                    std::string("RegexMapper not initialized"));
            }
            auto fields_result = impl_->regex_mapper->extract(
                raw_payload, config_.regex_pattern, config_.field_mappings);
            if (!fields_result) {
                return std::unexpected(
                    std::string("RegexMapper::extract failed: ") +
                    fields_result.error());
            }
            extracted_fields = std::move(*fields_result);
            break;
        }

        case Format::Grok: {
            if (!impl_->regex_mapper) {
                return std::unexpected(
                    std::string("RegexMapper not initialized (Grok)"));
            }
            auto fields_result = impl_->regex_mapper->extractGrok(
                raw_payload, config_.grok_pattern,
                config_.grok_custom_patterns, config_.field_mappings);
            if (!fields_result) {
                return std::unexpected(
                    std::string("RegexMapper::extractGrok failed: ") +
                    fields_result.error());
            }
            extracted_fields = std::move(*fields_result);
            break;
        }

        case Format::Protobuf: {
            // Protobuf 直接反序列化，无需字段提取步骤
            // INT_MAX 检查防止 size 溢出（protobuf ParseFromArray 接受 int）
            if (raw_payload.size() > static_cast<size_t>(INT_MAX)) {
                return std::unexpected(
                    std::string("Protobuf payload too large: ") +
                    std::to_string(raw_payload.size()) + " bytes");
            }
            if (!event->ParseFromArray(raw_payload.data(),
                                       static_cast<int>(raw_payload.size()))) {
                return std::unexpected(
                    std::string("Failed to parse Protobuf payload (") +
                    std::to_string(raw_payload.size()) + " bytes)");
            }
            // Protobuf 格式直接应用常量字段后返回
            for (const auto& cf : config_.constant_fields) {
                impl_->field_applier->applyConstant(*event, cf.target, cf.value);
            }
            return event;
        }

        case Format::AktoProtobuf: {
            // Akto HttpResponseParam protobuf (akto.api.logs2 Topic)
            // 使用 Akto 标准 proto 反序列化，字段映射到 HttpAccessEvent
            if (raw_payload.size() > static_cast<size_t>(INT_MAX)) {
                return std::unexpected(
                    std::string("Akto Protobuf payload too large: ") +
                    std::to_string(raw_payload.size()) + " bytes");
            }

            wge::kafka::akto::HttpResponseParam akto_pb;
            if (!akto_pb.ParseFromArray(raw_payload.data(),
                                        static_cast<int>(raw_payload.size()))) {
                return std::unexpected(
                    std::string("Failed to parse Akto HttpResponseParam (") +
                    std::to_string(raw_payload.size()) + " bytes)");
            }

            // P2-1: HttpResponseParam 无 event_id 字段，留空（由 AlertBuilder 生成 UUID v7）
            // event->set_event_id(generateUuidV7());  // 在 AlertBuilder 中生成

            // 时间戳: int32 秒 → int64 毫秒
            event->set_timestamp_ms(static_cast<int64_t>(akto_pb.time()) * 1000);

            // collector_id ← akto_account_id
            event->set_collector_id(akto_pb.akto_account_id());

            // P2-2: IP 映射
            event->set_downstream_ip(akto_pb.ip());
            event->set_upstream_ip(akto_pb.dest_ip());
            // downstream_port / upstream_port 保持常量注入 = 0

            // 请求信息
            event->set_request_method(akto_pb.method());
            event->set_request_uri(akto_pb.path());
            event->set_request_version(akto_pb.type());

            // P1-1: akto_vxlan_id → collection_id (与 Akto L610 一致)
            // Akto threat-detection 用 akto_vxlan_id (string) 解析为 int 作为 apiCollectionId
            event->set_akto_vxlan_id(akto_pb.akto_vxlan_id());
            event->set_akto_account_id(akto_pb.akto_account_id());
            int32_t col_id = 0;
            try {
                col_id = std::stoi(akto_pb.akto_vxlan_id());
            } catch (...) {
                col_id = 0;
            }
            event->set_akto_collection_id(col_id);

            // request_headers: map<string, StringList> → repeated Header
            for (const auto& [key, values] : akto_pb.request_headers()) {
                for (const auto& val : values.values()) {
                    auto* h = event->add_request_headers();
                    h->set_key(key);
                    h->set_value(val);
                }
            }

            // response_headers: 同上
            for (const auto& [key, values] : akto_pb.response_headers()) {
                for (const auto& val : values.values()) {
                    auto* h = event->add_response_headers();
                    h->set_key(key);
                    h->set_value(val);
                }
            }

            // request_payload / response_payload: 原始 body，直接映射
            // (Akto 的 rawToJsonString 是消费侧行为，不影响 proto 数据)
            event->set_request_body(akto_pb.request_payload());
            event->set_response_body(akto_pb.response_payload());

            // 响应状态
            event->set_response_status(akto_pb.status_code());

            // 常量字段
            for (const auto& cf : config_.constant_fields) {
                impl_->field_applier->applyConstant(*event, cf.target, cf.value);
            }

            spdlog::debug("LogMapper: Akto HttpResponseParam parsed successfully "
                          "(method={}, path={}, ip={})",
                          akto_pb.method(), akto_pb.path(), akto_pb.ip());

            return event;
        }
    }

    // ===== 步骤 2: 将提取的字段写入 HttpAccessEvent =====
    // FieldApplier 通过 protobuf 反射将 map 中的 field_name → value 设置到对应字段
    impl_->field_applier->applyFields(extracted_fields, *event, config_.field_mappings);

    // ===== 步骤 3: 时间戳解析（Regex/Grok 需要额外步骤）=====
    // JSON/Protobuf 格式中时间戳已是数字毫秒，无需额外解析
    if (config_.format == Format::Regex || config_.format == Format::Grok) {
        const auto& ts_cfg = config_.timestamp_config;
        auto it = extracted_fields.find(ts_cfg.source_field);
        if (it != extracted_fields.end()) {
            int64_t ts_ms = impl_->field_applier->parseTimestamp(
                it->second, ts_cfg.formats);
            if (ts_ms >= 0) {
                event->set_timestamp_ms(ts_ms);
            } else {
                spdlog::warn("Failed to parse timestamp from field '{}'='{}'",
                             ts_cfg.source_field, it->second);
            }
        }
    }

    // ===== 步骤 4: 应用常量字段 =====
    // 常量字段不受源数据影响，始终写入固定值
    // 如 collector_id、environment 等
    for (const auto& cf : config_.constant_fields) {
        impl_->field_applier->applyConstant(*event, cf.target, cf.value);
    }

    return event;
}

}  // namespace wge::kafka::mapper
