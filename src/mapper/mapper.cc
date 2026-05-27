/**
 * @file mapper.cc
 * @brief LogMapper 实现
 */

#include "mapper/mapper.h"

#include <climits>
#include <stdexcept>

#include "http_access.pb.h"
#include "mapper/field_applier.h"
#include "mapper/json_mapper.h"
#include "mapper/regex_mapper.h"
#include "spdlog/spdlog.h"

namespace wge::kafka::mapper {

// ============================================================================
// Pimpl 实现结构体
// ============================================================================

struct LogMapper::Impl {
    MapperConfig config;

    // 具体映射器按需持有
    std::unique_ptr<JsonMapper> json_mapper;
    std::unique_ptr<RegexMapper> regex_mapper;
    std::unique_ptr<FieldApplier> field_applier;

    explicit Impl(const MapperConfig& cfg)
        : config(cfg)
        , field_applier(std::make_unique<FieldApplier>()) {}
};

// ============================================================================
// 构造与析构
// ============================================================================

LogMapper::LogMapper(const MapperConfig& config)
    : config_(config)
    , impl_(std::make_unique<Impl>(config)) {
    // 根据 format 预初始化对应的映射器
    switch (config_.format) {
        case Format::Json:
            impl_->json_mapper = std::make_unique<JsonMapper>();
            spdlog::debug("LogMapper: initialized JsonMapper");
            break;

        case Format::Regex:
            impl_->regex_mapper = std::make_unique<RegexMapper>();
            // 预编译 regex 模式
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
            // Grok 最终也会转换为 Regex，使用 RegexMapper
            impl_->regex_mapper = std::make_unique<RegexMapper>();
            spdlog::debug("LogMapper: initialized RegexMapper for Grok");
            break;

        case Format::Protobuf:
            // Protobuf 无需额外映射器，直接反序列化
            spdlog::debug("LogMapper: using direct Protobuf deserialization");
            break;
    }
}

LogMapper::~LogMapper() = default;

// ============================================================================
// map
// ============================================================================

std::expected<std::shared_ptr<HttpAccessEvent>, std::string> LogMapper::map(
    std::string_view raw_payload) const {
    if (raw_payload.empty()) {
        return std::unexpected(std::string("Empty payload"));
    }

    auto event = std::make_shared<HttpAccessEvent>();

    // 1. 按格式分发提取字段
    std::map<std::string, std::string> extracted_fields;

    switch (config_.format) {
        case Format::Json: {
            if (!impl_->json_mapper) {
                return std::unexpected(
                    std::string("JsonMapper not initialized"));
            }
            auto fields_result = impl_->json_mapper->extract(
                raw_payload, config_.field_mappings);
            if (!fields_result) {
                return std::unexpected(
                    std::string("JsonMapper::extract failed: ") +
                    fields_result.error());
            }
            extracted_fields = std::move(*fields_result);

            // 提取 headers
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
            // 直接反序列化 Protobuf
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
            // 应用常量字段
            for (const auto& cf : config_.constant_fields) {
                impl_->field_applier->applyConstant(*event, cf.target, cf.value);
            }
            return event;
        }
    }

    // 2. 将提取的字段应用到 event
    impl_->field_applier->applyFields(extracted_fields, *event);

    // 3. 处理时间戳（Regex/Grok 格式需要额外解析）
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

    // 4. 应用常量字段
    for (const auto& cf : config_.constant_fields) {
        impl_->field_applier->applyConstant(*event, cf.target, cf.value);
    }

    return event;
}

}  // namespace wge::kafka::mapper
