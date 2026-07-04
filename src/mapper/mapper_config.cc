/**
 * @file mapper_config.cc
 * @brief MapperConfig 实现：枚举解析和 YAML 加载
 */

#include "mapper/mapper_config.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <stdexcept>

#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"

namespace wge::kafka::mapper {

// ============================================================================
// 枚举解析
// ============================================================================

std::expected<Format, std::string> parseFormat(std::string_view str) {
    // 转换为小写比较
    std::string lower;
    lower.reserve(str.size());
    for (char c : str) {
        lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    }

    if (lower == "protobuf") return Format::Protobuf;
    if (lower == "json") return Format::Json;
    if (lower == "regex") return Format::Regex;
    if (lower == "grok") return Format::Grok;
    if (lower == "akto_protobuf" || lower == "akto-protobuf" || lower == "akto")
        return Format::AktoProtobuf;

    return std::unexpected(
        std::string("Unknown format: '") + std::string(str) +
        "'. Expected one of: protobuf, json, regex, grok, akto_protobuf");
}

const char* formatToString(Format fmt) noexcept {
    switch (fmt) {
        case Format::Protobuf:     return "protobuf";
        case Format::Json:         return "json";
        case Format::Regex:        return "regex";
        case Format::Grok:         return "grok";
        case Format::AktoProtobuf: return "akto_protobuf";
    }
    return "unknown";
}

std::expected<FieldType, std::string> parseFieldType(std::string_view str) {
    std::string lower;
    lower.reserve(str.size());
    for (char c : str) {
        lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    }

    if (lower == "string") return FieldType::String;
    if (lower == "int32") return FieldType::Int32;
    if (lower == "int64") return FieldType::Int64;
    if (lower == "bytes") return FieldType::Bytes;
    if (lower == "bool") return FieldType::Bool;
    if (lower == "header") return FieldType::Header;

    return std::unexpected(
        std::string("Unknown field type: '") + std::string(str) +
        "'. Expected one of: string, int32, int64, bytes, bool, header");
}

// ============================================================================
// YAML 解析辅助函数
// ============================================================================

namespace {

/**
 * @brief 解析 HeaderStrategy
 */
std::expected<HeaderStrategy, std::string> parseHeaderStrategy(
    std::string_view str) {
    std::string lower;
    lower.reserve(str.size());
    for (char c : str) {
        lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    }

    if (lower == "none") return HeaderStrategy::None;
    if (lower == "embedded") return HeaderStrategy::Embedded;
    if (lower == "prefix") return HeaderStrategy::Prefix;

    return std::unexpected(
        std::string("Unknown header strategy: '") + std::string(str) +
        "'. Expected one of: none, embedded, prefix");
}

/**
 * @brief 解析 HeaderExtractionConfig 从 YAML
 */
std::expected<void, std::string> parseHeaderExtraction(const YAML::Node& node,
                               HeaderExtractionConfig& cfg) {
    if (!node) return {};

    if (node["strategy"]) {
        auto result = parseHeaderStrategy(node["strategy"].as<std::string>());
        if (result) {
            cfg.strategy = *result;
        } else {
            return std::unexpected(result.error());
        }
    }

    if (node["embedded_path"]) {
        cfg.embedded_path = node["embedded_path"].as<std::string>();
    }
    if (node["prefix"]) {
        cfg.prefix = node["prefix"].as<std::string>();
    }
    if (node["is_request"]) {
        cfg.is_request = node["is_request"].as<bool>();
    }
    if (node["normalize_keys"]) {
        cfg.normalize_keys = node["normalize_keys"].as<bool>();
    }
    return {};
}

/**
 * @brief 解析 TimestampConfig 从 YAML
 */
void parseTimestampConfig(const YAML::Node& node, TimestampConfig& cfg) {
    if (!node) return;

    if (node["source_field"]) {
        cfg.source_field = node["source_field"].as<std::string>();
    }
    if (node["target_field"]) {
        cfg.target_field = node["target_field"].as<std::string>();
    }
    if (node["formats"] && node["formats"].IsSequence()) {
        cfg.formats.clear();
        for (const auto& fmt : node["formats"]) {
            cfg.formats.emplace_back(fmt.as<std::string>());
        }
    }
    if (node["timezone"]) {
        cfg.timezone = node["timezone"].as<std::string>();
    }
}

/**
 * @brief 解析单个 FieldMapping 从 YAML
 */
std::expected<FieldMapping, std::string> parseFieldMapping(
    const YAML::Node& node) {
    FieldMapping mapping;

    if (!node["source"]) {
        return std::unexpected("FieldMapping missing 'source'");
    }
    mapping.source = node["source"].as<std::string>();

    if (!node["target"]) {
        return std::unexpected(
            "FieldMapping missing 'target' for source='" + mapping.source +
            "'");
    }
    mapping.target = node["target"].as<std::string>();

    if (node["type"]) {
        auto type_result =
            parseFieldType(node["type"].as<std::string>());
        if (!type_result) return std::unexpected(type_result.error());
        mapping.type = *type_result;
    }

    if (node["required"]) {
        mapping.required = node["required"].as<bool>();
    }

    if (node["default"]) {
        mapping.default_value = node["default"].as<std::string>();
    }

    return mapping;
}

/**
 * @brief 解析 ConstantField 从 YAML
 */
std::expected<ConstantField, std::string> parseConstantField(
    const YAML::Node& node) {
    ConstantField field;

    if (!node["target"]) {
        return std::unexpected("ConstantField missing 'target'");
    }
    field.target = node["target"].as<std::string>();

    if (!node["value"]) {
        return std::unexpected(
            "ConstantField missing 'value' for target='" + field.target +
            "'");
    }
    field.value = node["value"].as<std::string>();

    return field;
}

}  // namespace

// ============================================================================
// loadFromFile
// ============================================================================

std::expected<MapperConfig, std::string> MapperConfig::loadFromFile(
    const std::string& path) {
    MapperConfig config;

    YAML::Node root;
    try {
        root = YAML::LoadFile(path);
    } catch (const YAML::BadFile& e) {
        return std::unexpected(
            std::string("Failed to open mapping config file: ") + path +
            " - " + e.what());
    } catch (const YAML::ParserException& e) {
        return std::unexpected(
            std::string("YAML parse error in mapping config: ") + path +
            " - " + e.what());
    }

    if (!root || root.IsNull()) {
        return std::unexpected(
            "Empty mapping config: " + path);
    }

    try {
        // 解析 format（必填）
        if (!root["format"]) {
            return std::unexpected(
                "Mapping config missing required field 'format' in: " +
                path);
        }
        auto fmt_result = parseFormat(root["format"].as<std::string>());
        if (!fmt_result) return std::unexpected(fmt_result.error());
        config.format = *fmt_result;

        // 解析 regex_pattern（regex 格式必填）
        if (config.format == Format::Regex) {
            if (!root["regex_pattern"]) {
                return std::unexpected(
                    "Regex format requires 'regex_pattern' field in: " +
                    path);
            }
            config.regex_pattern = root["regex_pattern"].as<std::string>();
        }

        // 解析 grok_pattern（grok 格式必填）
        if (config.format == Format::Grok) {
            if (!root["grok_pattern"]) {
                return std::unexpected(
                    "Grok format requires 'grok_pattern' field in: " + path);
            }
            config.grok_pattern = root["grok_pattern"].as<std::string>();

            // 自定义 grok 模式
            if (root["grok_custom_patterns"]) {
                for (const auto& kv : root["grok_custom_patterns"]) {
                    config.grok_custom_patterns[kv.first.as<std::string>()] =
                        kv.second.as<std::string>();
                }
            }
        }

        // 解析 field_mappings
        if (root["field_mappings"] && root["field_mappings"].IsSequence()) {
            for (const auto& item : root["field_mappings"]) {
                auto mapping = parseFieldMapping(item);
                if (!mapping) return std::unexpected(mapping.error());
                config.field_mappings.emplace_back(std::move(*mapping));
            }
        }

        // 解析 constant_fields
        if (root["constant_fields"] && root["constant_fields"].IsSequence()) {
            for (const auto& item : root["constant_fields"]) {
                auto field = parseConstantField(item);
                if (!field) return std::unexpected(field.error());
                config.constant_fields.emplace_back(std::move(*field));
            }
        }

        // 解析 request_headers
        if (root["request_headers"]) {
            auto req_result = parseHeaderExtraction(root["request_headers"],
                                  config.request_headers);
            if (!req_result) return std::unexpected(req_result.error());
        }

        // 解析 response_headers
        if (root["response_headers"]) {
            auto resp_result = parseHeaderExtraction(root["response_headers"],
                                  config.response_headers);
            if (!resp_result) return std::unexpected(resp_result.error());
        }

        // 解析 timestamp_config
        if (root["timestamp"]) {
            parseTimestampConfig(root["timestamp"], config.timestamp_config);
        }

    } catch (const YAML::Exception& e) {
        return std::unexpected(
            std::string("YAML traversal error in mapping config: ") +
            e.what());
    } catch (const std::exception& e) {
        return std::unexpected(
            std::string("Unexpected error parsing mapping config: ") +
            e.what());
    }

    spdlog::info("Loaded mapping config from {}: format={}, {} field mappings",
                 path, formatToString(config.format),
                 config.field_mappings.size());

    return config;
}

}  // namespace wge::kafka::mapper
