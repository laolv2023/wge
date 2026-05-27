/**
 * @file json_mapper.cc
 * @brief JsonMapper 实现
 */

#include "mapper/json_mapper.h"

#include <charconv>
#include <sstream>

#include "http_access.pb.h"
#include "simdjson.h"
#include "spdlog/spdlog.h"

namespace wge::kafka::mapper {

// ============================================================================
// 辅助函数
// ============================================================================

namespace {

/**
 * @brief 将 simdjson element 转为字符串表示
 */
std::string simdjsonValueToString(simdjson::ondemand::value element) {
    using namespace simdjson;
    std::string result;

    switch (element.type()) {
        case simdjson_type::string: {
            std::string_view sv = element.get_string().value_unsafe();
            result.assign(sv.data(), sv.size());
            break;
        }
        case simdjson_type::int64: {
            result = std::to_string(element.get_int64().value_unsafe());
            break;
        }
        case simdjson_type::uint64: {
            result = std::to_string(element.get_uint64().value_unsafe());
            break;
        }
        case simdjson_type::double_type: {
            result = std::to_string(element.get_double().value_unsafe());
            break;
        }
        case simdjson_type::boolean: {
            result = element.get_bool().value_unsafe() ? "true" : "false";
            break;
        }
        case simdjson_type::null_value: {
            result = "";
            break;
        }
        default:
            result = "";
            break;
    }

    return result;
}

/**
 * @brief 将点号分隔的路径拆分为各段
 */
std::vector<std::string_view> splitDotPath(std::string_view path) {
    std::vector<std::string_view> segments;
    size_t start = 0;
    size_t pos;

    while ((pos = path.find('.', start)) != std::string_view::npos) {
        if (pos > start) {
            segments.emplace_back(path.substr(start, pos - start));
        }
        start = pos + 1;
    }
    if (start < path.size()) {
        segments.emplace_back(path.substr(start));
    }

    return segments;
}

/**
 * @brief 将 simdjson dom::element 转为字符串表示
 */
std::string domElementToString(simdjson::dom::element element) {
    using namespace simdjson;
    switch (element.type()) {
        case dom::element_type::STRING: {
            std::string_view sv = element.get_string().value_unsafe();
            return std::string(sv);
        }
        case dom::element_type::INT64:
            return std::to_string(element.get_int64().value_unsafe());
        case dom::element_type::UINT64:
            return std::to_string(element.get_uint64().value_unsafe());
        case dom::element_type::DOUBLE:
            return std::to_string(element.get_double().value_unsafe());
        case dom::element_type::BOOL:
            return element.get_bool().value_unsafe() ? "true" : "false";
        case dom::element_type::NULL_VALUE:
            return std::string("");
        default:
            return std::string("");
    }
}

}  // namespace

// ============================================================================
// extractJsonPath
// ============================================================================

std::expected<std::string, std::string> JsonMapper::extractJsonPath(
    std::string_view raw_payload, std::string_view dot_path) const {
    if (raw_payload.empty()) {
        return std::unexpected(std::string("Empty JSON payload"));
    }
    if (dot_path.empty()) {
        return std::unexpected(std::string("Empty path"));
    }

    simdjson::ondemand::parser parser;
    simdjson::ondemand::document doc;
    simdjson::ondemand::value current;

    auto error = parser.iterate(raw_payload).get(doc);
    if (error) {
        return std::unexpected(
            std::string("JSON parse error: ") +
            simdjson::error_message(error));
    }

    // 获取文档根
    error = doc.get_value().get(current);
    if (error) {
        return std::unexpected(
            std::string("Failed to get JSON root: ") +
            simdjson::error_message(error));
    }

    auto segments = splitDotPath(dot_path);
    if (segments.empty()) {
        return std::unexpected(
            std::string("Invalid path: '") + std::string(dot_path) + "'");
    }

    // 逐层导航
    for (size_t i = 0; i < segments.size(); ++i) {
        std::string_view key = segments[i];
        bool is_last = (i == segments.size() - 1);

        simdjson::ondemand::object obj;
        error = current.get_object().get(obj);
        if (error) {
            return std::unexpected(
                std::string("Path segment '") + std::string(key) +
                "': expected object at '" + std::string(dot_path) +
                "', error: " + simdjson::error_message(error));
        }

        error = obj[key].get(current);
        if (error) {
            if (error == simdjson::error_code::NO_SUCH_FIELD) {
                return std::unexpected(
                    std::string("Field not found: '") + std::string(key) +
                    "' in path '" + std::string(dot_path) + "'");
            }
            return std::unexpected(
                std::string("Error accessing '") + std::string(key) +
                "' in path '" + std::string(dot_path) +
                "': " + simdjson::error_message(error));
        }

        // 最后一个段：转为字符串
        if (is_last) {
            return simdjsonValueToString(current);
        }
    }

    return std::unexpected(
        std::string("Path traversal ended unexpectedly for '") +
        std::string(dot_path) + "'");
}

// ============================================================================
// extract
// ============================================================================

std::expected<std::map<std::string, std::string>, std::string>
JsonMapper::extract(std::string_view raw_payload,
                    const std::vector<FieldMapping>& field_mappings) const {
    if (raw_payload.empty()) {
        return std::unexpected(std::string("Empty JSON payload"));
    }

    // 一次性解析 JSON，避免每个字段重复解析
    simdjson::dom::parser parser;
    simdjson::dom::element doc;
    simdjson::padded_string padded(raw_payload);
    auto error = parser.parse(padded).get(doc);
    if (error) {
        return std::unexpected(
            std::string("JSON parse error: ") +
            simdjson::error_message(error));
    }

    std::map<std::string, std::string> result;

    for (const auto& mapping : field_mappings) {
        if (mapping.source.empty()) continue;

        auto segments = splitDotPath(mapping.source);
        if (segments.empty()) {
            if (mapping.required) {
                return std::unexpected(
                    std::string("Required field '") + mapping.source +
                    "' (→ " + mapping.target + ") has empty path");
            }
            if (mapping.default_value.has_value()) {
                result[mapping.target] = *mapping.default_value;
            }
            continue;
        }

        // 从根节点出发导航
        simdjson::dom::element current = doc;
        bool found = true;
        std::string nav_error;

        for (size_t i = 0; i < segments.size(); ++i) {
            std::string_view key = segments[i];
            bool is_last = (i == segments.size() - 1);

            if (current.type() != simdjson::dom::element_type::OBJECT) {
                found = false;
                nav_error = std::string("Path segment '") + std::string(key) +
                    "': expected object at '" + mapping.source + "'";
                break;
            }

            simdjson::dom::object obj = current.get_object().value_unsafe();
            auto child_err = obj[key];
            if (child_err.error()) {
                found = false;
                if (child_err.error() != simdjson::NO_SUCH_FIELD) {
                    nav_error = std::string("Error accessing '") +
                        std::string(key) + "' in path '" + mapping.source +
                        "': " + simdjson::error_message(child_err.error());
                }
                break;
            }

            simdjson::dom::element child = child_err.value_unsafe();

            if (is_last) {
                result[mapping.target] = domElementToString(child);
                spdlog::trace("Field '{}' → '{}' = '{}'", mapping.source,
                              mapping.target, result[mapping.target]);
                break;
            }
            current = child;
        }

        if (!found) {
            if (mapping.required) {
                if (nav_error.empty()) {
                    return std::unexpected(
                        std::string("Required field '") + mapping.source +
                        "' (→ " + mapping.target + ") not found");
                }
                return std::unexpected(nav_error);
            }
            if (mapping.default_value.has_value()) {
                result[mapping.target] = *mapping.default_value;
                spdlog::trace("Field '{}' → '{}': using default '{}'",
                              mapping.source, mapping.target,
                              *mapping.default_value);
            } else {
                spdlog::trace("Field '{}' → '{}': not found, skipping",
                              mapping.source, mapping.target);
            }
        }
    }

    return result;
}

// ============================================================================
// extractHeaders
// ============================================================================

void JsonMapper::extractHeaders(std::string_view raw_payload,
                                const HeaderExtractionConfig& req_cfg,
                                const HeaderExtractionConfig& resp_cfg,
                                HttpAccessEvent& event) const {
    // 请求 headers
    switch (req_cfg.strategy) {
        case HeaderStrategy::Embedded:
            extractEmbeddedHeaders(raw_payload, req_cfg.embedded_path,
                                   true, event);
            break;
        case HeaderStrategy::Prefix:
            extractPrefixHeaders(raw_payload, req_cfg.prefix,
                                 req_cfg.normalize_keys, true, event);
            break;
        case HeaderStrategy::None:
        default:
            break;
    }

    // 响应 headers
    switch (resp_cfg.strategy) {
        case HeaderStrategy::Embedded:
            extractEmbeddedHeaders(raw_payload, resp_cfg.embedded_path,
                                   false, event);
            break;
        case HeaderStrategy::Prefix:
            extractPrefixHeaders(raw_payload, resp_cfg.prefix,
                                 resp_cfg.normalize_keys, false, event);
            break;
        case HeaderStrategy::None:
        default:
            break;
    }
}

// ============================================================================
// extractEmbeddedHeaders
// ============================================================================

void JsonMapper::extractEmbeddedHeaders(std::string_view raw_payload,
                                        std::string_view embedded_path,
                                        bool is_request,
                                        HttpAccessEvent& event) const {
    if (embedded_path.empty()) {
        spdlog::warn("Embedded headers path is empty, skipping");
        return;
    }

    simdjson::ondemand::parser parser;
    simdjson::ondemand::document doc;
    simdjson::ondemand::value current;

    auto error = parser.iterate(raw_payload).get(doc);
    if (error) {
        spdlog::warn("Failed to parse JSON for embedded headers: {}",
                     simdjson::error_message(error));
        return;
    }

    error = doc.get_value().get(current);
    if (error) {
        spdlog::warn("Failed to get JSON root for embedded headers: {}",
                     simdjson::error_message(error));
        return;
    }

    // 导航到 embedded_path
    auto segments = splitDotPath(embedded_path);
    for (size_t i = 0; i < segments.size(); ++i) {
        simdjson::ondemand::object obj;
        error = current.get_object().get(obj);
        if (error) {
            spdlog::warn("Embedded headers path '{}': not an object at segment '{}'",
                         embedded_path, segments[i]);
            return;
        }

        error = obj[segments[i]].get(current);
        if (error) {
            spdlog::warn("Embedded headers: field '{}' not found in path '{}'",
                         segments[i], embedded_path);
            return;
        }
    }

    // current 现在应该是一个 object，包含所有 header kv 对
    simdjson::ondemand::object headers_obj;
    error = current.get_object().get(headers_obj);
    if (error) {
        spdlog::warn("Embedded headers: path '{}' does not point to an object",
                     embedded_path);
        return;
    }

    for (auto field : headers_obj) {
        std::string_view key_raw = field.unescaped_key().value_unsafe();
        std::string value = simdjsonValueToString(field.value());

        auto* header = is_request ? event.add_request_headers()
                                  : event.add_response_headers();
        header->set_key(std::string(key_raw));
        header->set_value(value);
    }

    spdlog::debug("Extracted {} embedded {} headers",
                  is_request ? "request" : "response",
                  is_request ? event.request_headers_size()
                             : event.response_headers_size());
}

// ============================================================================
// extractPrefixHeaders
// ============================================================================

void JsonMapper::extractPrefixHeaders(std::string_view raw_payload,
                                      std::string_view prefix,
                                      bool normalize, bool is_request,
                                      HttpAccessEvent& event) const {
    if (prefix.empty()) {
        spdlog::warn("Prefix headers: empty prefix, skipping");
        return;
    }

    simdjson::ondemand::parser parser;
    simdjson::ondemand::document doc;
    simdjson::ondemand::object root_obj;

    auto error = parser.iterate(raw_payload).get(doc);
    if (error) {
        spdlog::warn("Failed to parse JSON for prefix headers: {}",
                     simdjson::error_message(error));
        return;
    }

    error = doc.get_object().get(root_obj);
    if (error) {
        spdlog::warn("JSON root is not an object for prefix headers: {}",
                     simdjson::error_message(error));
        return;
    }

    int count = 0;
    for (auto field : root_obj) {
        std::string_view key_raw = field.unescaped_key().value_unsafe();

        // 检查是否以 prefix 开头
        if (key_raw.size() <= prefix.size()) continue;
        if (key_raw.substr(0, prefix.size()) != prefix) continue;

        // 移除前缀
        std::string_view header_name = key_raw.substr(prefix.size());
        std::string value = simdjsonValueToString(field.value());

        std::string final_key;
        if (normalize) {
            // 规范化: 下划线转横线，首字母大写
            final_key.reserve(header_name.size());
            bool new_word = true;
            for (char c : header_name) {
                if (c == '_') {
                    final_key.push_back('-');
                    new_word = true;
                } else if (new_word && std::isalpha(static_cast<unsigned char>(c))) {
                    final_key.push_back(std::toupper(static_cast<unsigned char>(c)));
                    new_word = false;
                } else {
                    final_key.push_back(c);
                    new_word = false;
                }
            }
        } else {
            final_key.assign(header_name.data(), header_name.size());
        }

        auto* header = is_request ? event.add_request_headers()
                                  : event.add_response_headers();
        header->set_key(final_key);
        header->set_value(value);
        ++count;
    }

    spdlog::debug("Extracted {} prefix {} headers (prefix='{}')",
                  count, is_request ? "request" : "response", prefix);
}

}  // namespace wge::kafka::mapper
