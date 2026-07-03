#pragma once

/**
 * @file json_mapper.h
 * @brief JSON 日志映射器
 *
 * 使用 simdjson 进行高性能 JSON 解析，支持嵌套路径提取和 header 提取。
 *
 * 线程安全: 所有方法为 const，内部无共享可变状态，天然线程安全。
 *           多个线程可以安全地共享同一个 JsonMapper 实例。
 */

#include <expected>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "mapper/mapper_config.h"

namespace wge::kafka {
class HttpAccessEvent;
}

namespace wge::kafka::mapper {

/**
 * @brief JSON 日志映射器
 *
 * 使用 simdjson On-Demand API 解析 JSON 日志，
 * 支持点号分隔的嵌套路径提取和多种 header 提取策略。
 */
class JsonMapper {
public:
    JsonMapper() = default;
    ~JsonMapper() = default;

    // 可移动但不可拷贝（simdjson parser 内部状态）
    JsonMapper(JsonMapper&&) = default;
    JsonMapper& operator=(JsonMapper&&) = default;
    JsonMapper(const JsonMapper&) = delete;
    JsonMapper& operator=(const JsonMapper&) = delete;

    /**
     * @brief 从 JSON payload 中提取字段
     *
     * 遍历 field_mappings，对每个 mapping 调用 extractJsonPath()
     * 获取对应的值。
     *
     * @param raw_payload 原始 JSON 字符串
     * @param field_mappings 字段映射列表
     * @return std::expected<std::map<std::string, std::string>, std::string>
     *         成功返回 "target → value" 映射
     */
    [[nodiscard]] std::expected<std::map<std::string, std::string>, std::string>
    extract(std::string_view raw_payload,
            const std::vector<FieldMapping>& field_mappings) const;

    /**
     * @brief 从 JSON 中提取 headers 并写入 event
     *
     * 根据 request_headers 和 response_headers 的配置策略:
     * - Embedded: 从指定路径提取 header 子对象
     * - Prefix: 提取以指定前缀开头的顶层字段
     *
     * @param raw_payload 原始 JSON 字符串
     * @param req_cfg 请求 header 提取配置
     * @param resp_cfg 响应 header 提取配置
     * @param event 目标 HttpAccessEvent
     */
    void extractHeaders(std::string_view raw_payload,
                        const HeaderExtractionConfig& req_cfg,
                        const HeaderExtractionConfig& resp_cfg,
                        HttpAccessEvent& event) const;

    /**
     * @brief 从 JSON 中提取嵌套路径的值
     *
     * 支持点号分隔的路径，例如:
     * - "request.method" → json["request"]["method"]
     * - "request.headers.User-Agent" → json["request"]["headers"]["User-Agent"]
     *
     * @param raw_payload 原始 JSON 字符串
     * @param dot_path 点号分隔的路径
     * @return std::expected<std::string, std::string> 提取的字符串值
     */
    [[nodiscard]] std::expected<std::string, std::string>
    extractJsonPath(std::string_view raw_payload,
                    std::string_view dot_path) const;

private:
    /**
     * @brief 从 JSON 对象中提取子对象的所有 kv 对作为 headers
     *
     * @param raw_payload 原始 JSON 字符串
     * @param embedded_path 指向 header 子对象的路径
     * @param is_request true 则添加到 request_headers，false 则为 response_headers
     * @param event 目标 event
     */
    void extractEmbeddedHeaders(std::string_view raw_payload,
                                std::string_view embedded_path,
                                bool is_request,
                                HttpAccessEvent& event) const;

    /**
     * @brief 提取以 prefix 开头的顶层字段作为 headers
     *
     * @param raw_payload 原始 JSON 字符串
     * @param prefix header 字段前缀
     * @param normalize 是否规范化 header key
     * @param is_request true 则添加到 request_headers
     * @param event 目标 event
     */
    void extractPrefixHeaders(std::string_view raw_payload,
                              std::string_view prefix, bool normalize,
                              bool is_request,
                              HttpAccessEvent& event) const;
};

}  // namespace wge::kafka::mapper
