#pragma once

/**
 * @file mapper_config.h
 * @brief 日志映射配置结构体定义
 *
 * 定义 log_mapping.yaml 对应的 MapperConfig 结构体。
 * 描述如何将原始日志（JSON / Regex / Protobuf / Grok）的字段
 * 映射到 HttpAccessEvent protobuf 消息的各个字段。
 *
 * 线程安全: MapperConfig 在初始化后不可变，读取操作线程安全。
 */

#include <cstdint>
#include <expected>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace wge::kafka::mapper {

// ============================================================================
// 枚举定义
// ============================================================================

/**
 * @brief 日志格式枚举
 */
enum class Format : uint8_t {
    Protobuf = 0,  ///< Protobuf 二进制格式
    Json = 1,      ///< JSON 文本格式
    Regex = 2,     ///< 正则表达式命名分组提取
    Grok = 3,      ///< Grok 模式匹配（类似 Logstash）
};

/**
 * @brief 从字符串解析 Format 枚举
 * @param str 格式名称 ("protobuf", "json", "regex", "grok"，大小写不敏感)
 * @return std::expected<Format, std::string>
 */
[[nodiscard]] std::expected<Format, std::string> parseFormat(
    std::string_view str);

/**
 * @brief Format 枚举转字符串
 */
[[nodiscard]] const char* formatToString(Format fmt) noexcept;

/**
 * @brief 字段映射的目标类型
 */
enum class FieldType : uint8_t {
    String = 0,   ///< 直接赋值 protobuf string 字段
    Int32 = 1,    ///< 转换为 int32
    Int64 = 2,    ///< 转换为 int64
    Bytes = 3,    ///< 解码为 bytes（base64/hex/raw）
    Bool = 4,     ///< 转换为 bool
    Header = 5,   ///< 添加到 repeated Header
};

/**
 * @brief 从字符串解析 FieldType
 */
[[nodiscard]] std::expected<FieldType, std::string> parseFieldType(
    std::string_view str);

// ============================================================================
// 结构体定义
// ============================================================================

/**
 * @brief 单字段映射定义
 *
 * 描述如何从解析后的字段提取一个值并写入 HttpAccessEvent 的某个字段。
 *
 * 示例 (YAML):
 * @code
 *   - source: "request.method"
 *     target: "request_method"
 *     type: String
 * @endcode
 */
struct FieldMapping {
    /// @brief 源字段路径。对于 JSON: 点号分隔的嵌套路径；对于 Regex: 捕获组名
    std::string source{};

    /// @brief 目标字段名。对应 HttpAccessEvent 的 protobuf 字段名
    std::string target{};

    /// @brief 字段类型，决定如何转换和赋值
    FieldType type{FieldType::String};

    /// @brief 是否为必填。若为 true 且 source 缺失，映射失败
    bool required{false};

    /// @brief 默认值（当 optional 且缺失时使用）
    std::optional<std::string> default_value{};
};

/**
 * @brief 常量字段定义
 *
 * 为 HttpAccessEvent 的某个字段设置固定值。
 *
 * 示例 (YAML):
 * @code
 *   - target: "collector_id"
 *     value: "nginx-edge-01"
 * @endcode
 */
struct ConstantField {
    /// @brief 目标字段名
    std::string target{};

    /// @brief 常数值
    std::string value{};
};

/**
 * @brief Header 提取策略
 */
enum class HeaderStrategy : uint8_t {
    None = 0,       ///< 不提取 headers
    Embedded = 1,   ///< Headers 嵌入在 JSON 子对象中
    Prefix = 2,     ///< Headers 以字段名前缀形式散布在根对象中
};

/**
 * @brief Header 提取配置
 */
struct HeaderExtractionConfig {
    /// @brief Header 提取策略
    HeaderStrategy strategy{HeaderStrategy::None};

    /// @brief 当 strategy=Embedded 时，指向包含 headers 的 JSON 路径
    ///        例如 "request.headers"
    std::string embedded_path{};

    /// @brief 当 strategy=Prefix 时，header 字段的前缀
    ///        例如 "http_headers_" 匹配 http_headers_Content-Type 等
    std::string prefix{};

    /// @brief 是否为请求 headers。true: request_headers, false: response_headers
    bool is_request{true};

    /// @brief 前缀提取时，移除前缀后的 key 是否需要进行规范化
    ///        （下划线转横线，首字母大写等）
    bool normalize_keys{true};
};

/**
 * @brief Regex/Grok 模式的 timestamp 解析配置
 */
struct TimestampConfig {
    /// @brief 包含 timestamp 的捕获组名
    std::string source_field{"timestamp"};

    /// @brief 目标字段名 (timestamp_ms 或自定义)
    std::string target_field{"timestamp_ms"};

    /// @brief 尝试的时间格式列表，按优先级排列
    ///        空表示自动尝试以下所有格式:
    ///        1. ISO 8601: "2006-01-02T15:04:05Z07:00"
    ///        2. Nginx: "02/Jan/2006:15:04:05 -0700"
    ///        3. Simple: "2006-01-02 15:04:05"
    ///        4. Unix epoch seconds
    ///        5. Unix epoch milliseconds
    std::vector<std::string> formats{};

    /// @brief 时区。空表示 UTC
    std::string timezone{"UTC"};
};

/**
 * @brief 全部映射配置
 *
 * 对应于 log_mapping.yaml 文件内容。
 * 在启动时加载一次，之后不可变（immutable）。
 *
 * 线程安全: 所有读取操作线程安全。通过 shared_ptr 传递给 LogMapper。
 */
struct MapperConfig {
    /// @brief 日志格式
    Format format{Format::Json};

    /// @brief 字段映射列表
    std::vector<FieldMapping> field_mappings{};

    /// @brief 常量字段列表
    std::vector<ConstantField> constant_fields{};

    /// @brief 请求 header 提取配置
    HeaderExtractionConfig request_headers{};

    /// @brief 响应 header 提取配置
    HeaderExtractionConfig response_headers{};

    /// @brief Regex/Grok 模式下的时间戳解析配置
    TimestampConfig timestamp_config{};

    /// @brief Regex 模式用的正则表达式字符串
    std::string regex_pattern{};

    /// @brief Grok 模式用的 Grok 表达式字符串
    std::string grok_pattern{};

    /// @brief Grok 自定义模式定义 (模式名 -> 正则子表达式)
    std::map<std::string, std::string> grok_custom_patterns{};

    // =========================================================================
    // 静态工厂方法
    // =========================================================================

    /**
     * @brief 从 log_mapping.yaml 文件加载映射配置
     *
     * @param path YAML 文件路径
     * @return std::expected<MapperConfig, std::string>
     */
    [[nodiscard]] static std::expected<MapperConfig, std::string>
    loadFromFile(const std::string& path);
};

}  // namespace wge::kafka::mapper
