#pragma once

/**
 * @file field_applier.h
 * @brief 字段赋值器
 *
 * 将提取后的字段值 (std::map<string,string>) 按类型转换并
 * 写入 HttpAccessEvent protobuf 消息。
 *
 * 线程安全: 所有方法为 const 纯函数，无内部状态，天然线程安全。
 */

#include <cstdint>
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
 * @brief 字段赋值器
 *
 * 负责将 extracted_fields 中的值按照 FieldMapping 定义的类型
 * 转换并写入 HttpAccessEvent 的对应字段。
 *
 * 使用示例:
 * @code
 *   FieldApplier applier;
 *   applier.applyFields(extracted_fields, event);
 * @endcode
 */
class FieldApplier {
public:
    FieldApplier() = default;
    ~FieldApplier() = default;

    /**
     * @brief 批量应用字段映射
     *
     * 遍历 fields map，根据 field_mappings 的类型定义写入 event。
     *
     * @note 此方法假设 field_mappings 来自 MapperConfig，但也可以直接
     *       传入任意 map 和 event。若需要按类型转换，需同时传入 mappings。
     *
     * @param fields  已提取的字段: target → raw_value
     * @param event   目标 HttpAccessEvent
     * @param mappings 字段类型定义（可选）。若为空，所有值按 String 处理。
     */
    void applyFields(
        const std::map<std::string, std::string>& fields,
        HttpAccessEvent& event,
        const std::vector<FieldMapping>& mappings = {}) const;

    /**
     * @brief 按类型设置单个 protobuf 字段
     *
     * @param event     目标 event
     * @param field_name protobuf 字段名
     * @param raw_value  原始字符串值
     * @param type       目标字段类型
     */
    void setField(HttpAccessEvent& event, const std::string& field_name,
                  const std::string& raw_value, FieldType type) const;

    /**
     * @brief 解析时间戳字符串
     *
     * 按 formats 顺序尝试解析，失败返回 -1。
     *
     * @param raw 时间戳字符串
     * @param formats 格式字符串列表（如 "%Y-%m-%dT%H:%M:%S"）
     * @return int64_t epoch milliseconds
     */
    [[nodiscard]] int64_t parseTimestamp(
        const std::string& raw,
        const std::vector<std::string>& formats) const;

    /**
     * @brief 解码 bytes 字段
     *
     * @param raw 原始字符串表示
     * @param encoding 编码方式: "base64" | "hex" | "raw"
     * @return std::string 解码后的字节数据
     */
    [[nodiscard]] std::string decodeBytes(const std::string& raw,
                                          const std::string& encoding) const;

    /**
     * @brief 应用常量字段
     *
     * 为 event 的指定字段设置固定值。
     *
     * @param event 目标 event
     * @param key   protobuf 字段名
     * @param value 常量值
     */
    void applyConstant(HttpAccessEvent& event, const std::string& key,
                       const std::string& value) const;

private:
    /**
     * @brief 通过反射将值写入 protobuf string 字段
     */
    void setStringField(HttpAccessEvent& event, const std::string& field_name,
                        const std::string& value) const;

    /**
     * @brief 通过反射将值写入 protobuf int32 字段
     */
    void setInt32Field(HttpAccessEvent& event, const std::string& field_name,
                       const std::string& value) const;

    /**
     * @brief 通过反射将值写入 protobuf int64 字段
     */
    void setInt64Field(HttpAccessEvent& event, const std::string& field_name,
                       const std::string& value) const;

    /**
     * @brief 通过反射将值写入 protobuf bytes 字段
     */
    void setBytesField(HttpAccessEvent& event, const std::string& field_name,
                       const std::string& value,
                       const std::string& encoding) const;

    /**
     * @brief 通过反射将值写入 protobuf bool 字段
     */
    void setBoolField(HttpAccessEvent& event, const std::string& field_name,
                      const std::string& value) const;

    /**
     * @brief Hex 字符转 nibble
     */
    [[nodiscard]] static int hexCharToNibble(char c) noexcept;

    /**
     * @brief Base64 解码
     */
    [[nodiscard]] static std::string base64Decode(std::string_view encoded);
};

}  // namespace wge::kafka::mapper
