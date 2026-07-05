#pragma once

/**
 * @file mapper.h
 * @brief 日志映射器主类
 *
 * LogMapper 负责将原始日志（JSON / Regex / Protobuf / Grok）
 * 转换为 HttpAccessEvent protobuf 消息。
 *
 * 使用策略模式，根据 MapperConfig::format 分发到不同的具体映射器。
 *
 * 线程安全: LogMapper 持有不可变的 MapperConfig，所有 const 方法线程安全。
 *           instances 可以跨线程共享（通过 shared_ptr）。
 */

#include <expected>
#include <memory>
#include <string>
#include <string_view>

#include "mapper/mapper_config.h"

// 前向声明 protobuf 类型
namespace wge::kafka {
class HttpAccessEvent;
}

namespace wge::kafka::mapper {

/**
 * @brief 日志映射器
 *
 * 将任意格式的原始日志 payload 映射到 HttpAccessEvent。
 *
 * 使用示例:
 * @code
 *   auto config = MapperConfig::loadFromFile("config/log_mapping.yaml");
 *   if (!config) { ... }
 *   auto mapper = std::make_shared<LogMapper>(*config);
 *   auto event = mapper->map(raw_log_line);
 * @endcode
 */
class LogMapper {
public:
    /**
     * @brief 构造函数
     * @param config 不可变的映射配置（构造后内部存储）
     */
    explicit LogMapper(const MapperConfig& config);

    /**
     * @brief 默认析构
     */
    ~LogMapper();

    // 禁止拷贝和移动（持有实现对象）
    LogMapper(const LogMapper&) = delete;
    LogMapper& operator=(const LogMapper&) = delete;
    LogMapper(LogMapper&&) = delete;
    LogMapper& operator=(LogMapper&&) = delete;

    /**
     * @brief 将原始日志 payload 映射为 HttpAccessEvent
     *
     * 根据 config_.format 自动分发到正确的解析器:
     * - Format::Json         → JsonMapper
     * - Format::Protobuf     → 直接反序列化 Protobuf
     * - Format::AktoProtobuf → 直接反序列化 Akto HttpResponseParam
     * Regex/Grok 格式已移除，不再支持。
     *
     * @param raw_payload 原始日志字符串
     * @return std::expected<std::shared_ptr<HttpAccessEvent>, std::string>
     *         成功返回 event，失败返回错误描述
     *
     * @note 返回 shared_ptr 以便调用方可以在多个检测器间共享 event
     * @note 线程安全: 本方法为 const，内部无共享可变状态
     */
    [[nodiscard]] std::expected<std::shared_ptr<HttpAccessEvent>, std::string>
    map(std::string_view raw_payload) const;

    /**
     * @brief 获取当前使用的映射配置（只读）
     */
    [[nodiscard]] const MapperConfig& config() const noexcept {
        return config_;
    }

private:
    /// @brief 映射配置（不可变）
    MapperConfig config_;

    // Pimpl 惯用法：隐藏具体映射器实现细节
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace wge::kafka::mapper
