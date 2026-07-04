#pragma once

/**
 * @file config_loader.h
 * @brief 基于 YAML 的配置加载器
 *
 * 从 YAML 文件加载 AppConfig，支持环境变量替换和配置重载。
 *
 * 线程安全: 所有静态方法均为纯函数，不访问共享状态，天然线程安全。
 */

#include <expected>
#include <string>

#include "config.h"

namespace wge::kafka::config {

/**
 * @brief 配置加载器
 *
 * 提供从 YAML 文件加载应用配置的能力，支持:
 * - YAML 解析 (yaml-cpp)
 * - 环境变量替换 ${VAR_NAME} 语法
 * - 必填字段验证
 * - 配置热重载
 */
class ConfigLoader {
public:
    /**
     * @brief 从 YAML 文件加载完整应用配置
     *
     * 解析指定的 YAML 文件，对字符串值进行环境变量替换，
     * 验证必填字段，缺失字段使用 AppConfig 默认值。
     *
     * @param path YAML 配置文件路径
     * @return std::expected<AppConfig, std::string> 成功返回配置，失败返回错误描述
     *
     * @note 此方法不修改任何静态或全局状态，线程安全。
     */
    [[nodiscard]] static std::expected<AppConfig, std::string>
    loadFromFile(const std::string& path);

    /**
     * @brief 热重载配置
     *
     * 从指定路径加载新配置，新配置中缺失的字段回退到 base 中的值。
     * 适用于 SIGHUP 信号触发的配置重载场景。
     *
     * @param base 当前运行中的配置，作为缺失字段的默认值来源
     * @param path 新 YAML 配置文件路径
     * @return std::expected<AppConfig, std::string> 成功返回合并后的配置，失败返回错误描述
     *
     * @note 调用方负责在替换配置时确保原子性（如使用 atomic shared_ptr）。
     */
    [[nodiscard]] static std::expected<AppConfig, std::string>
    reload(const AppConfig& base, const std::string& path);

    /**
     * @brief 环境变量替换
     *
     * 扫描输入字符串中的 ${VAR_NAME} 模式，替换为 getenv 返回值。
     * 若环境变量未设置，替换为空字符串。
     * 支持 ${VAR_NAME:-default} 语法指定默认值。
     *
     * @param value 原始字符串
     * @return std::string 替换后的字符串
     */
    [[nodiscard]] static std::string substituteEnvVars(const std::string& value);

private:
    /**
     * @brief 验证必填字段
     *
     * @param config 待验证的配置
     * @return std::expected<void, std::string> 成功或错误描述
     */
    [[nodiscard]] static std::expected<void, std::string>
    validate(const AppConfig& config);

    /// @brief 禁止实例化
    ConfigLoader() = delete;
};

}  // namespace wge::kafka::config
