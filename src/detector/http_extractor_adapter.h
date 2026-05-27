#pragma once

/**
 * @file http_extractor_adapter.h
 * @brief HttpAccessEvent → WGE HeaderFind/HeaderTraversal 适配器
 *
 * HttpExtractorAdapter 将 HttpAccessEvent protobuf 消息适配为 WGE 引擎
 * 所需的 HeaderFind 和 HeaderTraversal 回调接口。
 *
 * 设计要点:
 * - 构造时一次性构建 request/response header 索引 (unordered_map<string,vector<string>>)
 * - 之后所有查找均为 O(1) 平均复杂度
 * - 索引不可变，不存在 rehash 失效问题
 * - responseStatusCode() 和 responseProtocol() 缓存为 string，避免每次构造 string_view
 *
 * 线程安全: 构造后只读，天然线程安全。多个线程可以安全地共享同一实例。
 */

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace wge::kafka {

// 前向声明 protobuf 类型
class HttpAccessEvent;

namespace detector {

// ============================================================================
// 类型别名 — WGE 引擎所需回调签名
// ============================================================================

/**
 * @brief Header 查找回调
 *
 * 给定 header key (大小写不敏感)，返回第一个匹配的 value。
 *
 * @param key  要查找的 header 名称
 * @param value [out] 若找到，写入对应 value
 * @return true  找到至少一个匹配
 * @return false 未找到
 */
using HeaderFind = std::function<bool(std::string_view key, std::string_view& value)>;

/**
 * @brief Header 遍历回调
 *
 * 对每个 header 调用一次 visitor 回调。
 * visitor 签名为 void(std::string_view key, std::string_view value)
 */
using HeaderTraversal = std::function<void(std::function<void(std::string_view, std::string_view)>)>;

// ============================================================================
// HttpExtractorAdapter
// ============================================================================

/**
 * @brief HttpAccessEvent → WGE 检测接口适配器
 *
 * 将 protobuf HttpAccessEvent 的 header 访问模式适配为 WGE 引擎
 * 所需的 O(1) HeaderFind + HeaderTraversal 回调。
 *
 * 使用示例:
 * @code
 *   auto event = std::make_shared<HttpAccessEvent>();
 *   // ... 填充 event ...
 *   HttpExtractorAdapter adapter(event);
 *
 *   // 在 WGE detect() 中使用:
 *   auto tx = engine.makeTransaction();
 *   tx->processRequestHeaders(
 *       adapter.requestHeaderFind(),
 *       adapter.requestHeaderTraversal(),
 *       adapter.requestHeaderCount(),
 *       logCallback, &result,
 *       nullptr, nullptr);
 * @endcode
 */
class HttpExtractorAdapter {
public:
    /**
     * @brief 构造函数 — 构建 header 索引
     *
     * 从 HttpAccessEvent 中提取所有 request_headers 和 response_headers，
     * 构建 unordered_map 索引。key 统一转为小写以实现大小写不敏感查找。
     *
     * @param event 共享所有权的 HttpAccessEvent
     * @throws std::runtime_error 若 event 为 nullptr
     *
     * @note 索引在构造完成后只读，不存在 rehash 失效。
     * @note 采用 vector<string> 存储同一 key 的多个 value (Set-Cookie 等场景)。
     */
    explicit HttpExtractorAdapter(std::shared_ptr<HttpAccessEvent> event);

    /// @brief 默认析构
    ~HttpExtractorAdapter() = default;

    // 禁止拷贝，允许移动
    HttpExtractorAdapter(const HttpExtractorAdapter&) = delete;
    HttpExtractorAdapter& operator=(const HttpExtractorAdapter&) = delete;
    HttpExtractorAdapter(HttpExtractorAdapter&&) noexcept = default;
    HttpExtractorAdapter& operator=(HttpExtractorAdapter&&) noexcept = default;

    // ---- WGE 回调接口 ----

    /**
     * @brief 获取 Request Header 查找 Lambda
     *
     * 返回的 HeaderFind 闭包捕获 [this]，在已构建索引中进行 O(1) 查找。
     * key 大小写不敏感。
     *
     * @return HeaderFind 可调用对象
     */
    [[nodiscard]] HeaderFind requestHeaderFind() const;

    /**
     * @brief 获取 Request Header 遍历 Lambda
     *
     * 返回的 HeaderTraversal 闭包捕获 [this]，遍历所有 request headers。
     * 遍历顺序与原始 HttpAccessEvent 中 header 顺序一致。
     *
     * @return HeaderTraversal 可调用对象
     */
    [[nodiscard]] HeaderTraversal requestHeaderTraversal() const;

    /**
     * @brief 获取 Request Header 总数
     *
     * @return size_t request header 数量 (按 key-value 对计数，包括重复 key)
     */
    [[nodiscard]] size_t requestHeaderCount() const;

    /**
     * @brief 获取 Response Header 查找 Lambda
     *
     * @return HeaderFind 可调用对象
     */
    [[nodiscard]] HeaderFind responseHeaderFind() const;

    /**
     * @brief 获取 Response Header 遍历 Lambda
     *
     * @return HeaderTraversal 可调用对象
     */
    [[nodiscard]] HeaderTraversal responseHeaderTraversal() const;

    /**
     * @brief 获取 Response Header 总数
     *
     * @return size_t response header 数量
     */
    [[nodiscard]] size_t responseHeaderCount() const;

    // ---- 辅助访问器 ----

    /**
     * @brief 获取响应状态码字符串表示
     *
     * 将 int32 response_status 转换为 string 并缓存。
     *
     * @return std::string_view 状态码字符串 (如 "200", "403")，指向内部缓存
     */
    [[nodiscard]] std::string_view responseStatusCode() const;

    /**
     * @brief 获取响应协议字符串
     *
     * 构造 "HTTP/1.1" 格式的协议字符串并缓存。
     * 若 response_version 为空则默认 "1.1"。
     *
     * @return std::string_view 协议字符串 (如 "HTTP/1.1")，指向内部缓存
     */
    [[nodiscard]] std::string_view responseProtocol() const;

    /**
     * @brief 获取底层 HttpAccessEvent
     *
     * @return const std::shared_ptr<HttpAccessEvent>&
     */
    [[nodiscard]] const std::shared_ptr<HttpAccessEvent>& event() const noexcept {
        return event_;
    }

private:
    /**
     * @brief 构建 header 索引
     *
     * 遍历 protobuf repeated Header 字段，统一 key 为小写后存入 unordered_map。
     */
    void buildRequestHeaderIndex();
    void buildResponseHeaderIndex();

    /**
     * @brief 将字符串转为小写 (用于 key 规范化)
     */
    [[nodiscard]] static std::string toLower(std::string_view sv);

    /// @brief 共享所有权的 HttpAccessEvent
    std::shared_ptr<HttpAccessEvent> event_;

    /// @brief Request header 索引: lowercased_key → values
    std::unordered_map<std::string, std::vector<std::string>> request_header_index_;

    /// @brief Response header 索引: lowercased_key → values
    std::unordered_map<std::string, std::vector<std::string>> response_header_index_;

    /// @brief Request header 总对数 (用于 count)
    size_t request_header_count_{0};

    /// @brief Response header 总对数
    size_t response_header_count_{0};

    /// @brief 缓存的状态码字符串
    mutable std::string cached_status_{};

    /// @brief 缓存的协议字符串
    mutable std::string cached_protocol_{};

    /// @brief 缓存的 request header 列表 (保持原始插入顺序，用于遍历)
    std::vector<std::pair<std::string, std::string>> request_header_list_{};

    /// @brief 缓存的 response header 列表
    std::vector<std::pair<std::string, std::string>> response_header_list_{};
};

}  // namespace detector
}  // namespace wge::kafka
