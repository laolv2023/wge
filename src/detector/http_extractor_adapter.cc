/**
 * @file http_extractor_adapter.cc
 * @brief HttpExtractorAdapter 实现 — WGE Header 提取适配器
 *
 * ## 模块职责
 * HttpExtractorAdapter 是 HttpAccessEvent (protobuf) 和 WGE 引擎之间的桥梁。
 * 它负责：
 * 1. 构建请求/响应 header 的三种视图（小写 key 索引、原始列表、计数）
 * 2. 生成 Lambda 闭包，适配 WGE 引擎的 HeaderFind / HeaderTraversal 接口
 * 3. 缓存状态码和协议版本字符串，避免重复构造
 *
 * ## 核心设计
 * - **双索引结构**: 对每个 header 同时维护:
 *   - `*_header_index_`: unordered_map<string → vector<string>>，支持 O(1) 按 key 查找
 *   - `*_header_list_`: vector<pair<string,string>>，保留原始顺序，支持遍历
 * - **Lambda 捕获 this**: HeaderFind/HeaderTraversal 返回的 Lambda 捕获 this 指针，
 *   因此 adapter 对象生命周期必须长于 Lambda 使用期。
 *   在 worker_pool.cc 中 adapter 在 detect() 栈上创建，Lambda 在同一函数内使用，安全。
 * - **大小写不敏感**: 所有 key 统一转为小写后索引，符合 HTTP header 的 case-insensitive 语义
 */

#include "detector/http_extractor_adapter.h"

#include <algorithm>
#include <cctype>
#include <stdexcept>

#include "http_access.pb.h"
#include "spdlog/spdlog.h"

namespace wge::kafka::detector {

// ============================================================================
// 辅助函数
// ============================================================================

std::string HttpExtractorAdapter::toLower(std::string_view sv) {
    std::string result(sv.size(), '\0');
    std::transform(sv.begin(), sv.end(), result.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return result;
}

// ============================================================================
// 构造
// ============================================================================

HttpExtractorAdapter::HttpExtractorAdapter(const HttpAccessEvent& event)
    : event_(&event) {  // 存储指针而非拷贝，调用方保证生命周期
    // event 由调用方（detect() 的栈上 event 参数）保证非空

    // 构建双索引：header_index_（O(1)查找）和 header_list_（顺序遍历）
    buildRequestHeaderIndex();
    buildResponseHeaderIndex();

    // 预构建状态码缓存：将 int32 状态码转为 string，如 200 → "200"
    cached_status_ = std::to_string(event_->response_status());

    // 预构建协议缓存: "HTTP/" + version，如 "HTTP/1.1"
    std::string version = event_->response_version();
    if (version.empty()) {
        version = "1.1";  // 默认 HTTP/1.1
    }
    cached_protocol_ = "HTTP/";
    cached_protocol_ += version;

    SPDLOG_DEBUG("HttpExtractorAdapter: indexed {} request headers, {} response headers",
                 request_header_count_, response_header_count_);
}

// ============================================================================
// Header 索引构建
// ============================================================================

void HttpExtractorAdapter::buildRequestHeaderIndex() {
    // 清空已有索引，支持重复调用（理论上不会）
    request_header_index_.clear();
    request_header_list_.clear();
    request_header_count_ = 0;

    for (int i = 0; i < event_->request_headers_size(); ++i) {
        const auto& hdr = event_->request_headers(i);
        std::string lower_key = toLower(hdr.key());  // HTTP header key 大小写不敏感

        // 索引 1: key → values 映射，同一 key 可对应多个 value（如 Set-Cookie）
        request_header_index_[lower_key].push_back(hdr.value());

        // 索引 2: 保留原始 key（不做小写转换）和 value 的列表，用于遍历
        request_header_list_.emplace_back(hdr.key(), hdr.value());
        ++request_header_count_;
    }
}

void HttpExtractorAdapter::buildResponseHeaderIndex() {
    response_header_index_.clear();
    response_header_list_.clear();
    response_header_count_ = 0;

    for (int i = 0; i < event_->response_headers_size(); ++i) {
        const auto& hdr = event_->response_headers(i);
        std::string lower_key = toLower(hdr.key());

        // 与请求 header 索引结构一致
        response_header_index_[lower_key].push_back(hdr.value());
        response_header_list_.emplace_back(hdr.key(), hdr.value());
        ++response_header_count_;
    }
}

// ============================================================================
// Request Header 接口
// ============================================================================

HeaderFind HttpExtractorAdapter::requestHeaderFind() const {
    // WGE SDK HeaderFind 签名:
    //   std::function<std::vector<std::string_view>(const std::string& lower_case_key)>
    // SDK 传入的 key 已经是小写，直接在索引中查找
    return [this](const std::string& lower_case_key) -> std::vector<std::string_view> {
        auto it = request_header_index_.find(lower_case_key);
        if (it != request_header_index_.end()) {
            // 返回所有匹配值（vector<string_view> 指向内部 string 数据）
            std::vector<std::string_view> result;
            result.reserve(it->second.size());
            for (const auto& v : it->second) {
                result.emplace_back(v);
            }
            return result;
        }
        return {};
    };
}

HeaderTraversal HttpExtractorAdapter::requestHeaderTraversal() const {
    // WGE SDK HeaderTraversal 签名:
    //   std::function<void(HeaderTraversalCallback)>
    //   HeaderTraversalCallback = std::function<bool(std::string_view lower_case_key, std::string_view value)>
    // callback 返回 false 表示停止遍历
    return [this](Wge::HeaderTraversalCallback callback) {
        for (const auto& [key, value] : request_header_list_) {
            std::string lower_key = toLower(key);
            if (!callback(lower_key, value)) {
                break;  // callback 返回 false，停止遍历
            }
        }
    };
}

size_t HttpExtractorAdapter::requestHeaderCount() const {
    return request_header_count_;
}

// ============================================================================
// Response Header 接口
// ============================================================================

HeaderFind HttpExtractorAdapter::responseHeaderFind() const {
    return [this](const std::string& lower_case_key) -> std::vector<std::string_view> {
        auto it = response_header_index_.find(lower_case_key);
        if (it != response_header_index_.end()) {
            std::vector<std::string_view> result;
            result.reserve(it->second.size());
            for (const auto& v : it->second) {
                result.emplace_back(v);
            }
            return result;
        }
        return {};
    };
}

HeaderTraversal HttpExtractorAdapter::responseHeaderTraversal() const {
    return [this](Wge::HeaderTraversalCallback callback) {
        for (const auto& [key, value] : response_header_list_) {
            std::string lower_key = toLower(key);
            if (!callback(lower_key, value)) {
                break;
            }
        }
    };
}

size_t HttpExtractorAdapter::responseHeaderCount() const {
    return response_header_count_;
}

// ============================================================================
// 辅助访问器
// ============================================================================

std::string_view HttpExtractorAdapter::responseStatusCode() const {
    return cached_status_;
}

std::string_view HttpExtractorAdapter::responseProtocol() const {
    return cached_protocol_;
}

}  // namespace wge::kafka::detector
