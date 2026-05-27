/**
 * @file http_extractor_adapter.cc
 * @brief HttpExtractorAdapter 实现
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

HttpExtractorAdapter::HttpExtractorAdapter(std::shared_ptr<HttpAccessEvent> event)
    : event_(std::move(event)) {
    if (!event_) {
        throw std::runtime_error("HttpExtractorAdapter: event is null");
    }

    buildRequestHeaderIndex();
    buildResponseHeaderIndex();

    // 预构建状态码缓存
    cached_status_ = std::to_string(event_->response_status());

    // 预构建协议缓存: "HTTP/" + version
    std::string version = event_->response_version();
    if (version.empty()) {
        version = "1.1";
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
    request_header_index_.clear();
    request_header_list_.clear();
    request_header_count_ = 0;

    for (int i = 0; i < event_->request_headers_size(); ++i) {
        const auto& hdr = event_->request_headers(i);
        std::string lower_key = toLower(hdr.key());

        request_header_index_[lower_key].push_back(hdr.value());
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

        response_header_index_[lower_key].push_back(hdr.value());
        response_header_list_.emplace_back(hdr.key(), hdr.value());
        ++response_header_count_;
    }
}

// ============================================================================
// Request Header 接口
// ============================================================================

HeaderFind HttpExtractorAdapter::requestHeaderFind() const {
    // 捕获 this 指针 (adapter 生命周期需长于返回的 Lambda)
    return [this](std::string_view key, std::string_view& value) -> bool {
        std::string lower_key = toLower(key);
        auto it = request_header_index_.find(lower_key);
        if (it != request_header_index_.end() && !it->second.empty()) {
            value = it->second[0];  // 返回第一个值
            return true;
        }
        return false;
    };
}

HeaderTraversal HttpExtractorAdapter::requestHeaderTraversal() const {
    return [this](std::function<void(std::string_view, std::string_view)> visitor) {
        for (const auto& [key, value] : request_header_list_) {
            visitor(key, value);
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
    return [this](std::string_view key, std::string_view& value) -> bool {
        std::string lower_key = toLower(key);
        auto it = response_header_index_.find(lower_key);
        if (it != response_header_index_.end() && !it->second.empty()) {
            value = it->second[0];
            return true;
        }
        return false;
    };
}

HeaderTraversal HttpExtractorAdapter::responseHeaderTraversal() const {
    return [this](std::function<void(std::string_view, std::string_view)> visitor) {
        for (const auto& [key, value] : response_header_list_) {
            visitor(key, value);
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
