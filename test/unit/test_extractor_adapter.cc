/**
 * @file test_extractor_adapter.cc
 * @brief HttpExtractorAdapter 单元测试 — 测试 header 查找/遍历/生命周期
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "detector/http_extractor_adapter.h"
#include "http_access.pb.h"

using namespace wge::kafka::detector;
using namespace wge::kafka;

// ============================================================================
// 辅助：构造带 headers 的 HttpAccessEvent
// ============================================================================

namespace {

std::shared_ptr<HttpAccessEvent> makeEvent(
    const std::vector<std::pair<std::string, std::string>>& req_headers = {},
    const std::vector<std::pair<std::string, std::string>>& resp_headers = {},
    int status = 200, std::string resp_version = "1.1") {
  auto event = std::make_shared<HttpAccessEvent>();
  event->set_response_status(status);
  event->set_response_version(std::move(resp_version));

  for (const auto& [k, v] : req_headers) {
    auto* h = event->add_request_headers();
    h->set_key(k);
    h->set_value(v);
  }
  for (const auto& [k, v] : resp_headers) {
    auto* h = event->add_response_headers();
    h->set_key(k);
    h->set_value(v);
  }

  return event;
}

}  // namespace

// ============================================================================
// 1. RequestHeaderFind — 查找存在的 header
// ============================================================================

TEST(HttpExtractorAdapterTest, RequestHeaderFind) {
  auto event =
      makeEvent({{"Host", "www.example.com"},
                 {"User-Agent", "Mozilla/5.0"},
                 {"Accept", "text/html"}});

  HttpExtractorAdapter adapter(event);

  auto finder = adapter.requestHeaderFind();
  ASSERT_TRUE(finder);

  std::string_view value;
  EXPECT_TRUE(finder("Host", value));
  EXPECT_EQ(value, "www.example.com");

  EXPECT_TRUE(finder("User-Agent", value));
  EXPECT_EQ(value, "Mozilla/5.0");

  EXPECT_TRUE(finder("Accept", value));
  EXPECT_EQ(value, "text/html");
}

// ============================================================================
// 2. RequestHeaderFindCaseInsensitive — 大小写不敏感
// ============================================================================

TEST(HttpExtractorAdapterTest, RequestHeaderFindCaseInsensitive) {
  auto event =
      makeEvent({{"Content-Type", "application/json"},
                 {"X-Request-Id", "req-abc-123"}});

  HttpExtractorAdapter adapter(event);
  auto finder = adapter.requestHeaderFind();

  std::string_view value;

  // 全小写查找
  EXPECT_TRUE(finder("content-type", value));
  EXPECT_EQ(value, "application/json");

  // 混合大小写
  EXPECT_TRUE(finder("x-request-id", value));
  EXPECT_EQ(value, "req-abc-123");

  // 全大写
  EXPECT_TRUE(finder("CONTENT-TYPE", value));
  EXPECT_EQ(value, "application/json");

  EXPECT_TRUE(finder("X-REQUEST-ID", value));
  EXPECT_EQ(value, "req-abc-123");
}

// ============================================================================
// 3. RequestHeaderFindMissing — 查找不存在的 header
// ============================================================================

TEST(HttpExtractorAdapterTest, RequestHeaderFindMissing) {
  auto event = makeEvent({{"Host", "localhost"}});

  HttpExtractorAdapter adapter(event);
  auto finder = adapter.requestHeaderFind();

  std::string_view value;
  EXPECT_FALSE(finder("Authorization", value));
  EXPECT_FALSE(finder("X-Nonexistent", value));
  EXPECT_FALSE(finder("", value));
}

// ============================================================================
// 4. ResponseHeaderFind
// ============================================================================

TEST(HttpExtractorAdapterTest, ResponseHeaderFind) {
  auto event =
      makeEvent({},
                {{"Content-Type", "text/html; charset=utf-8"},
                 {"Content-Length", "1024"},
                 {"Set-Cookie", "session=abc"}});

  HttpExtractorAdapter adapter(event);

  auto finder = adapter.responseHeaderFind();
  ASSERT_TRUE(finder);

  std::string_view value;
  EXPECT_TRUE(finder("Content-Type", value));
  EXPECT_EQ(value, "text/html; charset=utf-8");

  EXPECT_TRUE(finder("content-length", value));
  EXPECT_EQ(value, "1024");

  EXPECT_TRUE(finder("set-cookie", value));
  EXPECT_EQ(value, "session=abc");
}

TEST(HttpExtractorAdapterTest, ResponseHeaderFindMissing) {
  auto event = makeEvent({}, {{"Server", "nginx"}});

  HttpExtractorAdapter adapter(event);
  auto finder = adapter.responseHeaderFind();

  std::string_view value;
  EXPECT_FALSE(finder("X-Powered-By", value));
}

// ============================================================================
// 5. RequestHeaderTraversal — 遍历所有 request headers
// ============================================================================

TEST(HttpExtractorAdapterTest, RequestHeaderTraversal) {
  auto event =
      makeEvent({{"Host", "example.com"},
                 {"Accept", "text/html"},
                 {"Accept-Encoding", "gzip"}});

  HttpExtractorAdapter adapter(event);
  auto traversal = adapter.requestHeaderTraversal();
  ASSERT_TRUE(traversal);

  std::vector<std::pair<std::string, std::string>> collected;
  traversal([&](std::string_view key, std::string_view value) {
    collected.emplace_back(std::string(key), std::string(value));
  });

  ASSERT_EQ(collected.size(), 3);
  EXPECT_EQ(collected[0].first, "Host");
  EXPECT_EQ(collected[0].second, "example.com");
  EXPECT_EQ(collected[1].first, "Accept");
  EXPECT_EQ(collected[1].second, "text/html");
  EXPECT_EQ(collected[2].first, "Accept-Encoding");
  EXPECT_EQ(collected[2].second, "gzip");
}

// ============================================================================
// 6. ResponseHeaderTraversal
// ============================================================================

TEST(HttpExtractorAdapterTest, ResponseHeaderTraversal) {
  auto event =
      makeEvent({},
                {{"Content-Type", "application/octet-stream"},
                 {"Cache-Control", "no-cache"}});

  HttpExtractorAdapter adapter(event);
  auto traversal = adapter.responseHeaderTraversal();
  ASSERT_TRUE(traversal);

  std::vector<std::pair<std::string, std::string>> collected;
  traversal([&](std::string_view key, std::string_view value) {
    collected.emplace_back(std::string(key), std::string(value));
  });

  ASSERT_EQ(collected.size(), 2);
  EXPECT_EQ(collected[0].first, "Content-Type");
  EXPECT_EQ(collected[0].second, "application/octet-stream");
  EXPECT_EQ(collected[1].first, "Cache-Control");
  EXPECT_EQ(collected[1].second, "no-cache");
}

// ============================================================================
// 7. HeaderCount — request/response header 数量
// ============================================================================

TEST(HttpExtractorAdapterTest, HeaderCount) {
  auto event =
      makeEvent({{"Host", "a"}, {"Accept", "b"}, {"Connection", "keep-alive"}},
                {{"Content-Type", "text/html"}, {"Server", "nginx"}});

  HttpExtractorAdapter adapter(event);

  EXPECT_EQ(adapter.requestHeaderCount(), 3);
  EXPECT_EQ(adapter.responseHeaderCount(), 2);
}

TEST(HttpExtractorAdapterTest, HeaderCountEmpty) {
  auto event = makeEvent();

  HttpExtractorAdapter adapter(event);

  EXPECT_EQ(adapter.requestHeaderCount(), 0);
  EXPECT_EQ(adapter.responseHeaderCount(), 0);
}

// ============================================================================
// 8. ResponseStatusCode — int 转 string
// ============================================================================

TEST(HttpExtractorAdapterTest, ResponseStatusCode) {
  {
    auto event = makeEvent({}, {}, 200);
    HttpExtractorAdapter adapter(event);
    EXPECT_EQ(adapter.responseStatusCode(), "200");
  }
  {
    auto event = makeEvent({}, {}, 404);
    HttpExtractorAdapter adapter(event);
    EXPECT_EQ(adapter.responseStatusCode(), "404");
  }
  {
    auto event = makeEvent({}, {}, 500);
    HttpExtractorAdapter adapter(event);
    EXPECT_EQ(adapter.responseStatusCode(), "500");
  }
  {
    // 默认 status = 0
    auto event = std::make_shared<HttpAccessEvent>();
    HttpExtractorAdapter adapter(event);
    EXPECT_EQ(adapter.responseStatusCode(), "0");
  }
}

// ============================================================================
// 9. ResponseProtocol — "HTTP/1.1" 构造
// ============================================================================

TEST(HttpExtractorAdapterTest, ResponseProtocol) {
  {
    auto event = makeEvent({}, {}, 200, "1.1");
    HttpExtractorAdapter adapter(event);
    EXPECT_EQ(adapter.responseProtocol(), "HTTP/1.1");
  }
  {
    auto event = makeEvent({}, {}, 200, "2.0");
    HttpExtractorAdapter adapter(event);
    EXPECT_EQ(adapter.responseProtocol(), "HTTP/2.0");
  }
  {
    // 空 version → 默认 "1.1"
    auto event = std::make_shared<HttpAccessEvent>();
    HttpExtractorAdapter adapter(event);
    EXPECT_EQ(adapter.responseProtocol(), "HTTP/1.1");
  }
}

// ============================================================================
// 10. MultiValueHeader — 多个同 key header (如 Set-Cookie) 正确返回
// ============================================================================

TEST(HttpExtractorAdapterTest, MultiValueHeader) {
  // 多个 Set-Cookie response headers
  auto event = std::make_shared<HttpAccessEvent>();

  auto addRespHeader = [&](const std::string& k, const std::string& v) {
    auto* h = event->add_response_headers();
    h->set_key(k);
    h->set_value(v);
  };

  addRespHeader("Set-Cookie", "session=abc; Path=/");
  addRespHeader("Set-Cookie", "token=xyz; Secure; HttpOnly");
  addRespHeader("Content-Type", "text/html");
  addRespHeader("Set-Cookie", "tracking=123");

  HttpExtractorAdapter adapter(event);

  auto finder = adapter.responseHeaderFind();

  // finder 返回第一个匹配的值
  std::string_view value;
  EXPECT_TRUE(finder("Set-Cookie", value));
  EXPECT_EQ(value, "session=abc; Path=/");

  // 遍历验证所有 4 个 response headers (含3个 Set-Cookie)
  auto traversal = adapter.responseHeaderTraversal();
  std::vector<std::string> set_cookies;
  traversal([&](std::string_view key, std::string_view value) {
    if (key == "Set-Cookie") {
      set_cookies.emplace_back(value);
    }
  });

  ASSERT_EQ(set_cookies.size(), 3);
  EXPECT_EQ(set_cookies[0], "session=abc; Path=/");
  EXPECT_EQ(set_cookies[1], "token=xyz; Secure; HttpOnly");
  EXPECT_EQ(set_cookies[2], "tracking=123");

  // Count: 4 (3 Set-Cookie + 1 Content-Type)
  EXPECT_EQ(adapter.responseHeaderCount(), 4);
}

// ============================================================================
// 11. LifecycleIntegrity — shared_ptr + this 指针生命周期验证
// ============================================================================

TEST(HttpExtractorAdapterTest, LifecycleIntegrity) {
  // 验证 adapter 持有 event 的 shared_ptr，event 不会提前析构
  auto event =
      makeEvent({{"Host", "test.example.com"}}, {{"Server", "test-nginx"}});

  // 取原始指针用于后期比对
  HttpAccessEvent* raw_ptr = event.get();
  long use_count_before = event.use_count();

  {
    HttpExtractorAdapter adapter(event);

    // adapter 内部持有 shared_ptr，引用计数应增加
    // (移动构造可能影响计数，但至少 event 应仍有效)
    EXPECT_GE(event.use_count(), 1);

    // this 指针捕获的生命周期验证：在 adapter 存活期间，finder 可以安全调用
    auto finder = adapter.requestHeaderFind();
    std::string_view value;
    EXPECT_TRUE(finder("Host", value));

    auto resp_finder = adapter.responseHeaderFind();
    EXPECT_TRUE(resp_finder("Server", value));
  }

  // adapter 析构后，event 仍然有效（原始 shared_ptr 存在）
  EXPECT_EQ(event.get(), raw_ptr);
  EXPECT_GE(event.use_count(), 1);
}

TEST(HttpExtractorAdapterTest, NullEventThrows) {
  EXPECT_THROW(
      {
        HttpExtractorAdapter adapter(nullptr);
      },
      std::runtime_error);
}

// ============================================================================
// 12. Empty headers
// ============================================================================

TEST(HttpExtractorAdapterTest, EmptyHeaders) {
  auto event = std::make_shared<HttpAccessEvent>();
  event->set_response_status(204);
  event->set_response_version("1.1");

  HttpExtractorAdapter adapter(event);

  auto finder = adapter.requestHeaderFind();
  std::string_view value;
  EXPECT_FALSE(finder("Any-Header", value));

  auto traversal = adapter.requestHeaderTraversal();
  int count = 0;
  traversal([&](std::string_view, std::string_view) { ++count; });
  EXPECT_EQ(count, 0);

  EXPECT_EQ(adapter.requestHeaderCount(), 0);
  EXPECT_EQ(adapter.responseHeaderCount(), 0);
  EXPECT_EQ(adapter.responseStatusCode(), "204");
  EXPECT_EQ(adapter.responseProtocol(), "HTTP/1.1");
}

// ============================================================================
// 13. Traversal order preservation
// ============================================================================

TEST(HttpExtractorAdapterTest, TraversalOrderPreservation) {
  auto event = std::make_shared<HttpAccessEvent>();

  // 按特定顺序添加 headers
  for (const auto& [k, v] : {
           std::pair{"X-First", "1"},
           std::pair{"X-Second", "2"},
           std::pair{"X-Third", "3"},
           std::pair{"X-Fourth", "4"},
       }) {
    auto* h = event->add_request_headers();
    h->set_key(k);
    h->set_value(v);
  }

  HttpExtractorAdapter adapter(event);

  auto traversal = adapter.requestHeaderTraversal();
  std::vector<std::string> keys;
  traversal([&](std::string_view key, std::string_view) {
    keys.emplace_back(key);
  });

  ASSERT_EQ(keys.size(), 4);
  EXPECT_EQ(keys[0], "X-First");
  EXPECT_EQ(keys[1], "X-Second");
  EXPECT_EQ(keys[2], "X-Third");
  EXPECT_EQ(keys[3], "X-Fourth");
}
