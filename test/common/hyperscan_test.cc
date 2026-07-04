/**
 * Copyright (c) 2024-2025 Stone Rhino and contributors.
 *
 * MIT License (http://opensource.org/licenses/MIT)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#include <filesystem>
#include <future>

#include <gtest/gtest.h>

#include "common/hyperscan/scanner.h"

TEST(HyperscanTest, greedy) {
  Wge::Common::Hyperscan::Scanner scanner(
      std::make_shared<Wge::Common::Hyperscan::HsDataBase>("a+", false, false, true, false, false));
  int count = 0;

  std::future<void> result = std::async([&]() {
    scanner.registMatchCallback(
        [](uint64_t id, unsigned long long from, unsigned long long to, unsigned int flags,
           void* user_data) -> int {
          int* count = static_cast<int*>(user_data);
          *count += 1;
          return 0;
        },
        &count);

    scanner.blockScan("aaaabaaaa", Wge::Common::Hyperscan::Scanner::ScanMode::Normal, nullptr,
                      nullptr);
    EXPECT_EQ(count, 8);

    count = 0;
    scanner.blockScan("aaaabaaaa", Wge::Common::Hyperscan::Scanner::ScanMode::GreedyAndGlobal,
                      nullptr, nullptr);
    EXPECT_EQ(count, 2);

    count = 0;
    scanner.blockScan("aaaabaaaa", Wge::Common::Hyperscan::Scanner::ScanMode::Greedy, nullptr,
                      nullptr);
    EXPECT_EQ(count, 1);
  });

  result.get();
}

TEST(HyperscanTest, serialize) {
  const char* serialize_dir = "/tmp/HyperscanTest";
  Wge::Common::Hyperscan::Scanner scanner(std::make_shared<Wge::Common::Hyperscan::HsDataBase>(
      "a+", false, false, true, false, true, serialize_dir));

  std::string serialize_file_path = serialize_dir;
  serialize_file_path += "/";
  serialize_file_path += scanner.databaseSha1();
  serialize_file_path += ".bdb";
  EXPECT_TRUE(std::filesystem::exists(serialize_file_path));

  // Get the file create time
  auto file_time = std::filesystem::last_write_time(serialize_file_path);

  int count = 0;
  std::future<void> result = std::async([&]() {
    scanner.registMatchCallback(
        [](uint64_t id, unsigned long long from, unsigned long long to, unsigned int flags,
           void* user_data) -> int {
          int* count = static_cast<int*>(user_data);
          *count += 1;
          return 0;
        },
        &count);

    scanner.blockScan("aaaabaaaa", Wge::Common::Hyperscan::Scanner::ScanMode::Normal, nullptr,
                      nullptr);
    EXPECT_EQ(count, 8);
  });

  result.get();

  Wge::Common::Hyperscan::Scanner scanner2(std::make_shared<Wge::Common::Hyperscan::HsDataBase>(
      "a+", false, false, true, false, true, serialize_dir));
  EXPECT_TRUE(std::filesystem::exists(serialize_file_path));
  EXPECT_EQ(file_time, std::filesystem::last_write_time(serialize_file_path));

  count = 0;
  result = std::async([&]() {
    scanner2.registMatchCallback(
        [](uint64_t id, unsigned long long from, unsigned long long to, unsigned int flags,
           void* user_data) -> int {
          int* count = static_cast<int*>(user_data);
          *count += 1;
          return 0;
        },
        &count);

    scanner.blockScan("aaaabaaaa", Wge::Common::Hyperscan::Scanner::ScanMode::Normal, nullptr,
                      nullptr);
    EXPECT_EQ(count, 8);
  });
  result.get();

  // Remove the serialize directory
  std::filesystem::remove_all(serialize_dir);
  EXPECT_FALSE(std::filesystem::exists(serialize_dir));
}