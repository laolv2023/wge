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
#pragma once

#include <memory>
#include <string>

#define SHA1_VALUE_SIZE 20

namespace boost {
namespace uuids {
namespace detail {
class sha1;
}
} // namespace uuids
} // namespace boost

namespace Wge {
namespace Common {
class Sha1 {
public:
  Sha1();
  ~Sha1(){};

public:
  void update(const void* data, size_t size);
  void update(const std::string& data);
  std::string update(const void* data, size_t size, bool hex_encode);
  std::string update(const std::string& data, bool hex_encode);

public:
  static std::string marshal(const void* data, size_t size, bool hex_encode);
  static std::string marshal(const std::string& data, bool hex_encode);

private:
  static void updateInternal(const void* data, size_t size,
                             std::shared_ptr<boost::uuids::detail::sha1> sha1);
  static std::string updateInternal(const void* data, size_t size,
                                    std::shared_ptr<boost::uuids::detail::sha1> sha1,
                                    bool hex_encode);
  std::shared_ptr<boost::uuids::detail::sha1> sha1_;
};
} // namespace Common
} // namespace Wge