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
#include "sha1.h"

#include <iostream>

#include <boost/algorithm/hex.hpp>
#include <boost/uuid/detail/md5.hpp>
#include <boost/uuid/detail/sha1.hpp>

namespace Wge {
namespace Common {
Sha1::Sha1() {
  sha1_ = std::shared_ptr<boost::uuids::detail::sha1>(new boost::uuids::detail::sha1);
}

void Sha1::update(const void* data, size_t size) { updateInternal(data, size, sha1_); }

void Sha1::update(const std::string& data) { updateInternal(data.c_str(), data.size(), sha1_); }

std::string Sha1::update(const void* data, size_t size, bool hex_encode) {
  return updateInternal(data, size, sha1_, hex_encode);
}

std::string Sha1::update(const std::string& data, bool hex_encode) {
  return updateInternal(data.c_str(), data.size(), sha1_, hex_encode);
}

std::string Sha1::marshal(const void* data, size_t size, bool hex_encode) {
  return updateInternal(data, size,
                        std::shared_ptr<boost::uuids::detail::sha1>(new boost::uuids::detail::sha1),
                        hex_encode);
}

std::string Sha1::marshal(const std::string& data, bool hex_encode) {
  return updateInternal(data.c_str(), data.size(),
                        std::shared_ptr<boost::uuids::detail::sha1>(new boost::uuids::detail::sha1),
                        hex_encode);
}

void Sha1::updateInternal(const void* data, size_t size,
                          std::shared_ptr<boost::uuids::detail::sha1> sha1) {
  if (data == nullptr) {
    return;
  }

  sha1->process_bytes(data, size);
  boost::uuids::detail::sha1::digest_type digest;
  sha1->get_digest(digest);
}

std::string Sha1::updateInternal(const void* data, size_t size,
                                 std::shared_ptr<boost::uuids::detail::sha1> sha1,
                                 bool hex_encode) {
  std::string ret;
  if (data == nullptr) {
    return ret;
  }

  sha1->process_bytes(data, size);
  boost::uuids::detail::sha1::digest_type digest;
  sha1->get_digest(digest);

  // adjust bytes order
  boost::uuids::detail::sha1::digest_type order;
  memcpy(&order, &digest, sizeof(order));
  for (size_t i = 0; i < sizeof(digest) / sizeof(int); ++i) {
    for (size_t j = 0; j < sizeof(int); ++j) {
      reinterpret_cast<char*>(&order[i])[j] =
          reinterpret_cast<char*>(&digest[i])[sizeof(int) - j - 1];
    }
  }

  const auto char_digest = reinterpret_cast<const char*>(&order);

  if (hex_encode) {
    boost::algorithm::hex_lower(char_digest,
                                char_digest + sizeof(boost::uuids::detail::sha1::digest_type),
                                std::back_inserter(ret));
  } else {
    ret.append(char_digest, sizeof(boost::uuids::detail::sha1::digest_type));
  }

  return ret;
}
} // namespace Common
} // namespace Wge