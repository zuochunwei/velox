/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/functions/sparksql/MightContain.h"
#include "velox/common/base/BloomFilter.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class MightContainTest : public SparkFunctionBaseTest {
 protected:
  std::optional<bool> mightContain(
      std::optional<StringView> bloom,
      int64_t value) {
    return evaluateOnce<bool>(
        fmt::format("might_contain(cast(c0 as varbinary), {})", value), bloom);
  }
  std::shared_ptr<memory::MemoryPool> pool_{memory::getDefaultMemoryPool()};
  HashStringAllocator allocator_{pool_.get()};
};

TEST_F(MightContainTest, common) {
  constexpr int64_t kSize = 10;
  BloomFilter bloom{StlAllocator<uint64_t>(&allocator_)};
  bloom.reset(kSize);
  for (auto i = 0; i < kSize; ++i) {
    bloom.insert(folly::hasher<int64_t>()(i));
  }
  std::string data;
  data.resize(bloom.serializedSize());
  bloom.serialize(data.data());

  StringView serialized(data.data(), data.size());
  for (auto i = 0; i < kSize; ++i) {
    EXPECT_TRUE(mightContain(serialized, i).value());
  }

  EXPECT_FALSE(mightContain(serialized, kSize + 123451).value());
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
