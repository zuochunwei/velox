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

#include <folly/Math.h>
#include <re2/re2.h>

#include "folly/experimental/EventCount.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/HashAggregation.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/SumNonPODAggregate.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::exec::test {

using core::QueryConfig;
using facebook::velox::test::BatchMaker;
using namespace common::testutil;

/// No-op implementation of Aggregate. Provides public access to following
/// base class methods: setNull, clearNull and isNull.
class AggregateFunc : public Aggregate {
 public:
  explicit AggregateFunc(TypePtr resultType) : Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return 0;
  }

  bool setNullTest(char* group) {
    return Aggregate::setNull(group);
  }

  bool clearNullTest(char* group) {
    return Aggregate::clearNull(group);
  }

  bool isNullTest(char* group) const {
    return Aggregate::isNull(group);
  }

  void initializeNewGroups(
      char** /*groups*/,
      folly::Range<const vector_size_t*> /*indices*/) override {}

  void addRawInput(
      char** /*groups*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {}

  void extractValues(
      char** /*groups*/,
      int32_t /*numGroups*/,
      VectorPtr* /*result*/) override {}

  void addIntermediateResults(
      char** /*groups*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {}

  void addSingleGroupRawInput(
      char* /*group*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {}

  void addSingleGroupIntermediateResults(
      char* /*group*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {}

  void extractAccumulators(
      char** /*groups*/,
      int32_t /*numGroups*/,
      VectorPtr* /*result*/) override {}
};

class AggregationTest : public OperatorTestBase {
 protected:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
    TestValue::enable();
  }

  void SetUp() override {
    OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();
    registerSumNonPODAggregate("sumnonpod", 64);
  }

  std::vector<RowVectorPtr>
  makeVectors(const RowTypePtr& rowType, vector_size_t size, int numVectors) {
    std::vector<RowVectorPtr> vectors;
    for (int32_t i = 0; i < numVectors; ++i) {
      auto vector = std::dynamic_pointer_cast<RowVector>(
          velox::test::BatchMaker::createBatch(rowType, size, *pool_));
      vectors.push_back(vector);
    }
    return vectors;
  }

  template <typename T>
  void testSingleKey(
      const std::vector<RowVectorPtr>& vectors,
      const std::string& keyName,
      bool ignoreNullKeys,
      bool distinct) {
    NonPODInt64::clearStats();
    std::vector<std::string> aggregates;
    if (!distinct) {
      aggregates = {
          "sum(15)", "sum(0.1)", "sum(c1)",     "sum(c2)", "sum(c4)", "sum(c5)",
          "min(15)", "min(0.1)", "min(c1)",     "min(c2)", "min(c3)", "min(c4)",
          "min(c5)", "max(15)",  "max(0.1)",    "max(c1)", "max(c2)", "max(c3)",
          "max(c4)", "max(c5)",  "sumnonpod(1)"};
    }

    auto op = PlanBuilder()
                  .values(vectors)
                  .aggregation(
                      {keyName},
                      aggregates,
                      {},
                      core::AggregationNode::Step::kPartial,
                      ignoreNullKeys)
                  .planNode();

    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause += " WHERE " + keyName + " IS NOT NULL";
    }
    if (distinct) {
      assertQuery(op, "SELECT distinct " + keyName + " " + fromClause);
    } else {
      assertQuery(
          op,
          "SELECT " + keyName +
              ", sum(15), sum(cast(0.1 as double)), sum(c1), sum(c2), sum(c4), sum(c5) , min(15), min(0.1), min(c1), min(c2), min(c3), min(c4), min(c5), max(15), max(0.1), max(c1), max(c2), max(c3), max(c4), max(c5), sum(1) " +
              fromClause + " GROUP BY " + keyName);
    }
    EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);
  }

  void testMultiKey(
      const std::vector<RowVectorPtr>& vectors,
      bool ignoreNullKeys,
      bool distinct) {
    std::vector<std::string> aggregates;
    if (!distinct) {
      aggregates = {
          "sum(15)",
          "sum(0.1)",
          "sum(c4)",
          "sum(c5)",
          "min(15)",
          "min(0.1)",
          "min(c3)",
          "min(c4)",
          "min(c5)",
          "max(15)",
          "max(0.1)",
          "max(c3)",
          "max(c4)",
          "max(c5)",
          "sumnonpod(1)"};
    }
    auto op = PlanBuilder()
                  .values(vectors)
                  .aggregation(
                      {"c0", "c1", "c6"},
                      aggregates,
                      {},
                      core::AggregationNode::Step::kPartial,
                      ignoreNullKeys)
                  .planNode();

    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause +=
          " WHERE c0 IS NOT NULL AND c1 IS NOT NULL AND c6 IS NOT NULL";
    }
    if (distinct) {
      assertQuery(op, "SELECT distinct c0, c1, c6 " + fromClause);
    } else {
      assertQuery(
          op,
          "SELECT c0, c1, c6, sum(15), sum(cast(0.1 as double)), sum(c4), sum(c5), min(15), min(0.1), min(c3), min(c4), min(c5), max(15), max(0.1), max(c3), max(c4), max(c5), sum(1) " +
              fromClause + " GROUP BY c0, c1, c6");
    }
    EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);
  }

  template <typename T>
  void setTestKey(
      int64_t value,
      int32_t multiplier,
      vector_size_t row,
      FlatVector<T>* vector) {
    vector->set(row, value * multiplier);
  }

  template <typename T>
  void setKey(
      int32_t column,
      int32_t cardinality,
      int32_t multiplier,
      int32_t row,
      RowVector* batch) {
    auto vector = batch->childAt(column)->asUnchecked<FlatVector<T>>();
    auto value = folly::Random::rand32(rng_) % cardinality;
    setTestKey(value, multiplier, row, vector);
  }

  void makeModeTestKeys(
      TypePtr rowType,
      int32_t numRows,
      int32_t c0,
      int32_t c1,
      int32_t c2,
      int32_t c3,
      int32_t c4,
      int32_t c5,
      std::vector<RowVectorPtr>& batches) {
    RowVectorPtr rowVector;
    for (auto count = 0; count < numRows; ++count) {
      if (count % 1000 == 0) {
        rowVector = BaseVector::create<RowVector>(
            rowType, std::min(1000, numRows - count), pool_.get());
        batches.push_back(rowVector);
        for (auto& child : rowVector->children()) {
          child->resize(1000);
        }
      }
      setKey<int64_t>(0, c0, 6, count % 1000, rowVector.get());
      setKey<int16_t>(1, c1, 1, count % 1000, rowVector.get());
      setKey<int8_t>(2, c2, 1, count % 1000, rowVector.get());
      setKey<StringView>(3, c3, 2, count % 1000, rowVector.get());
      setKey<StringView>(4, c4, 5, count % 1000, rowVector.get());
      setKey<StringView>(5, c5, 8, count % 1000, rowVector.get());
    }
  }

  // Inserts 'key' into 'order' with random bits and a serial
  // number. The serial number makes repeats of 'key' unique and the
  // random bits randomize the order in the set.
  void insertRandomOrder(
      int64_t key,
      int64_t serial,
      folly::F14FastSet<uint64_t>& order) {
    // The word has 24 bits of grouping key, 8 random bits and 32 bits of serial
    // number.
    order.insert(
        ((folly::Random::rand32(rng_) & 0xff) << 24) | key | (serial << 32));
  }

  // Returns the key from a value inserted with insertRandomOrder().
  int32_t randomOrderKey(uint64_t key) {
    return key & ((1 << 24) - 1);
  }

  void addBatch(
      int32_t count,
      RowVectorPtr rows,
      BufferPtr& dictionary,
      std::vector<RowVectorPtr>& batches) {
    std::vector<VectorPtr> children;
    dictionary->setSize(count * sizeof(vector_size_t));
    children.push_back(BaseVector::wrapInDictionary(
        BufferPtr(nullptr), dictionary, count, rows->childAt(0)));
    children.push_back(BaseVector::wrapInDictionary(
        BufferPtr(nullptr), dictionary, count, rows->childAt(1)));
    children.push_back(children[1]);
    batches.push_back(vectorMaker_.rowVector(children));
    dictionary = AlignedBuffer::allocate<vector_size_t>(
        dictionary->capacity() / sizeof(vector_size_t), rows->pool());
  }

  // Makes batches which reference rows in 'rows' via dictionary. The
  // dictionary indices are given by 'order', wich has values with
  // indices plus random bits so as to create randomly scattered,
  // sometimes repeated values.
  void makeBatches(
      RowVectorPtr rows,
      folly::F14FastSet<uint64_t>& order,
      std::vector<RowVectorPtr>& batches) {
    constexpr int32_t kBatch = 1000;
    BufferPtr dictionary =
        AlignedBuffer::allocate<vector_size_t>(kBatch, rows->pool());
    auto rawIndices = dictionary->asMutable<vector_size_t>();
    int32_t counter = 0;
    for (auto& n : order) {
      rawIndices[counter++] = randomOrderKey(n);
      if (counter == kBatch) {
        addBatch(counter, rows, dictionary, batches);
        rawIndices = dictionary->asMutable<vector_size_t>();
        counter = 0;
      }
    }
    if (counter > 0) {
      addBatch(counter, rows, dictionary, batches);
    }
  }

  std::unique_ptr<RowContainer> makeRowContainer(
      const std::vector<TypePtr>& keyTypes,
      const std::vector<TypePtr>& dependentTypes) {
    return std::make_unique<RowContainer>(
        keyTypes,
        false,
        std::vector<Accumulator>{},
        dependentTypes,
        false,
        false,
        true,
        true,
        pool_.get(),
        ContainerRowSerde::instance());
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {BIGINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           VARCHAR()})};
  folly::Random::DefaultGenerator rng_;
};

template <>
void AggregationTest::setTestKey(
    int64_t value,
    int32_t multiplier,
    vector_size_t row,
    FlatVector<StringView>* vector) {
  std::string chars;
  if (multiplier == 2) {
    chars.resize(2);
    chars[0] = (value % 64) + 32;
    chars[1] = ((value / 64) % 64) + 32;
  } else {
    chars = fmt::format("{}", value);
    for (int i = 2; i < multiplier; ++i) {
      chars = chars + fmt::format("{}", i * value);
    }
  }
  vector->set(row, StringView(chars));
}

TEST_F(AggregationTest, distinctWithSpilling) {
  auto vectors = makeVectors(rowType_, 10, 20);
  //createDuckDbTable(vectors);
  for (auto& x : vectors) {
    auto str = x->toString(0, 10000, "\n");
    std::cout << str << std::endl;
  }

  std::cout << "==================\n";

  auto spillDirectory = exec::test::TempDirectoryPath::create();

  core::PlanNodeId aggrNodeId;

  PlanBuilder pb;
  pb.values(vectors);
  pb.singleAggregation({"c0"}, {}, {});
  pb.capturePlanNodeId(aggrNodeId);

  AssertQueryBuilder aqb(duckDbQueryRunner_);
  aqb.spillDirectory(spillDirectory->path);
  aqb.config(QueryConfig::kSpillEnabled, "true");
  aqb.config(QueryConfig::kAggregationSpillEnabled, "true");
  aqb.config(QueryConfig::kTestingSpillPct, "100");
  aqb.plan(pb.planNode());

#if 1
  auto result = aqb.copyResults(pool_.get());
  auto str = result->toString(0, 10000, "\n");
  std::cout << str << std::endl;
#else
  auto task = aqb.assertResults("SELECT distinct c0 FROM tmp");

  // Verify that spilling is not triggered.
  ASSERT_EQ(toPlanStats(task->taskStats()).at(aggrNodeId).spilledBytes, 0);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
#endif
}

} // namespace facebook::velox::exec::test
