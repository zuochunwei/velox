# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(utils)

add_executable(
  velox_dwio_common_test
  BitConcatenationTest.cpp
  BitPackDecoderTest.cpp
  ChainedBufferTests.cpp
  DataBufferTests.cpp
  DecoderUtilTest.cpp
  LocalFileSinkTest.cpp
  LoggedExceptionTest.cpp
  RangeTests.cpp
  ReadFileInputStreamTests.cpp
  RetryTests.cpp
  TestBufferedInput.cpp
  TestColumnSelector.cpp
  TypeTests.cpp)
add_test(velox_dwio_common_test velox_dwio_common_test)
target_link_libraries(
  velox_dwio_common_test
  velox_dwio_common_test_utils
  velox_temp_path
  Boost::regex
  ${VELOX_LINK_LIBS}
  ${FOLLY_WITH_DEPENDENCIES}
  ${TEST_LINK_LIBS}
  ${gflags_LIBRARIES}
  gtest
  gtest_main
  gmock
  glog::glog
  ${FMT})

add_executable(velox_dwio_common_data_buffer_benchmark DataBufferBenchmark.cpp)

target_link_libraries(
  velox_dwio_common_data_buffer_benchmark velox_dwio_common velox_memory
  velox_dwio_common_exception ${FOLLY} ${FOLLY_BENCHMARK})

add_executable(velox_dwio_common_int_decoder_benchmark IntDecoderBenchmark.cpp)
target_link_libraries(
  velox_dwio_common_int_decoder_benchmark velox_dwio_common_exception
  velox_exception velox_dwio_dwrf_common ${FOLLY} ${FOLLY_BENCHMARK})

add_library(velox_e2e_filter_test_base E2EFilterTestBase.cpp)

target_link_libraries(
  velox_e2e_filter_test_base
  velox_functions_prestosql
  velox_parse_parser
  velox_vector_test_lib
  ${VELOX_LINK_LIBS}
  ${FOLLY_WITH_DEPENDENCIES}
  ${FMT}
  ${LZ4}
  ${LZO}
  ${ZSTD}
  ${ZLIB_LIBRARIES}
  ${TEST_LINK_LIBS})

if(VELOX_ENABLE_ARROW)
  add_executable(velox_dwio_common_bitpack_decoder_benchmark
                 BitPackDecoderBenchmark.cpp)
  target_link_libraries(
    velox_dwio_common_bitpack_decoder_benchmark velox_dwio_common arrow duckdb
    ${FOLLY} ${FOLLY_BENCHMARK})
endif()
