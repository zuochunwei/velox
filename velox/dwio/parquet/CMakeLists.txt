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

add_subdirectory(duckdb_reader)
add_subdirectory(thrift)
add_subdirectory(reader)
add_subdirectory(writer)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()

add_library(velox_dwio_parquet_reader RegisterParquetReader.cpp)
target_link_libraries(
  velox_dwio_parquet_reader
  velox_dwio_duckdb_parquet_reader
  velox_dwio_native_parquet_reader xsimd)
