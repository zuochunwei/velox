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

#include "velox/substrait/SubstraitParser.h"
#include "velox/common/base/Exceptions.h"
#include "velox/substrait/TypeUtils.h"

namespace facebook::velox::substrait {

std::shared_ptr<SubstraitParser::SubstraitType> SubstraitParser::parseType(
    const ::substrait::Type& substraitType) {
  // The used type names should be aligned with those in Velox.
  std::string typeName;
  ::substrait::Type_Nullability nullability;
  switch (substraitType.kind_case()) {
    case ::substrait::Type::KindCase::kBool: {
      typeName = "BOOLEAN";
      nullability = substraitType.bool_().nullability();
      break;
    }
    case ::substrait::Type::KindCase::kI8: {
      typeName = "TINYINT";
      nullability = substraitType.i8().nullability();
      break;
    }
    case ::substrait::Type::KindCase::kI16: {
      typeName = "SMALLINT";
      nullability = substraitType.i16().nullability();
      break;
    }
    case ::substrait::Type::KindCase::kI32: {
      typeName = "INTEGER";
      nullability = substraitType.i32().nullability();
      break;
    }
    case ::substrait::Type::KindCase::kI64: {
      typeName = "BIGINT";
      nullability = substraitType.i64().nullability();
      break;
    }
    case ::substrait::Type::KindCase::kFp32: {
      typeName = "REAL";
      nullability = substraitType.fp32().nullability();
      break;
    }
    case ::substrait::Type::KindCase::kFp64: {
      typeName = "DOUBLE";
      nullability = substraitType.fp64().nullability();
      break;
    }
    case ::substrait::Type::KindCase::kString: {
      typeName = "VARCHAR";
      nullability = substraitType.string().nullability();
      break;
    }
    case ::substrait::Type::KindCase::kBinary: {
      typeName = "VARBINARY";
      nullability = substraitType.string().nullability();
      break;
    }
    case ::substrait::Type::KindCase::kStruct: {
      // The type name of struct is in the format of:
      // ROW<type0,type1,ROW<type2>>...typen.
      typeName = "ROW<";
      const auto& sStruct = substraitType.struct_();
      const auto& substraitTypes = sStruct.types();
      for (int i = 0; i < substraitTypes.size(); i++) {
        if (i > 0) {
          typeName += ",";
        }
        typeName += parseType(substraitTypes[i])->type;
      }
      typeName += ">";
      nullability = substraitType.struct_().nullability();
      break;
    }
    case ::substrait::Type::KindCase::kList: {
      // The type name of list is in the format of: ARRAY<T>.
      const auto& sList = substraitType.list();
      const auto& sType = sList.type();
      typeName = "ARRAY<" + parseType(sType)->type + ">";
      nullability = substraitType.list().nullability();
      break;
    }
    case ::substrait::Type::KindCase::kMap: {
      // The type name of map is in the format of: MAP<K,V>.
      const auto& sMap = substraitType.map();
      const auto& keyType = sMap.key();
      const auto& valueType = sMap.value();
      typeName = "MAP<" + parseType(keyType)->type + "," +
          parseType(valueType)->type + ">";
      nullability = substraitType.map().nullability();
      break;
    }
    case ::substrait::Type::KindCase::kUserDefined: {
      // We only support UNKNOWN type to handle the null literal whose type is
      // not known.
      VELOX_CHECK_EQ(substraitType.user_defined().type_reference(), 0);
      typeName = "UNKNOWN";
      nullability = substraitType.string().nullability();
      break;
    }
    case ::substrait::Type::KindCase::kDate: {
      typeName = "DATE";
      nullability = substraitType.date().nullability();
      break;
    }
    default:
      VELOX_NYI(
          "Parsing for Substrait type not supported: {}",
          substraitType.DebugString());
  }

  bool nullable;
  switch (nullability) {
    case ::substrait::Type_Nullability::
        Type_Nullability_NULLABILITY_UNSPECIFIED:
      nullable = true;
      break;
    case ::substrait::Type_Nullability::Type_Nullability_NULLABILITY_NULLABLE:
      nullable = true;
      break;
    case ::substrait::Type_Nullability::Type_Nullability_NULLABILITY_REQUIRED:
      nullable = false;
      break;
    default:
      VELOX_NYI(
          "Substrait parsing for nullability {} not supported.", nullability);
  }
  SubstraitType type = {typeName, nullable};
  return std::make_shared<SubstraitType>(type);
}

std::string SubstraitParser::parseType(const std::string& substraitType) {
  auto it = typeMap_.find(substraitType);
  if (it == typeMap_.end()) {
    VELOX_NYI("Substrait parsing for type {} not supported.", substraitType);
  }
  return it->second;
};

std::vector<std::shared_ptr<SubstraitParser::SubstraitType>>
SubstraitParser::parseNamedStruct(const ::substrait::NamedStruct& namedStruct) {
  // Nte that "names" are not used.
  // Parse Struct.
  const auto& sStruct = namedStruct.struct_();
  const auto& sTypes = sStruct.types();
  std::vector<std::shared_ptr<SubstraitType>> substraitTypeList;
  substraitTypeList.reserve(sTypes.size());
  for (const auto& type : sTypes) {
    substraitTypeList.emplace_back(parseType(type));
  }
  return substraitTypeList;
}

std::vector<bool> SubstraitParser::parsePartitionColumns(
    const ::substrait::NamedStruct& namedStruct) {
  const auto& columnsTypes = namedStruct.partition_columns().column_type();
  std::vector<bool> isPartitionColumns;
  if (columnsTypes.size() == 0) {
    // Regard all columns as non-partitioned columns.
    isPartitionColumns.resize(namedStruct.names().size(), false);
    return isPartitionColumns;
  } else {
    VELOX_CHECK(
        columnsTypes.size() == namedStruct.names().size(),
        "Invalid partion columns.");
  }

  isPartitionColumns.reserve(columnsTypes.size());
  for (const auto& columnType : columnsTypes) {
    switch (columnType) {
      case ::substrait::PartitionColumns::NORMAL_COL:
        isPartitionColumns.emplace_back(false);
        break;
      case ::substrait::PartitionColumns::PARTITION_COL:
        isPartitionColumns.emplace_back(true);
        break;
      default:
        VELOX_FAIL("Patition column type is not supported.");
    }
  }
  return isPartitionColumns;
}

int32_t SubstraitParser::parseReferenceSegment(
    const ::substrait::Expression::ReferenceSegment& refSegment) {
  auto typeCase = refSegment.reference_type_case();
  switch (typeCase) {
    case ::substrait::Expression::ReferenceSegment::ReferenceTypeCase::
        kStructField: {
      return refSegment.struct_field().field();
    }
    default:
      VELOX_NYI(
          "Substrait conversion not supported for ReferenceSegment '{}'",
          typeCase);
  }
}

std::vector<std::string> SubstraitParser::makeNames(
    const std::string& prefix,
    int size) {
  std::vector<std::string> names;
  names.reserve(size);
  for (int i = 0; i < size; i++) {
    names.emplace_back(fmt::format("{}_{}", prefix, i));
  }
  return names;
}

std::string SubstraitParser::makeNodeName(int node_id, int col_idx) {
  return fmt::format("n{}_{}", node_id, col_idx);
}

int SubstraitParser::getIdxFromNodeName(const std::string& nodeName) {
  // Get the position of "_" in the function name.
  std::size_t pos = nodeName.find("_");
  if (pos == std::string::npos) {
    VELOX_FAIL("Invalid node name.");
  }
  if (pos == nodeName.size() - 1) {
    VELOX_FAIL("Invalid node name.");
  }
  // Get the column index.
  std::string colIdx = nodeName.substr(pos + 1);
  try {
    return stoi(colIdx);
  } catch (const std::exception& err) {
    VELOX_FAIL(err.what());
  }
}

std::string SubstraitParser::findSubstraitFuncSpec(
    const std::unordered_map<uint64_t, std::string>& functionMap,
    uint64_t id) const {
  if (functionMap.find(id) == functionMap.end()) {
    VELOX_FAIL("Could not find function id {} in function map.", id);
  }
  std::unordered_map<uint64_t, std::string>& map =
      const_cast<std::unordered_map<uint64_t, std::string>&>(functionMap);
  return map[id];
}

std::string SubstraitParser::getSubFunctionName(
    const std::string& subFuncSpec) const {
  // Get the position of ":" in the function name.
  std::size_t pos = subFuncSpec.find(":");
  if (pos == std::string::npos) {
    return subFuncSpec;
  }
  return subFuncSpec.substr(0, pos);
}

void SubstraitParser::getSubFunctionTypes(
    const std::string& subFuncSpec,
    std::vector<std::string>& types) const {
  // Get the position of ":" in the function name.
  std::size_t pos = subFuncSpec.find(":");
  // Get the parameter types.
  std::string funcTypes;
  if (pos == std::string::npos) {
    funcTypes = subFuncSpec;
  } else {
    if (pos == subFuncSpec.size() - 1) {
      return;
    }
    funcTypes = subFuncSpec.substr(pos + 1);
  }
  // Split the types with delimiter.
  std::string delimiter = "_";
  while ((pos = funcTypes.find(delimiter)) != std::string::npos) {
    auto type = funcTypes.substr(0, pos);
    if (type != "opt" && type != "req") {
      types.emplace_back(type);
    }
    funcTypes.erase(0, pos + delimiter.length());
  }
  types.emplace_back(funcTypes);
}

std::string SubstraitParser::findVeloxFunction(
    const std::unordered_map<uint64_t, std::string>& functionMap,
    uint64_t id) const {
  std::string funcSpec = findSubstraitFuncSpec(functionMap, id);
  std::string funcName = getSubFunctionName(funcSpec);
  return mapToVeloxFunction(funcName);
}

std::string SubstraitParser::mapToVeloxFunction(
    const std::string& subFunc) const {
  auto it = substraitVeloxFunctionMap_.find(subFunc);
  if (it != substraitVeloxFunctionMap_.end()) {
    return it->second;
  }

  // If not finding the mapping from Substrait function name to Velox function
  // name, the original Substrait function name will be used.
  return subFunc;
}

bool SubstraitParser::configSetInOptimization(
    const ::substrait::extensions::AdvancedExtension& extension,
    const std::string& config) const {
  if (extension.has_optimization()) {
    google::protobuf::StringValue msg;
    extension.optimization().UnpackTo(&msg);
    std::size_t pos = msg.value().find(config);
    if ((pos != std::string::npos) &&
        (msg.value().substr(pos + config.size(), 1) == "1")) {
      return true;
    }
  }
  return false;
}

} // namespace facebook::velox::substrait
