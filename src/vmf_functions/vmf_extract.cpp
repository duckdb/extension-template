#include "vmf_executors.hpp"

namespace duckdb {

static inline string_t ExtractFromVal(yyvmf_val *val, yyvmf_alc *alc, Vector &, ValidityMask &, idx_t) {
	return VMFCommon::WriteVal<yyvmf_val>(val, alc);
}

static inline string_t ExtractStringFromVal(yyvmf_val *val, yyvmf_alc *alc, Vector &, ValidityMask &mask, idx_t idx) {
	switch (yyvmf_get_tag(val)) {
	case YYVMF_TYPE_NULL | YYVMF_SUBTYPE_NONE:
		mask.SetInvalid(idx);
		return string_t {};
	case YYVMF_TYPE_STR | YYVMF_SUBTYPE_NOESC:
	case YYVMF_TYPE_STR | YYVMF_SUBTYPE_NONE:
		return string_t(unsafe_yyvmf_get_str(val), unsafe_yyvmf_get_len(val));
	default:
		return VMFCommon::WriteVal<yyvmf_val>(val, alc);
	}
}

static void ExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::BinaryExecute<string_t>(args, state, result, ExtractFromVal);
}

static void ExtractManyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::ExecuteMany<string_t>(args, state, result, ExtractFromVal);
}

static void ExtractStringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::BinaryExecute<string_t>(args, state, result, ExtractStringFromVal);
}

static void ExtractStringManyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::ExecuteMany<string_t>(args, state, result, ExtractStringFromVal);
}

static void GetExtractFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type, LogicalType::BIGINT}, LogicalType::VMF(), ExtractFunction,
	                               VMFReadFunctionData::Bind, nullptr, nullptr, VMFFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::VMF(), ExtractFunction,
	                               VMFReadFunctionData::Bind, nullptr, nullptr, VMFFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::VMF()), ExtractManyFunction,
	                               VMFReadManyFunctionData::Bind, nullptr, nullptr, VMFFunctionLocalState::Init));
}

ScalarFunctionSet VMFFunctions::GetExtractFunction() {
	// Generic extract function
	ScalarFunctionSet set("vmf_extract");
	GetExtractFunctionsInternal(set, LogicalType::VARCHAR);
	GetExtractFunctionsInternal(set, LogicalType::VMF());
	return set;
}

static void GetExtractStringFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type, LogicalType::BIGINT}, LogicalType::VARCHAR, ExtractStringFunction,
	                               VMFReadFunctionData::Bind, nullptr, nullptr, VMFFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::VARCHAR, ExtractStringFunction,
	                               VMFReadFunctionData::Bind, nullptr, nullptr, VMFFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::VARCHAR), ExtractStringManyFunction,
	                               VMFReadManyFunctionData::Bind, nullptr, nullptr, VMFFunctionLocalState::Init));
}

ScalarFunctionSet VMFFunctions::GetExtractStringFunction() {
	// String extract function
	ScalarFunctionSet set("vmf_extract_string");
	GetExtractStringFunctionsInternal(set, LogicalType::VARCHAR);
	GetExtractStringFunctionsInternal(set, LogicalType::VMF());
	return set;
}

} // namespace duckdb
