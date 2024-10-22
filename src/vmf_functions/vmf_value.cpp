#include "vmf_executors.hpp"

namespace duckdb {

static inline string_t ValueFromVal(yyjson_val *val, yyjson_alc *alc, Vector &, ValidityMask &mask, idx_t idx) {
	switch (yyjson_get_tag(val)) {
	case yyjson_TYPE_NULL | yyjson_SUBTYPE_NONE:
	case yyjson_TYPE_ARR | yyjson_SUBTYPE_NONE:
	case yyjson_TYPE_OBJ | yyjson_SUBTYPE_NONE:
		mask.SetInvalid(idx);
		return string_t {};
	default:
		return VMFCommon::WriteVal<yyjson_val>(val, alc);
	}
}

static void ValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::BinaryExecute<string_t>(args, state, result, ValueFromVal);
}

static void ValueManyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::ExecuteMany<string_t>(args, state, result, ValueFromVal);
}

static void GetValueFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type, LogicalType::BIGINT}, LogicalType::VARCHAR, ValueFunction,
	                               VMFReadFunctionData::Bind, nullptr, nullptr, VMFFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::VARCHAR, ValueFunction,
	                               VMFReadFunctionData::Bind, nullptr, nullptr, VMFFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::VARCHAR), ValueManyFunction,
	                               VMFReadManyFunctionData::Bind, nullptr, nullptr, VMFFunctionLocalState::Init));
}

ScalarFunctionSet VMFFunctions::GetValueFunction() {
	// The value function is just like the extract function but returns NULL if the VMF is not a scalar value
	ScalarFunctionSet set("vmf_value");
	GetValueFunctionsInternal(set, LogicalType::VARCHAR);
	GetValueFunctionsInternal(set, LogicalType::VMF());
	return set;
}

} // namespace duckdb
