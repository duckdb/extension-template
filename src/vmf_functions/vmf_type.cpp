#include "vmf_executors.hpp"

namespace duckdb {

static inline string_t GetType(yyjson_val *val, yyjson_alc *, Vector &, ValidityMask &mask, idx_t idx) {
	return VMFCommon::ValTypeToStringT(val);
}

static void UnaryTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::UnaryExecute<string_t>(args, state, result, GetType);
}

static void BinaryTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::BinaryExecute<string_t>(args, state, result, GetType);
}

static void ManyTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::ExecuteMany<string_t>(args, state, result, GetType);
}

static void GetTypeFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type}, LogicalType::VARCHAR, UnaryTypeFunction, nullptr, nullptr, nullptr,
	                               VMFFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::VARCHAR, BinaryTypeFunction,
	                               VMFReadFunctionData::Bind, nullptr, nullptr, VMFFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::VARCHAR), ManyTypeFunction,
	                               VMFReadManyFunctionData::Bind, nullptr, nullptr, VMFFunctionLocalState::Init));
}

ScalarFunctionSet VMFFunctions::GetTypeFunction() {
	ScalarFunctionSet set("vmf_type");
	GetTypeFunctionsInternal(set, LogicalType::VARCHAR);
	GetTypeFunctionsInternal(set, LogicalType::VMF());
	return set;
}

} // namespace duckdb
