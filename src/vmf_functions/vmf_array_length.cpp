#include "vmf_executors.hpp"

namespace duckdb {

static inline uint64_t GetArrayLength(yyvmf_val *val, yyvmf_alc *, Vector &, ValidityMask &, idx_t) {
	return yyvmf_arr_size(val);
}

static void UnaryArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::UnaryExecute<uint64_t>(args, state, result, GetArrayLength);
}

static void BinaryArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::BinaryExecute<uint64_t>(args, state, result, GetArrayLength);
}

static void ManyArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::ExecuteMany<uint64_t>(args, state, result, GetArrayLength);
}

static void GetArrayLengthFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type}, LogicalType::UBIGINT, UnaryArrayLengthFunction, nullptr, nullptr,
	                               nullptr, VMFFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::UBIGINT, BinaryArrayLengthFunction,
	                               VMFReadFunctionData::Bind, nullptr, nullptr, VMFFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::UBIGINT), ManyArrayLengthFunction,
	                               VMFReadManyFunctionData::Bind, nullptr, nullptr, VMFFunctionLocalState::Init));
}

ScalarFunctionSet VMFFunctions::GetArrayLengthFunction() {
	ScalarFunctionSet set("vmf_array_length");
	GetArrayLengthFunctionsInternal(set, LogicalType::VARCHAR);
	GetArrayLengthFunctionsInternal(set, LogicalType::VMF());
	return set;
}

} // namespace duckdb
