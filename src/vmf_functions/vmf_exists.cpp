#include "vmf_executors.hpp"

namespace duckdb {

static inline bool VMFExists(yyjson_val *val, yyjson_alc *, Vector &, ValidityMask &, idx_t) {
	return val;
}

static void BinaryExistsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::BinaryExecute<bool, false>(args, state, result, VMFExists);
}

static void ManyExistsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::ExecuteMany<bool, false>(args, state, result, VMFExists);
}

static void GetExistsFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::BOOLEAN, BinaryExistsFunction,
	                               VMFReadFunctionData::Bind, nullptr, nullptr, VMFFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::BOOLEAN), ManyExistsFunction,
	                               VMFReadManyFunctionData::Bind, nullptr, nullptr, VMFFunctionLocalState::Init));
}

ScalarFunctionSet VMFFunctions::GetExistsFunction() {
	ScalarFunctionSet set("vmf_exists");
	GetExistsFunctionsInternal(set, LogicalType::VARCHAR);
	GetExistsFunctionsInternal(set, LogicalType::VMF());
	return set;
}

} // namespace duckdb
