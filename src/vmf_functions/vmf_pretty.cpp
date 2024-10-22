#include "vmf_executors.hpp"

namespace duckdb {

//! Pretty Print a given VMF Document
string_t PrettyPrint(yyjson_val *val, yyjson_alc *alc, Vector &, ValidityMask &, idx_t) {
	D_ASSERT(alc);
	idx_t len;
	auto data =
	    yyjson_val_write_opts(val, VMFCommon::WRITE_PRETTY_FLAG, alc, reinterpret_cast<size_t *>(&len), nullptr);
	return string_t(data, len);
}

static void PrettyPrintFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto vmf_type = args.data[0].GetType();
	D_ASSERT(vmf_type == LogicalType::VARCHAR || vmf_type == LogicalType::VMF());

	VMFExecutors::UnaryExecute<string_t>(args, state, result, PrettyPrint);
}

static void GetPrettyPrintFunctionInternal(ScalarFunctionSet &set, const LogicalType &vmf) {
	set.AddFunction(ScalarFunction("vmf_pretty", {vmf}, LogicalType::VARCHAR, PrettyPrintFunction, nullptr, nullptr,
	                               nullptr, VMFFunctionLocalState::Init));
}

ScalarFunctionSet VMFFunctions::GetPrettyPrintFunction() {
	ScalarFunctionSet set("vmf_pretty");
	GetPrettyPrintFunctionInternal(set, LogicalType::VMF());
	return set;
}

} // namespace duckdb
