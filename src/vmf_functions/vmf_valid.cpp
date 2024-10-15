#include "vmf_executors.hpp"

namespace duckdb {

static void ValidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = VMFFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.vmf_allocator.GetYYAlc();
	auto &inputs = args.data[0];
	UnaryExecutor::Execute<string_t, bool>(inputs, result, args.size(), [&](string_t input) {
		return VMFCommon::ReadDocumentUnsafe(input, VMFCommon::READ_FLAG, alc);
	});
}

static void GetValidFunctionInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction("vmf_valid", {input_type}, LogicalType::BOOLEAN, ValidFunction, nullptr, nullptr,
	                               nullptr, VMFFunctionLocalState::Init));
}

ScalarFunctionSet VMFFunctions::GetValidFunction() {
	ScalarFunctionSet set("vmf_valid");
	GetValidFunctionInternal(set, LogicalType::VARCHAR);
	GetValidFunctionInternal(set, LogicalType::VMF());

	return set;
}

} // namespace duckdb
