#include "vmf_executors.hpp"

namespace duckdb {

static inline list_entry_t GetVMFKeys(yyvmf_val *val, yyvmf_alc *, Vector &result, ValidityMask &, idx_t) {
	auto num_keys = yyvmf_obj_size(val);
	auto current_size = ListVector::GetListSize(result);
	auto new_size = current_size + num_keys;

	// Grow list if needed
	if (ListVector::GetListCapacity(result) < new_size) {
		ListVector::Reserve(result, new_size);
	}

	// Write the strings to the child vector
	auto keys = FlatVector::GetData<string_t>(ListVector::GetEntry(result));
	size_t idx, max;
	yyvmf_val *key, *child_val;
	yyvmf_obj_foreach(val, idx, max, key, child_val) {
		keys[current_size + idx] = string_t(unsafe_yyvmf_get_str(key), unsafe_yyvmf_get_len(key));
	}

	// Update size
	ListVector::SetListSize(result, current_size + num_keys);

	return {current_size, num_keys};
}

static void UnaryVMFKeysFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::UnaryExecute<list_entry_t>(args, state, result, GetVMFKeys);
}

static void BinaryVMFKeysFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::BinaryExecute<list_entry_t>(args, state, result, GetVMFKeys);
}

static void ManyVMFKeysFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::ExecuteMany<list_entry_t>(args, state, result, GetVMFKeys);
}

static void GetVMFKeysFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type}, LogicalType::LIST(LogicalType::VARCHAR), UnaryVMFKeysFunction,
	                               nullptr, nullptr, nullptr, VMFFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::LIST(LogicalType::VARCHAR),
	                               BinaryVMFKeysFunction, VMFReadFunctionData::Bind, nullptr, nullptr,
	                               VMFFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::LIST(LogicalType::VARCHAR)), ManyVMFKeysFunction,
	                               VMFReadManyFunctionData::Bind, nullptr, nullptr, VMFFunctionLocalState::Init));
}

ScalarFunctionSet VMFFunctions::GetKeysFunction() {
	ScalarFunctionSet set("vmf_keys");
	GetVMFKeysFunctionsInternal(set, LogicalType::VARCHAR);
	GetVMFKeysFunctionsInternal(set, LogicalType::VMF());
	return set;
}

} // namespace duckdb
