#include "vmf_executors.hpp"

namespace duckdb {

static inline bool VMFContains(yyjson_val *haystack, yyjson_val *needle);
static inline bool VMFFuzzyEquals(yyjson_val *haystack, yyjson_val *needle);

static inline bool VMFArrayFuzzyEquals(yyjson_val *haystack, yyjson_val *needle) {
	D_ASSERT(yyjson_get_tag(haystack) == (yyjson_TYPE_ARR | yyjson_SUBTYPE_NONE) &&
	         yyjson_get_tag(needle) == (yyjson_TYPE_ARR | yyjson_SUBTYPE_NONE));

	size_t needle_idx, needle_max, haystack_idx, haystack_max;
	yyjson_val *needle_child, *haystack_child;
	yyjson_arr_foreach(needle, needle_idx, needle_max, needle_child) {
		bool found = false;
		yyjson_arr_foreach(haystack, haystack_idx, haystack_max, haystack_child) {
			if (VMFFuzzyEquals(haystack_child, needle_child)) {
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

static inline bool VMFObjectFuzzyEquals(yyjson_val *haystack, yyjson_val *needle) {
	D_ASSERT(yyjson_get_tag(haystack) == (yyjson_TYPE_OBJ | yyjson_SUBTYPE_NONE) &&
	         yyjson_get_tag(needle) == (yyjson_TYPE_OBJ | yyjson_SUBTYPE_NONE));

	size_t idx, max;
	yyjson_val *key, *needle_child;
	yyjson_obj_foreach(needle, idx, max, key, needle_child) {
		auto haystack_child = yyjson_obj_getn(haystack, unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key));
		if (!haystack_child || !VMFFuzzyEquals(haystack_child, needle_child)) {
			return false;
		}
	}
	return true;
}

static inline bool VMFFuzzyEquals(yyjson_val *haystack, yyjson_val *needle) {
	D_ASSERT(haystack && needle);

	// Strict equality
	if (unsafe_yyjson_equals(haystack, needle)) {
		return true;
	}

	auto haystack_tag = yyjson_get_tag(needle);
	if (haystack_tag != yyjson_get_tag(haystack)) {
		return false;
	}

	// Fuzzy equality (contained in)
	switch (haystack_tag) {
	case yyjson_TYPE_ARR | yyjson_SUBTYPE_NONE:
		return VMFArrayFuzzyEquals(haystack, needle);
	case yyjson_TYPE_OBJ | yyjson_SUBTYPE_NONE:
		return VMFObjectFuzzyEquals(haystack, needle);
	default:
		return false;
	}
}

static inline bool VMFArrayContains(yyjson_val *haystack_array, yyjson_val *needle) {
	D_ASSERT(yyjson_get_tag(haystack_array) == (yyjson_TYPE_ARR | yyjson_SUBTYPE_NONE));

	size_t idx, max;
	yyjson_val *child_haystack;
	yyjson_arr_foreach(haystack_array, idx, max, child_haystack) {
		if (VMFContains(child_haystack, needle)) {
			return true;
		}
	}
	return false;
}

static inline bool VMFObjectContains(yyjson_val *haystack_object, yyjson_val *needle) {
	D_ASSERT(yyjson_get_tag(haystack_object) == (yyjson_TYPE_OBJ | yyjson_SUBTYPE_NONE));

	size_t idx, max;
	yyjson_val *key, *child_haystack;
	yyjson_obj_foreach(haystack_object, idx, max, key, child_haystack) {
		if (VMFContains(child_haystack, needle)) {
			return true;
		}
	}
	return false;
}

static inline bool VMFContains(yyjson_val *haystack, yyjson_val *needle) {
	if (VMFFuzzyEquals(haystack, needle)) {
		return true;
	}

	switch (yyjson_get_tag(haystack)) {
	case yyjson_TYPE_ARR | yyjson_SUBTYPE_NONE:
		return VMFArrayContains(haystack, needle);
	case yyjson_TYPE_OBJ | yyjson_SUBTYPE_NONE:
		return VMFObjectContains(haystack, needle);
	default:
		return false;
	}
}

static void VMFContainsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	auto &lstate = VMFFunctionLocalState::ResetAndGet(state);

	auto &haystacks = args.data[0];
	auto &needles = args.data[1];

	if (needles.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(needles)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
			return;
		}
		auto &needle_str = *ConstantVector::GetData<string_t>(needles);
		auto needle_doc = VMFCommon::ReadDocument(needle_str, VMFCommon::READ_FLAG, lstate.vmf_allocator.GetYYAlc());
		UnaryExecutor::Execute<string_t, bool>(haystacks, result, args.size(), [&](string_t haystack_str) {
			auto haystack_doc =
			    VMFCommon::ReadDocument(haystack_str, VMFCommon::READ_FLAG, lstate.vmf_allocator.GetYYAlc());
			return VMFContains(haystack_doc->root, needle_doc->root);
		});
	} else {
		BinaryExecutor::Execute<string_t, string_t, bool>(
		    haystacks, needles, result, args.size(), [&](string_t haystack_str, string_t needle_str) {
			    auto needle_doc =
			        VMFCommon::ReadDocument(needle_str, VMFCommon::READ_FLAG, lstate.vmf_allocator.GetYYAlc());
			    auto haystack_doc =
			        VMFCommon::ReadDocument(haystack_str, VMFCommon::READ_FLAG, lstate.vmf_allocator.GetYYAlc());
			    return VMFContains(haystack_doc->root, needle_doc->root);
		    });
	}
}

static void GetContainsFunctionInternal(ScalarFunctionSet &set, const LogicalType &lhs, const LogicalType &rhs) {
	set.AddFunction(ScalarFunction({lhs, rhs}, LogicalType::BOOLEAN, VMFContainsFunction, nullptr, nullptr, nullptr,
	                               VMFFunctionLocalState::Init));
}

ScalarFunctionSet VMFFunctions::GetContainsFunction() {
	ScalarFunctionSet set("vmf_contains");
	GetContainsFunctionInternal(set, LogicalType::VARCHAR, LogicalType::VARCHAR);
	GetContainsFunctionInternal(set, LogicalType::VARCHAR, LogicalType::VMF());
	GetContainsFunctionInternal(set, LogicalType::VMF(), LogicalType::VARCHAR);
	GetContainsFunctionInternal(set, LogicalType::VMF(), LogicalType::VMF());
	// TODO: implement vmf_contains that accepts path argument as well

	return set;
}

} // namespace duckdb
