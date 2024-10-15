#include "vmf_executors.hpp"

namespace duckdb {

static inline bool VMFContains(yyvmf_val *haystack, yyvmf_val *needle);
static inline bool VMFFuzzyEquals(yyvmf_val *haystack, yyvmf_val *needle);

static inline bool VMFArrayFuzzyEquals(yyvmf_val *haystack, yyvmf_val *needle) {
	D_ASSERT(yyvmf_get_tag(haystack) == (YYVMF_TYPE_ARR | YYVMF_SUBTYPE_NONE) &&
	         yyvmf_get_tag(needle) == (YYVMF_TYPE_ARR | YYVMF_SUBTYPE_NONE));

	size_t needle_idx, needle_max, haystack_idx, haystack_max;
	yyvmf_val *needle_child, *haystack_child;
	yyvmf_arr_foreach(needle, needle_idx, needle_max, needle_child) {
		bool found = false;
		yyvmf_arr_foreach(haystack, haystack_idx, haystack_max, haystack_child) {
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

static inline bool VMFObjectFuzzyEquals(yyvmf_val *haystack, yyvmf_val *needle) {
	D_ASSERT(yyvmf_get_tag(haystack) == (YYVMF_TYPE_OBJ | YYVMF_SUBTYPE_NONE) &&
	         yyvmf_get_tag(needle) == (YYVMF_TYPE_OBJ | YYVMF_SUBTYPE_NONE));

	size_t idx, max;
	yyvmf_val *key, *needle_child;
	yyvmf_obj_foreach(needle, idx, max, key, needle_child) {
		auto haystack_child = yyvmf_obj_getn(haystack, unsafe_yyvmf_get_str(key), unsafe_yyvmf_get_len(key));
		if (!haystack_child || !VMFFuzzyEquals(haystack_child, needle_child)) {
			return false;
		}
	}
	return true;
}

static inline bool VMFFuzzyEquals(yyvmf_val *haystack, yyvmf_val *needle) {
	D_ASSERT(haystack && needle);

	// Strict equality
	if (unsafe_yyvmf_equals(haystack, needle)) {
		return true;
	}

	auto haystack_tag = yyvmf_get_tag(needle);
	if (haystack_tag != yyvmf_get_tag(haystack)) {
		return false;
	}

	// Fuzzy equality (contained in)
	switch (haystack_tag) {
	case YYVMF_TYPE_ARR | YYVMF_SUBTYPE_NONE:
		return VMFArrayFuzzyEquals(haystack, needle);
	case YYVMF_TYPE_OBJ | YYVMF_SUBTYPE_NONE:
		return VMFObjectFuzzyEquals(haystack, needle);
	default:
		return false;
	}
}

static inline bool VMFArrayContains(yyvmf_val *haystack_array, yyvmf_val *needle) {
	D_ASSERT(yyvmf_get_tag(haystack_array) == (YYVMF_TYPE_ARR | YYVMF_SUBTYPE_NONE));

	size_t idx, max;
	yyvmf_val *child_haystack;
	yyvmf_arr_foreach(haystack_array, idx, max, child_haystack) {
		if (VMFContains(child_haystack, needle)) {
			return true;
		}
	}
	return false;
}

static inline bool VMFObjectContains(yyvmf_val *haystack_object, yyvmf_val *needle) {
	D_ASSERT(yyvmf_get_tag(haystack_object) == (YYVMF_TYPE_OBJ | YYVMF_SUBTYPE_NONE));

	size_t idx, max;
	yyvmf_val *key, *child_haystack;
	yyvmf_obj_foreach(haystack_object, idx, max, key, child_haystack) {
		if (VMFContains(child_haystack, needle)) {
			return true;
		}
	}
	return false;
}

static inline bool VMFContains(yyvmf_val *haystack, yyvmf_val *needle) {
	if (VMFFuzzyEquals(haystack, needle)) {
		return true;
	}

	switch (yyvmf_get_tag(haystack)) {
	case YYVMF_TYPE_ARR | YYVMF_SUBTYPE_NONE:
		return VMFArrayContains(haystack, needle);
	case YYVMF_TYPE_OBJ | YYVMF_SUBTYPE_NONE:
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
