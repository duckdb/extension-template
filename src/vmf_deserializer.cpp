#include "vmf_deserializer.hpp"
#include "duckdb/common/types/blob.hpp"

namespace duckdb {

void VmfDeserializer::OnPropertyBegin(const field_id_t, const char *tag) {
	current_tag = tag;
}

void VmfDeserializer::OnPropertyEnd() {
}

bool VmfDeserializer::OnOptionalPropertyBegin(const field_id_t, const char *tag) {
	auto parent = Current();
	auto present = yyvmf_obj_get(parent.val, tag) != nullptr;
	if (present) {
		current_tag = tag;
	}
	return present;
}

void VmfDeserializer::OnOptionalPropertyEnd(bool) {
}

// If inside an object, return the value associated by the current tag (property name)
// If inside an array, return the next element in the sequence
yyvmf_val *VmfDeserializer::GetNextValue() {
	auto &parent_val = Current();
	yyvmf_val *val;
	if (yyvmf_is_obj(parent_val.val)) {
		val = yyvmf_obj_get(parent_val.val, current_tag);
		if (!val) {
			const char *vmf = yyvmf_val_write(Current().val, 0, nullptr);
			auto msg =
			    StringUtil::Format("Expected but did not find property '%s' in vmf object: '%s'", current_tag, vmf);
			free((void *)vmf);
			throw ParserException(msg);
		}
	} else if (yyvmf_is_arr(parent_val.val)) {
		val = yyvmf_arr_iter_next(&parent_val.arr_iter);
		if (!val) {
			const char *vmf = yyvmf_val_write(Current().val, 0, nullptr);
			auto msg =
			    StringUtil::Format("Expected but did not find another value after exhausting vmf array: '%s'", vmf);
			free((void *)vmf);
			throw ParserException(msg);
		}
	} else {
		// unreachable?
		throw InternalException("Cannot get value from non-array/object");
	}
	return val;
}

void VmfDeserializer::ThrowTypeError(yyvmf_val *val, const char *expected) {
	auto actual = yyvmf_get_type_desc(val);
	auto &parent = Current();
	if (yyvmf_is_obj(parent.val)) {
		auto msg =
		    StringUtil::Format("property '%s' expected type '%s', but got type: '%s'", current_tag, expected, actual);
		throw ParserException(msg);
	} else if (yyvmf_is_arr(parent.val)) {
		auto msg = StringUtil::Format("Sequence expect child of type '%s', but got type: %s", expected, actual);
		throw ParserException(msg);
	} else {
		// unreachable?
		throw InternalException("cannot get nested value from non object or array-type");
	}
}

void VmfDeserializer::DumpDoc() {
	const char *vmf = yyvmf_write(doc, 0, nullptr);
	printf("vmf: %s\n", vmf);
	free((void *)vmf);
}

void VmfDeserializer::DumpCurrent() {
	const char *vmf = yyvmf_val_write(Current().val, 0, nullptr);
	printf("vmf: %s\n", vmf);
	free((void *)vmf);
}

void VmfDeserializer::Dump(yyvmf_mut_val *val) {
	const char *vmf = yyvmf_mut_val_write(val, 0, nullptr);
	printf("vmf: %s\n", vmf);
	free((void *)vmf);
}

void VmfDeserializer::Dump(yyvmf_val *val) {
	const char *vmf = yyvmf_val_write(val, 0, nullptr);
	printf("vmf: %s\n", vmf);
	free((void *)vmf);
}

//===--------------------------------------------------------------------===//
// Nested Types Hooks
//===--------------------------------------------------------------------===//
void VmfDeserializer::OnObjectBegin() {
	auto val = GetNextValue();
	if (!yyvmf_is_obj(val)) {
		ThrowTypeError(val, "object");
	}
	Push(val);
}

void VmfDeserializer::OnObjectEnd() {
	stack.pop_back();
}

idx_t VmfDeserializer::OnListBegin() {
	auto val = GetNextValue();
	if (!yyvmf_is_arr(val)) {
		ThrowTypeError(val, "array");
	}
	Push(val);
	return yyvmf_arr_size(val);
}

void VmfDeserializer::OnListEnd() {
	Pop();
}

bool VmfDeserializer::OnNullableBegin() {
	auto &parent_val = Current();
	yyvmf_arr_iter iter;
	if (yyvmf_is_arr(parent_val.val)) {
		iter = parent_val.arr_iter;
	}
	auto val = GetNextValue();

	// Recover the iterator if we are inside an array
	if (yyvmf_is_arr(parent_val.val)) {
		parent_val.arr_iter = iter;
	}

	if (yyvmf_is_null(val)) {
		return false;
	}

	return true;
}

void VmfDeserializer::OnNullableEnd() {
}

//===--------------------------------------------------------------------===//
// Primitive Types
//===--------------------------------------------------------------------===//
bool VmfDeserializer::ReadBool() {
	auto val = GetNextValue();
	if (!yyvmf_is_bool(val)) {
		ThrowTypeError(val, "bool");
	}
	return yyvmf_get_bool(val);
}

int8_t VmfDeserializer::ReadSignedInt8() {
	auto val = GetNextValue();
	if (!yyvmf_is_int(val)) {
		ThrowTypeError(val, "int8_t");
	}
	return yyvmf_get_sint(val);
}

uint8_t VmfDeserializer::ReadUnsignedInt8() {
	auto val = GetNextValue();
	if (!yyvmf_is_uint(val)) {
		ThrowTypeError(val, "uint8_t");
	}
	return yyvmf_get_uint(val);
}

int16_t VmfDeserializer::ReadSignedInt16() {
	auto val = GetNextValue();
	if (!yyvmf_is_int(val)) {
		ThrowTypeError(val, "int16_t");
	}
	return yyvmf_get_sint(val);
}

uint16_t VmfDeserializer::ReadUnsignedInt16() {
	auto val = GetNextValue();
	if (!yyvmf_is_uint(val)) {
		ThrowTypeError(val, "uint16_t");
	}
	return yyvmf_get_uint(val);
}

int32_t VmfDeserializer::ReadSignedInt32() {
	auto val = GetNextValue();
	if (!yyvmf_is_int(val)) {
		ThrowTypeError(val, "int32_t");
	}
	return yyvmf_get_sint(val);
}

uint32_t VmfDeserializer::ReadUnsignedInt32() {
	auto val = GetNextValue();
	if (!yyvmf_is_uint(val)) {
		ThrowTypeError(val, "uint32_t");
	}
	return yyvmf_get_uint(val);
}

int64_t VmfDeserializer::ReadSignedInt64() {
	auto val = GetNextValue();
	if (!yyvmf_is_int(val)) {
		ThrowTypeError(val, "int64_t");
	}
	return yyvmf_get_sint(val);
}

uint64_t VmfDeserializer::ReadUnsignedInt64() {
	auto val = GetNextValue();
	if (!yyvmf_is_uint(val)) {
		ThrowTypeError(val, "uint64_t");
	}
	return yyvmf_get_uint(val);
}

float VmfDeserializer::ReadFloat() {
	auto val = GetNextValue();
	if (!yyvmf_is_real(val)) {
		ThrowTypeError(val, "float");
	}
	return yyvmf_get_real(val);
}

double VmfDeserializer::ReadDouble() {
	auto val = GetNextValue();
	if (!yyvmf_is_real(val)) {
		ThrowTypeError(val, "double");
	}
	return yyvmf_get_real(val);
}

string VmfDeserializer::ReadString() {
	auto val = GetNextValue();
	if (!yyvmf_is_str(val)) {
		ThrowTypeError(val, "string");
	}
	return yyvmf_get_str(val);
}

hugeint_t VmfDeserializer::ReadHugeInt() {
	auto val = GetNextValue();
	if (!yyvmf_is_obj(val)) {
		ThrowTypeError(val, "object");
	}
	Push(val);
	hugeint_t result;
	ReadProperty(100, "upper", result.upper);
	ReadProperty(101, "lower", result.lower);
	Pop();
	return result;
}

uhugeint_t VmfDeserializer::ReadUhugeInt() {
	auto val = GetNextValue();
	if (!yyvmf_is_obj(val)) {
		ThrowTypeError(val, "object");
	}
	Push(val);
	uhugeint_t result;
	ReadProperty(100, "upper", result.upper);
	ReadProperty(101, "lower", result.lower);
	Pop();
	return result;
}

void VmfDeserializer::ReadDataPtr(data_ptr_t &ptr, idx_t count) {
	auto val = GetNextValue();
	if (!yyvmf_is_str(val)) {
		ThrowTypeError(val, "string");
	}
	auto str = yyvmf_get_str(val);
	auto len = yyvmf_get_len(val);
	D_ASSERT(len == count);
	auto blob = string_t(str, len);
	Blob::ToString(blob, char_ptr_cast(ptr));
}

} // namespace duckdb
