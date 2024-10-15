#pragma once
#include "vmf_common.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

class VmfDeserializer : public Deserializer {
public:
	VmfDeserializer(yyvmf_val *val, yyvmf_doc *doc) : doc(doc) {
		deserialize_enum_from_string = true;
		stack.emplace_back(val);
	}
	~VmfDeserializer() {
		yyvmf_doc_free(doc);
	}

private:
	struct StackFrame {
		yyvmf_val *val;
		yyvmf_arr_iter arr_iter;
		explicit StackFrame(yyvmf_val *val) : val(val) {
			yyvmf_arr_iter_init(val, &arr_iter);
		}
	};

	yyvmf_doc *doc;
	const char *current_tag = nullptr;
	vector<StackFrame> stack;

	void DumpDoc();
	void DumpCurrent();
	void Dump(yyvmf_mut_val *val);
	void Dump(yyvmf_val *val);

	// Get the current vmf value
	inline StackFrame &Current() {
		return stack.back();
	};

	inline void Push(yyvmf_val *val) {
		stack.emplace_back(val);
	}
	inline void Pop() {
		stack.pop_back();
	}
	yyvmf_val *GetNextValue();

	void ThrowTypeError(yyvmf_val *val, const char *expected);

	//===--------------------------------------------------------------------===//
	// Nested Types Hooks
	//===--------------------------------------------------------------------===//
	void OnPropertyBegin(const field_id_t field_id, const char *tag) final;
	void OnPropertyEnd() final;
	bool OnOptionalPropertyBegin(const field_id_t field_id, const char *tag) final;
	void OnOptionalPropertyEnd(bool present) final;

	void OnObjectBegin() final;
	void OnObjectEnd() final;
	idx_t OnListBegin() final;
	void OnListEnd() final;
	bool OnNullableBegin() final;
	void OnNullableEnd() final;

	//===--------------------------------------------------------------------===//
	// Primitive Types
	//===--------------------------------------------------------------------===//
	bool ReadBool() final;
	int8_t ReadSignedInt8() final;
	uint8_t ReadUnsignedInt8() final;
	int16_t ReadSignedInt16() final;
	uint16_t ReadUnsignedInt16() final;
	int32_t ReadSignedInt32() final;
	uint32_t ReadUnsignedInt32() final;
	int64_t ReadSignedInt64() final;
	uint64_t ReadUnsignedInt64() final;
	float ReadFloat() final;
	double ReadDouble() final;
	string ReadString() final;
	hugeint_t ReadHugeInt() final;
	uhugeint_t ReadUhugeInt() final;
	void ReadDataPtr(data_ptr_t &ptr, idx_t count) final;
};

} // namespace duckdb
