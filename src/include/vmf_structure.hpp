//===----------------------------------------------------------------------===//
//                         DuckDB
//
// vmf_structure.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "vmf_common.hpp"

namespace duckdb {

struct VMFStructureDescription;
struct DateFormatMap;
struct StrpTimeFormat;

struct VMFStructureNode {
public:
	VMFStructureNode();
	VMFStructureNode(const char *key_ptr, const size_t key_len);
	VMFStructureNode(yyvmf_val *key_p, yyvmf_val *val_p, bool ignore_errors);

	//! Disable copy constructors
	VMFStructureNode(const VMFStructureNode &other) = delete;
	VMFStructureNode &operator=(const VMFStructureNode &) = delete;
	//! Enable move constructors
	VMFStructureNode(VMFStructureNode &&other) noexcept;
	VMFStructureNode &operator=(VMFStructureNode &&) noexcept;

	VMFStructureDescription &GetOrCreateDescription(LogicalTypeId type);

	bool ContainsVarchar() const;
	void InitializeCandidateTypes(idx_t max_depth, bool convert_strings_to_integers, idx_t depth = 0);
	void RefineCandidateTypes(yyvmf_val *vals[], idx_t val_count, Vector &string_vector, ArenaAllocator &allocator,
	                          DateFormatMap &date_format_map);

private:
	void RefineCandidateTypesArray(yyvmf_val *vals[], idx_t val_count, Vector &string_vector,
	                               ArenaAllocator &allocator, DateFormatMap &date_format_map);
	void RefineCandidateTypesObject(yyvmf_val *vals[], idx_t val_count, Vector &string_vector,
	                                ArenaAllocator &allocator, DateFormatMap &date_format_map);
	void RefineCandidateTypesString(yyvmf_val *vals[], idx_t val_count, Vector &string_vector,
	                                DateFormatMap &date_format_map);
	void EliminateCandidateTypes(idx_t vec_count, Vector &string_vector, DateFormatMap &date_format_map);
	bool EliminateCandidateFormats(idx_t vec_count, Vector &string_vector, const Vector &result_vector,
	                               vector<StrpTimeFormat> &formats);

public:
	unique_ptr<string> key;
	bool initialized = false;
	vector<VMFStructureDescription> descriptions;
	idx_t count;
	idx_t null_count;
};

struct VMFStructureDescription {
public:
	explicit VMFStructureDescription(LogicalTypeId type_p);
	//! Disable copy constructors
	VMFStructureDescription(const VMFStructureDescription &other) = delete;
	VMFStructureDescription &operator=(const VMFStructureDescription &) = delete;
	//! Enable move constructors
	VMFStructureDescription(VMFStructureDescription &&other) noexcept;
	VMFStructureDescription &operator=(VMFStructureDescription &&) noexcept;

	VMFStructureNode &GetOrCreateChild();
	VMFStructureNode &GetOrCreateChild(const char *key_ptr, size_t key_size);
	VMFStructureNode &GetOrCreateChild(yyvmf_val *key, yyvmf_val *val, bool ignore_errors);

public:
	//! Type of this description
	LogicalTypeId type = LogicalTypeId::INVALID;

	//! Map to children and children
	vmf_key_map_t<idx_t> key_map;
	vector<VMFStructureNode> children;

	//! Candidate types (if auto-detecting and type == LogicalTypeId::VARCHAR)
	vector<LogicalTypeId> candidate_types;
};

struct VMFStructure {
public:
	static void ExtractStructure(yyvmf_val *val, VMFStructureNode &node, bool ignore_errors);
	static LogicalType StructureToType(ClientContext &context, const VMFStructureNode &node, idx_t max_depth,
	                                   double field_appearance_threshold, idx_t map_inference_threshold,
	                                   idx_t depth = 0, const LogicalType &null_type = LogicalType::VMF());
};

} // namespace duckdb
