#include "vmf_structure.hpp"

#include "duckdb/common/enum_util.hpp"
#include "vmf_executors.hpp"
#include "vmf_scan.hpp"
#include "vmf_transform.hpp"

#include <duckdb/common/extra_type_info.hpp>

namespace duckdb {

static bool IsNumeric(LogicalTypeId type) {
	return type == LogicalTypeId::DOUBLE || type == LogicalTypeId::UBIGINT || type == LogicalTypeId::BIGINT;
}

static LogicalTypeId MaxNumericType(const LogicalTypeId &a, const LogicalTypeId &b) {
	D_ASSERT(a != b);
	if (a == LogicalTypeId::DOUBLE || b == LogicalTypeId::DOUBLE) {
		return LogicalTypeId::DOUBLE;
	}
	return LogicalTypeId::BIGINT;
}

VMFStructureNode::VMFStructureNode() : count(0), null_count(0) {
}

VMFStructureNode::VMFStructureNode(const char *key_ptr, const size_t key_len) : VMFStructureNode() {
	key = make_uniq<string>(key_ptr, key_len);
}

VMFStructureNode::VMFStructureNode(yyvmf_val *key_p, yyvmf_val *val_p, const bool ignore_errors)
    : VMFStructureNode(unsafe_yyvmf_get_str(key_p), unsafe_yyvmf_get_len(key_p)) {
	VMFStructure::ExtractStructure(val_p, *this, ignore_errors);
}

static void SwapVMFStructureNode(VMFStructureNode &a, VMFStructureNode &b) noexcept {
	std::swap(a.key, b.key);
	std::swap(a.initialized, b.initialized);
	std::swap(a.descriptions, b.descriptions);
	std::swap(a.count, b.count);
	std::swap(a.null_count, b.null_count);
}

VMFStructureNode::VMFStructureNode(VMFStructureNode &&other) noexcept {
	SwapVMFStructureNode(*this, other);
}

VMFStructureNode &VMFStructureNode::operator=(VMFStructureNode &&other) noexcept {
	SwapVMFStructureNode(*this, other);
	return *this;
}

VMFStructureDescription &VMFStructureNode::GetOrCreateDescription(const LogicalTypeId type) {
	if (descriptions.empty()) {
		// Empty, just put this type in there
		descriptions.emplace_back(type);
		return descriptions.back();
	}

	if (descriptions.size() == 1 && descriptions[0].type == LogicalTypeId::SQLNULL) {
		// Only a NULL in there, override
		descriptions[0].type = type;
		return descriptions[0];
	}

	if (type == LogicalTypeId::SQLNULL) {
		// 'descriptions' is non-empty, so let's not add NULL
		return descriptions.back();
	}

	// Check if type is already in there or if we can merge numerics
	const auto is_numeric = IsNumeric(type);
	for (auto &description : descriptions) {
		if (type == description.type) {
			return description;
		}
		if (is_numeric && IsNumeric(description.type)) {
			description.type = MaxNumericType(type, description.type);
			return description;
		}
	}
	// Type was not there, create a new description
	descriptions.emplace_back(type);
	return descriptions.back();
}

bool VMFStructureNode::ContainsVarchar() const {
	if (descriptions.size() != 1) {
		// We can't refine types if we have more than 1 description (yet), defaults to VMF type for now
		return false;
	}
	auto &description = descriptions[0];
	if (description.type == LogicalTypeId::VARCHAR) {
		return true;
	}
	for (auto &child : description.children) {
		if (child.ContainsVarchar()) {
			return true;
		}
	}

	return false;
}

void VMFStructureNode::InitializeCandidateTypes(const idx_t max_depth, const bool convert_strings_to_integers,
                                                 const idx_t depth) {
	if (depth >= max_depth) {
		return;
	}
	if (descriptions.size() != 1) {
		// We can't refine types if we have more than 1 description (yet), defaults to VMF type for now
		return;
	}
	auto &description = descriptions[0];
	if (description.type == LogicalTypeId::VARCHAR && !initialized) {
		// We loop through the candidate types and format templates from back to front
		if (convert_strings_to_integers) {
			description.candidate_types = {LogicalTypeId::UUID, LogicalTypeId::BIGINT, LogicalTypeId::TIMESTAMP,
			                               LogicalTypeId::DATE, LogicalTypeId::TIME};
		} else {
			description.candidate_types = {LogicalTypeId::UUID, LogicalTypeId::TIMESTAMP, LogicalTypeId::DATE,
			                               LogicalTypeId::TIME};
		}
		initialized = true;
	} else {
		for (auto &child : description.children) {
			child.InitializeCandidateTypes(max_depth, convert_strings_to_integers, depth + 1);
		}
	}
}

void VMFStructureNode::RefineCandidateTypes(yyvmf_val *vals[], const idx_t val_count, Vector &string_vector,
                                             ArenaAllocator &allocator, DateFormatMap &date_format_map) {
	if (descriptions.size() != 1) {
		// We can't refine types if we have more than 1 description (yet), defaults to VMF type for now
		return;
	}
	if (!ContainsVarchar()) {
		return;
	}
	auto &description = descriptions[0];
	switch (description.type) {
	case LogicalTypeId::LIST:
		return RefineCandidateTypesArray(vals, val_count, string_vector, allocator, date_format_map);
	case LogicalTypeId::STRUCT:
		return RefineCandidateTypesObject(vals, val_count, string_vector, allocator, date_format_map);
	case LogicalTypeId::VARCHAR:
		return RefineCandidateTypesString(vals, val_count, string_vector, date_format_map);
	default:
		return;
	}
}

void VMFStructureNode::RefineCandidateTypesArray(yyvmf_val *vals[], const idx_t val_count, Vector &string_vector,
                                                  ArenaAllocator &allocator, DateFormatMap &date_format_map) {
	D_ASSERT(descriptions.size() == 1 && descriptions[0].type == LogicalTypeId::LIST);
	auto &desc = descriptions[0];
	D_ASSERT(desc.children.size() == 1);
	auto &child = desc.children[0];

	idx_t total_list_size = 0;
	for (idx_t i = 0; i < val_count; i++) {
		if (vals[i] && !unsafe_yyvmf_is_null(vals[i])) {
			D_ASSERT(yyvmf_is_arr(vals[i]));
			total_list_size += unsafe_yyvmf_get_len(vals[i]);
		}
	}

	idx_t offset = 0;
	auto child_vals =
	    reinterpret_cast<yyvmf_val **>(allocator.AllocateAligned(total_list_size * sizeof(yyvmf_val *)));

	size_t idx, max;
	yyvmf_val *child_val;
	for (idx_t i = 0; i < val_count; i++) {
		if (vals[i] && !unsafe_yyvmf_is_null(vals[i])) {
			yyvmf_arr_foreach(vals[i], idx, max, child_val) {
				child_vals[offset++] = child_val;
			}
		}
	}
	child.RefineCandidateTypes(child_vals, total_list_size, string_vector, allocator, date_format_map);
}

void VMFStructureNode::RefineCandidateTypesObject(yyvmf_val *vals[], const idx_t val_count, Vector &string_vector,
                                                   ArenaAllocator &allocator, DateFormatMap &date_format_map) {
	D_ASSERT(descriptions.size() == 1 && descriptions[0].type == LogicalTypeId::STRUCT);
	auto &desc = descriptions[0];

	const idx_t child_count = desc.children.size();
	vector<yyvmf_val **> child_vals;
	child_vals.reserve(child_count);
	for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
		child_vals.emplace_back(
		    reinterpret_cast<yyvmf_val **>(allocator.AllocateAligned(val_count * sizeof(yyvmf_val *))));
	}

	const auto found_keys = reinterpret_cast<bool *>(allocator.AllocateAligned(sizeof(bool) * child_count));

	const auto &key_map = desc.key_map;
	size_t idx, max;
	yyvmf_val *child_key, *child_val;
	for (idx_t i = 0; i < val_count; i++) {
		if (vals[i] && !unsafe_yyvmf_is_null(vals[i])) {
			idx_t found_key_count = 0;
			memset(found_keys, false, child_count);

			D_ASSERT(yyvmf_is_obj(vals[i]));
			yyvmf_obj_foreach(vals[i], idx, max, child_key, child_val) {
				D_ASSERT(yyvmf_is_str(child_key));
				const auto key_ptr = unsafe_yyvmf_get_str(child_key);
				const auto key_len = unsafe_yyvmf_get_len(child_key);
				auto it = key_map.find({key_ptr, key_len});
				D_ASSERT(it != key_map.end());
				const auto child_idx = it->second;
				child_vals[child_idx][i] = child_val;
				found_keys[child_idx] = true;
				found_key_count++;
			}

			if (found_key_count != child_count) {
				// Set child val to nullptr so recursion doesn't break
				for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
					if (!found_keys[child_idx]) {
						child_vals[child_idx][i] = nullptr;
					}
				}
			}
		} else {
			for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
				child_vals[child_idx][i] = nullptr;
			}
		}
	}

	for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
		desc.children[child_idx].RefineCandidateTypes(child_vals[child_idx], val_count, string_vector, allocator,
		                                              date_format_map);
	}
}

void VMFStructureNode::RefineCandidateTypesString(yyvmf_val *vals[], const idx_t val_count, Vector &string_vector,
                                                   DateFormatMap &date_format_map) {
	D_ASSERT(descriptions.size() == 1 && descriptions[0].type == LogicalTypeId::VARCHAR);
	if (descriptions[0].candidate_types.empty()) {
		return;
	}
	static VMFTransformOptions OPTIONS;
	VMFTransform::GetStringVector(vals, val_count, LogicalType::SQLNULL, string_vector, OPTIONS);
	EliminateCandidateTypes(val_count, string_vector, date_format_map);
}

void VMFStructureNode::EliminateCandidateTypes(const idx_t vec_count, Vector &string_vector,
                                                DateFormatMap &date_format_map) {
	D_ASSERT(descriptions.size() == 1 && descriptions[0].type == LogicalTypeId::VARCHAR);
	auto &description = descriptions[0];
	auto &candidate_types = description.candidate_types;
	while (true) {
		if (candidate_types.empty()) {
			return;
		}
		const auto type = candidate_types.back();
		Vector result_vector(type, vec_count);
		if (date_format_map.HasFormats(type)) {
			auto &formats = date_format_map.GetCandidateFormats(type);
			if (EliminateCandidateFormats(vec_count, string_vector, result_vector, formats)) {
				return;
			} else {
				candidate_types.pop_back();
			}
		} else {
			string error_message;
			if (!VectorOperations::DefaultTryCast(string_vector, result_vector, vec_count, &error_message, true)) {
				candidate_types.pop_back();
			} else {
				return;
			}
		}
	}
}

template <class OP, class T>
bool TryParse(Vector &string_vector, StrpTimeFormat &format, const idx_t count) {
	const auto strings = FlatVector::GetData<string_t>(string_vector);
	const auto &validity = FlatVector::Validity(string_vector);

	T result;
	string error_message;
	if (validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			if (!OP::template Operation<T>(format, strings[i], result, error_message)) {
				return false;
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			if (validity.RowIsValid(i)) {
				if (!OP::template Operation<T>(format, strings[i], result, error_message)) {
					return false;
				}
			}
		}
	}
	return true;
}

bool VMFStructureNode::EliminateCandidateFormats(const idx_t vec_count, Vector &string_vector,
                                                  const Vector &result_vector, vector<StrpTimeFormat> &formats) {
	D_ASSERT(descriptions.size() == 1 && descriptions[0].type == LogicalTypeId::VARCHAR);
	const auto type = result_vector.GetType().id();
	for (idx_t i = formats.size(); i != 0; i--) {
		const idx_t actual_index = i - 1;
		auto &format = formats[actual_index];
		bool success;
		switch (type) {
		case LogicalTypeId::DATE:
			success = TryParse<TryParseDate, date_t>(string_vector, format, vec_count);
			break;
		case LogicalTypeId::TIMESTAMP:
			success = TryParse<TryParseTimeStamp, timestamp_t>(string_vector, format, vec_count);
			break;
		default:
			throw InternalException("No date/timestamp formats for %s", EnumUtil::ToString(type));
		}
		if (success) {
			while (formats.size() > i) {
				formats.pop_back();
			}
			return true;
		}
	}
	return false;
}

VMFStructureDescription::VMFStructureDescription(const LogicalTypeId type_p) : type(type_p) {
}

static void SwapVMFStructureDescription(VMFStructureDescription &a, VMFStructureDescription &b) noexcept {
	std::swap(a.type, b.type);
	std::swap(a.key_map, b.key_map);
	std::swap(a.children, b.children);
	std::swap(a.candidate_types, b.candidate_types);
}

VMFStructureDescription::VMFStructureDescription(VMFStructureDescription &&other) noexcept {
	SwapVMFStructureDescription(*this, other);
}

VMFStructureDescription &VMFStructureDescription::operator=(VMFStructureDescription &&other) noexcept {
	SwapVMFStructureDescription(*this, other);
	return *this;
}

VMFStructureNode &VMFStructureDescription::GetOrCreateChild() {
	D_ASSERT(type == LogicalTypeId::LIST);
	if (children.empty()) {
		children.emplace_back();
	}
	D_ASSERT(children.size() == 1);
	return children.back();
}

VMFStructureNode &VMFStructureDescription::GetOrCreateChild(const char *key_ptr, const size_t key_size) {
	// Check if there is already a child with the same key
	const VMFKey temp_key {key_ptr, key_size};
	const auto it = key_map.find(temp_key);
	if (it != key_map.end()) {
		return children[it->second]; // Found it
	}

	// Didn't find, create a new child
	children.emplace_back(key_ptr, key_size);
	const auto &persistent_key_string = *children.back().key;
	VMFKey new_key {persistent_key_string.c_str(), persistent_key_string.length()};
	key_map.emplace(new_key, children.size() - 1);
	return children.back();
}

VMFStructureNode &VMFStructureDescription::GetOrCreateChild(yyvmf_val *key, yyvmf_val *val,
                                                              const bool ignore_errors) {
	D_ASSERT(yyvmf_is_str(key));
	auto &child = GetOrCreateChild(unsafe_yyvmf_get_str(key), unsafe_yyvmf_get_len(key));
	VMFStructure::ExtractStructure(val, child, ignore_errors);
	return child;
}

static void ExtractStructureArray(yyvmf_val *arr, VMFStructureNode &node, const bool ignore_errors) {
	D_ASSERT(yyvmf_is_arr(arr));
	auto &description = node.GetOrCreateDescription(LogicalTypeId::LIST);
	auto &child = description.GetOrCreateChild();

	size_t idx, max;
	yyvmf_val *val;
	yyvmf_arr_foreach(arr, idx, max, val) {
		VMFStructure::ExtractStructure(val, child, ignore_errors);
	}
}

static void ExtractStructureObject(yyvmf_val *obj, VMFStructureNode &node, const bool ignore_errors) {
	D_ASSERT(yyvmf_is_obj(obj));
	auto &description = node.GetOrCreateDescription(LogicalTypeId::STRUCT);

	// Keep track of keys so we can detect duplicates
	unordered_set<string> obj_keys;
	case_insensitive_set_t ci_obj_keys;

	size_t idx, max;
	yyvmf_val *key, *val;
	yyvmf_obj_foreach(obj, idx, max, key, val) {
		const string obj_key(unsafe_yyvmf_get_str(key), unsafe_yyvmf_get_len(key));
		auto insert_result = obj_keys.insert(obj_key);
		if (!ignore_errors && !insert_result.second) { // Exact match
			VMFCommon::ThrowValFormatError("Duplicate key \"" + obj_key + "\" in object %s", obj);
		}
		insert_result = ci_obj_keys.insert(obj_key);
		if (!ignore_errors && !insert_result.second) { // Case-insensitive match
			VMFCommon::ThrowValFormatError("Duplicate key (different case) \"" + obj_key + "\" and \"" +
			                                    *insert_result.first + "\" in object %s",
			                                obj);
		}
		description.GetOrCreateChild(key, val, ignore_errors);
	}
}

static void ExtractStructureVal(yyvmf_val *val, VMFStructureNode &node) {
	D_ASSERT(!yyvmf_is_arr(val) && !yyvmf_is_obj(val));
	node.GetOrCreateDescription(VMFCommon::ValTypeToLogicalTypeId(val));
}

void VMFStructure::ExtractStructure(yyvmf_val *val, VMFStructureNode &node, const bool ignore_errors) {
	node.count++;
	const auto tag = yyvmf_get_tag(val);
	if (tag == (YYVMF_TYPE_NULL | YYVMF_SUBTYPE_NONE)) {
		node.null_count++;
	}

	switch (tag) {
	case YYVMF_TYPE_ARR | YYVMF_SUBTYPE_NONE:
		return ExtractStructureArray(val, node, ignore_errors);
	case YYVMF_TYPE_OBJ | YYVMF_SUBTYPE_NONE:
		return ExtractStructureObject(val, node, ignore_errors);
	default:
		return ExtractStructureVal(val, node);
	}
}

VMFStructureNode ExtractStructureInternal(yyvmf_val *val, const bool ignore_errors) {
	VMFStructureNode node;
	VMFStructure::ExtractStructure(val, node, ignore_errors);
	return node;
}

//! Forward declaration for recursion
static yyvmf_mut_val *ConvertStructure(const VMFStructureNode &node, yyvmf_mut_doc *doc);

static yyvmf_mut_val *ConvertStructureArray(const VMFStructureNode &node, yyvmf_mut_doc *doc) {
	D_ASSERT(node.descriptions.size() == 1 && node.descriptions[0].type == LogicalTypeId::LIST);
	const auto &desc = node.descriptions[0];
	D_ASSERT(desc.children.size() == 1);

	const auto arr = yyvmf_mut_arr(doc);
	yyvmf_mut_arr_append(arr, ConvertStructure(desc.children[0], doc));
	return arr;
}

static yyvmf_mut_val *ConvertStructureObject(const VMFStructureNode &node, yyvmf_mut_doc *doc) {
	D_ASSERT(node.descriptions.size() == 1 && node.descriptions[0].type == LogicalTypeId::STRUCT);
	auto &desc = node.descriptions[0];
	if (desc.children.empty()) {
		// Empty struct - let's do VMF instead
		return yyvmf_mut_str(doc, LogicalType::VMF_TYPE_NAME);
	}

	const auto obj = yyvmf_mut_obj(doc);
	for (auto &child : desc.children) {
		D_ASSERT(child.key);
		yyvmf_mut_obj_add(obj, yyvmf_mut_strn(doc, child.key->c_str(), child.key->length()),
		                   ConvertStructure(child, doc));
	}
	return obj;
}

static yyvmf_mut_val *ConvertStructure(const VMFStructureNode &node, yyvmf_mut_doc *doc) {
	if (node.descriptions.empty()) {
		return yyvmf_mut_str(doc, VMFCommon::TYPE_STRING_NULL);
	}
	if (node.descriptions.size() != 1) { // Inconsistent types, so we resort to VMF
		return yyvmf_mut_str(doc, LogicalType::VMF_TYPE_NAME);
	}
	auto &desc = node.descriptions[0];
	D_ASSERT(desc.type != LogicalTypeId::INVALID);
	switch (desc.type) {
	case LogicalTypeId::LIST:
		return ConvertStructureArray(node, doc);
	case LogicalTypeId::STRUCT:
		return ConvertStructureObject(node, doc);
	default:
		return yyvmf_mut_str(doc, EnumUtil::ToChars(desc.type));
	}
}

static string_t VMFStructureFunction(yyvmf_val *val, yyvmf_alc *alc, Vector &, ValidityMask &, idx_t) {
	return VMFCommon::WriteVal<yyvmf_mut_val>(
	    ConvertStructure(ExtractStructureInternal(val, true), yyvmf_mut_doc_new(alc)), alc);
}

static void StructureFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VMFExecutors::UnaryExecute<string_t>(args, state, result, VMFStructureFunction);
}

static void GetStructureFunctionInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type}, LogicalType::VMF(), StructureFunction, nullptr, nullptr, nullptr,
	                               VMFFunctionLocalState::Init));
}

ScalarFunctionSet VMFFunctions::GetStructureFunction() {
	ScalarFunctionSet set("vmf_structure");
	GetStructureFunctionInternal(set, LogicalType::VARCHAR);
	GetStructureFunctionInternal(set, LogicalType::VMF());
	return set;
}

static LogicalType StructureToTypeArray(ClientContext &context, const VMFStructureNode &node, const idx_t max_depth,
                                        const double field_appearance_threshold, const idx_t map_inference_threshold,
                                        const idx_t depth, const LogicalType &null_type) {
	D_ASSERT(node.descriptions.size() == 1 && node.descriptions[0].type == LogicalTypeId::LIST);
	const auto &desc = node.descriptions[0];
	D_ASSERT(desc.children.size() == 1);

	return LogicalType::LIST(VMFStructure::StructureToType(context, desc.children[0], max_depth,
	                                                        field_appearance_threshold, map_inference_threshold,
	                                                        depth + 1, null_type));
}

static void MergeNodes(VMFStructureNode &merged, const VMFStructureNode &node);

static void MergeNodeArray(VMFStructureNode &merged, const VMFStructureDescription &child_desc) {
	D_ASSERT(child_desc.type == LogicalTypeId::LIST);
	auto &merged_desc = merged.GetOrCreateDescription(LogicalTypeId::LIST);
	auto &merged_child = merged_desc.GetOrCreateChild();
	for (auto &list_child : child_desc.children) {
		MergeNodes(merged_child, list_child);
	}
}

static void MergeNodeObject(VMFStructureNode &merged, const VMFStructureDescription &child_desc) {
	D_ASSERT(child_desc.type == LogicalTypeId::STRUCT);
	auto &merged_desc = merged.GetOrCreateDescription(LogicalTypeId::STRUCT);
	for (auto &struct_child : child_desc.children) {
		const auto &struct_child_key = *struct_child.key;
		auto &merged_child = merged_desc.GetOrCreateChild(struct_child_key.c_str(), struct_child_key.length());
		MergeNodes(merged_child, struct_child);
	}
}

static void MergeNodeVal(VMFStructureNode &merged, const VMFStructureDescription &child_desc,
                         const bool node_initialized) {
	D_ASSERT(child_desc.type != LogicalTypeId::LIST && child_desc.type != LogicalTypeId::STRUCT);
	auto &merged_desc = merged.GetOrCreateDescription(child_desc.type);
	if (merged_desc.type != LogicalTypeId::VARCHAR || !node_initialized || merged.descriptions.size() != 1) {
		return;
	}
	if (!merged.initialized) {
		merged_desc.candidate_types = child_desc.candidate_types;
	} else if (!merged_desc.candidate_types.empty() && !child_desc.candidate_types.empty() &&
	           merged_desc.candidate_types.back() != child_desc.candidate_types.back()) {
		merged_desc.candidate_types.clear(); // Not the same, default to VARCHAR
	}
	merged.initialized = true;
}

static void MergeNodes(VMFStructureNode &merged, const VMFStructureNode &node) {
	merged.count += node.count;
	merged.null_count += node.null_count;
	for (const auto &child_desc : node.descriptions) {
		switch (child_desc.type) {
		case LogicalTypeId::LIST:
			MergeNodeArray(merged, child_desc);
			break;
		case LogicalTypeId::STRUCT:
			MergeNodeObject(merged, child_desc);
			break;
		default:
			MergeNodeVal(merged, child_desc, node.initialized);
			break;
		}
	}
}

static double CalculateTypeSimilarity(const LogicalType &merged, const LogicalType &type, idx_t max_depth, idx_t depth);

static double CalculateMapAndStructSimilarity(const LogicalType &map_type, const LogicalType &struct_type,
                                              const bool swapped, const idx_t max_depth, const idx_t depth) {
	const auto &map_value_type = MapType::ValueType(map_type);
	const auto &struct_child_types = StructType::GetChildTypes(struct_type);
	double total_similarity = 0;
	for (const auto &struct_child_type : struct_child_types) {
		const auto similarity =
		    swapped ? CalculateTypeSimilarity(struct_child_type.second, map_value_type, max_depth, depth + 1)
		            : CalculateTypeSimilarity(map_value_type, struct_child_type.second, max_depth, depth + 1);
		if (similarity < 0) {
			return similarity;
		}
		total_similarity += similarity;
	}
	return total_similarity / static_cast<double>(struct_child_types.size());
}

static double CalculateTypeSimilarity(const LogicalType &merged, const LogicalType &type, const idx_t max_depth,
                                      const idx_t depth) {
	if (depth >= max_depth || merged.id() == LogicalTypeId::SQLNULL || type.id() == LogicalTypeId::SQLNULL) {
		return 1;
	}
	if (merged.IsVMFType()) {
		// Incompatible types
		return -1;
	}
	if (type.IsVMFType() || merged == type) {
		return 1;
	}

	switch (merged.id()) {
	case LogicalTypeId::STRUCT: {
		if (type.id() == LogicalTypeId::MAP) {
			// This can happen for empty structs/maps ("{}"), or in rare cases where an inconsistent struct becomes
			// consistent when merged, but does not have enough children to be considered a map.
			return CalculateMapAndStructSimilarity(type, merged, true, max_depth, depth);
		}

		// Only structs can be merged into a struct
		D_ASSERT(type.id() == LogicalTypeId::STRUCT);
		const auto &merged_child_types = StructType::GetChildTypes(merged);
		const auto &type_child_types = StructType::GetChildTypes(type);

		unordered_map<string, const LogicalType &> merged_child_types_map;
		for (const auto &merged_child : merged_child_types) {
			merged_child_types_map.emplace(merged_child.first, merged_child.second);
		}

		double total_similarity = 0;
		for (const auto &type_child_type : type_child_types) {
			const auto it = merged_child_types_map.find(type_child_type.first);
			if (it == merged_child_types_map.end()) {
				return -1;
			}
			const auto similarity = CalculateTypeSimilarity(it->second, type_child_type.second, max_depth, depth + 1);
			if (similarity < 0) {
				return similarity;
			}
			total_similarity += similarity;
		}
		return total_similarity / static_cast<double>(merged_child_types.size());
	}
	case LogicalTypeId::MAP: {
		if (type.id() == LogicalTypeId::MAP) {
			return CalculateTypeSimilarity(MapType::ValueType(merged), MapType::ValueType(type), max_depth, depth + 1);
		}

		// Only maps and structs can be merged into a map
		D_ASSERT(type.id() == LogicalTypeId::STRUCT);
		return CalculateMapAndStructSimilarity(merged, type, false, max_depth, depth);
	}
	case LogicalTypeId::LIST: {
		// Only lists can be merged into a list
		D_ASSERT(type.id() == LogicalTypeId::LIST);
		const auto &merged_child_type = ListType::GetChildType(merged);
		const auto &type_child_type = ListType::GetChildType(type);
		return CalculateTypeSimilarity(merged_child_type, type_child_type, max_depth, depth + 1);
	}
	default:
		// This is only reachable if type has been inferred using candidate_types, but candidate_types were not
		// consistent among all map values
		return 1;
	}
}

static bool IsStructureInconsistent(const VMFStructureDescription &desc, const idx_t sample_count,
                                    const idx_t null_count, const double field_appearance_threshold) {
	D_ASSERT(sample_count > null_count);
	double total_child_counts = 0;
	for (const auto &child : desc.children) {
		total_child_counts += static_cast<double>(child.count) / static_cast<double>(sample_count - null_count);
	}
	const auto avg_occurrence = total_child_counts / static_cast<double>(desc.children.size());
	return avg_occurrence < field_appearance_threshold;
}

static LogicalType GetMergedType(ClientContext &context, const VMFStructureNode &node, const idx_t max_depth,
                                 const double field_appearance_threshold, const idx_t map_inference_threshold,
                                 const idx_t depth, const LogicalType &null_type) {
	D_ASSERT(node.descriptions.size() == 1);
	auto &desc = node.descriptions[0];
	VMFStructureNode merged;
	for (const auto &child : desc.children) {
		MergeNodes(merged, child);
	}
	return VMFStructure::StructureToType(context, merged, max_depth, field_appearance_threshold,
	                                      map_inference_threshold, depth + 1, null_type);
}

static LogicalType StructureToTypeObject(ClientContext &context, const VMFStructureNode &node, const idx_t max_depth,
                                         const double field_appearance_threshold, const idx_t map_inference_threshold,
                                         const idx_t depth, const LogicalType &null_type) {
	D_ASSERT(node.descriptions.size() == 1 && node.descriptions[0].type == LogicalTypeId::STRUCT);
	auto &desc = node.descriptions[0];

	// If it's an empty struct we do MAP of VMF instead
	if (desc.children.empty()) {
		// Empty struct - let's do MAP of VMF instead
		return LogicalType::MAP(LogicalType::VARCHAR, null_type);
	}

	// If it's an inconsistent object we also just do MAP with the best-possible, recursively-merged value type
	if (IsStructureInconsistent(desc, node.count, node.null_count, field_appearance_threshold)) {
		return LogicalType::MAP(LogicalType::VARCHAR,
		                        GetMergedType(context, node, max_depth, field_appearance_threshold,
		                                      map_inference_threshold, depth + 1, null_type));
	}

	// We have a consistent object
	child_list_t<LogicalType> child_types;
	child_types.reserve(desc.children.size());
	for (auto &child : desc.children) {
		D_ASSERT(child.key);
		child_types.emplace_back(*child.key,
		                         VMFStructure::StructureToType(context, child, max_depth, field_appearance_threshold,
		                                                        map_inference_threshold, depth + 1, null_type));
	}

	// If we have many children and all children have similar-enough types we infer map
	if (desc.children.size() >= map_inference_threshold) {
		LogicalType map_value_type = GetMergedType(context, node, max_depth, field_appearance_threshold,
		                                           map_inference_threshold, depth + 1, LogicalTypeId::SQLNULL);

		double total_similarity = 0;
		for (const auto &child_type : child_types) {
			const auto similarity = CalculateTypeSimilarity(map_value_type, child_type.second, max_depth, depth + 1);
			if (similarity < 0) {
				total_similarity = similarity;
				break;
			}
			total_similarity += similarity;
		}
		const auto avg_similarity = total_similarity / static_cast<double>(child_types.size());
		if (avg_similarity >= 0.8) {
			if (null_type != LogicalTypeId::SQLNULL) {
				map_value_type = GetMergedType(context, node, max_depth, field_appearance_threshold,
				                               map_inference_threshold, depth + 1, null_type);
			}
			return LogicalType::MAP(LogicalType::VARCHAR, map_value_type);
		}
	}

	return LogicalType::STRUCT(child_types);
}

static LogicalType StructureToTypeString(const VMFStructureNode &node) {
	D_ASSERT(node.descriptions.size() == 1 && node.descriptions[0].type == LogicalTypeId::VARCHAR);
	auto &desc = node.descriptions[0];
	if (desc.candidate_types.empty()) {
		return LogicalTypeId::VARCHAR;
	}
	return desc.candidate_types.back();
}

LogicalType VMFStructure::StructureToType(ClientContext &context, const VMFStructureNode &node, const idx_t max_depth,
                                           const double field_appearance_threshold, const idx_t map_inference_threshold,
                                           const idx_t depth, const LogicalType &null_type) {
	if (depth >= max_depth) {
		return LogicalType::VMF();
	}
	if (node.descriptions.empty()) {
		return null_type;
	}
	if (node.descriptions.size() != 1) { // Inconsistent types, so we resort to VMF
		return LogicalType::VMF();
	}
	auto &desc = node.descriptions[0];
	D_ASSERT(desc.type != LogicalTypeId::INVALID);
	switch (desc.type) {
	case LogicalTypeId::LIST:
		return StructureToTypeArray(context, node, max_depth, field_appearance_threshold, map_inference_threshold,
		                            depth, null_type);
	case LogicalTypeId::STRUCT:
		return StructureToTypeObject(context, node, max_depth, field_appearance_threshold, map_inference_threshold,
		                             depth, null_type);
	case LogicalTypeId::VARCHAR:
		return StructureToTypeString(node);
	case LogicalTypeId::UBIGINT:
		return LogicalTypeId::BIGINT; // We prefer not to return UBIGINT in our type auto-detection
	case LogicalTypeId::SQLNULL:
		return null_type;
	default:
		return desc.type;
	}
}

} // namespace duckdb
