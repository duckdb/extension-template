//===----------------------------------------------------------------------===//
//                         DuckDB
//
// vmf_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar/strftime_format.hpp"
#include "vmf_common.hpp"

namespace duckdb {

struct DateFormatMap;
class BufferedVMFReader;

//! Options for error handling while transforming VMF
struct VMFTransformOptions {
public:
	VMFTransformOptions();
	VMFTransformOptions(bool strict_cast, bool error_duplicate_key, bool error_missing_key, bool error_unkown_key);

public:
	//! Throws an error if the cast doesn't work (instead of NULL-ing it)
	bool strict_cast = false;
	//! Throws an error if there is a duplicate key (instead of ignoring it)
	bool error_duplicate_key = false;
	//! Throws an error if a key is missing (instead of NULL-ing it)
	bool error_missing_key = false;
	//! Throws an error if an object has a key we didn't know about
	bool error_unknown_key = false;

	//! Whether to delay the error when transforming (e.g., when non-strict casting or reading from file)
	bool delay_error = false;
	//! Date format used for parsing (can be NULL)
	optional_ptr<DateFormatMap> date_format_map = nullptr;
	//! String to store errors in
	string error_message;
	//! Index of the object where the error occurred
	idx_t object_index = DConstants::INVALID_INDEX;
	//! Cast parameters
	CastParameters parameters;

public:
	void Serialize(Serializer &serializer) const;
	static VMFTransformOptions Deserialize(Deserializer &deserializer);
};

struct TryParseDate {
	template <class T>
	static inline bool Operation(StrpTimeFormat &format, const string_t &input, T &result, string &error_message) {
		return format.TryParseDate(input, result, error_message);
	}
};

struct TryParseTimeStamp {
	template <class T>
	static inline bool Operation(StrpTimeFormat &format, const string_t &input, T &result, string &error_message) {
		return format.TryParseTimestamp(input, result, error_message);
	}
};

struct VMFTransform {
	static bool Transform(yyvmf_val *vals[], yyvmf_alc *alc, Vector &result, const idx_t count,
	                      VMFTransformOptions &options);
	static bool TransformObject(yyvmf_val *objects[], yyvmf_alc *alc, const idx_t count, const vector<string> &names,
	                            const vector<Vector *> &result_vectors, VMFTransformOptions &options);
	static bool GetStringVector(yyvmf_val *vals[], const idx_t count, const LogicalType &target, Vector &string_vector,
	                            VMFTransformOptions &options);
};

} // namespace duckdb
