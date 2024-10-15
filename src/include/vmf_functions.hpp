//===----------------------------------------------------------------------===//
//                         DuckDB
//
// vmf_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/extension_util.hpp"
#include "vmf_common.hpp"

namespace duckdb {

class TableRef;
struct ReplacementScanData;
class CastFunctionSet;
struct CastParameters;
struct CastLocalStateParameters;
struct VMFScanInfo;
class BuiltinFunctions;

// Scalar function stuff
struct VMFReadFunctionData : public FunctionData {
public:
	VMFReadFunctionData(bool constant, string path_p, idx_t len, VMFCommon::VMFPathType path_type);
	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments);

public:
	const bool constant;
	const string path;
	const VMFCommon::VMFPathType path_type;
	const char *ptr;
	const size_t len;
};

struct VMFReadManyFunctionData : public FunctionData {
public:
	VMFReadManyFunctionData(vector<string> paths_p, vector<size_t> lens_p);
	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments);

public:
	const vector<string> paths;
	vector<const char *> ptrs;
	const vector<size_t> lens;
};

struct VMFFunctionLocalState : public FunctionLocalState {
public:
	explicit VMFFunctionLocalState(Allocator &allocator);
	explicit VMFFunctionLocalState(ClientContext &context);
	static unique_ptr<FunctionLocalState> Init(ExpressionState &state, const BoundFunctionExpression &expr,
	                                           FunctionData *bind_data);
	static unique_ptr<FunctionLocalState> InitCastLocalState(CastLocalStateParameters &parameters);
	static VMFFunctionLocalState &ResetAndGet(ExpressionState &state);

public:
	VMFAllocator vmf_allocator;
};

class VMFFunctions {
public:
	static vector<ScalarFunctionSet> GetScalarFunctions();
	static vector<PragmaFunctionSet> GetPragmaFunctions();
	static vector<TableFunctionSet> GetTableFunctions();
	static unique_ptr<TableRef> ReadVMFReplacement(ClientContext &context, ReplacementScanInput &input,
	                                                optional_ptr<ReplacementScanData> data);
	static TableFunction GetReadVMFTableFunction(shared_ptr<VMFScanInfo> function_info);
	static CopyFunction GetVMFCopyFunction();
	static void RegisterSimpleCastFunctions(CastFunctionSet &casts);
	static void RegisterVMFCreateCastFunctions(CastFunctionSet &casts);
	static void RegisterVMFTransformCastFunctions(CastFunctionSet &casts);

private:
	// Scalar functions
	static ScalarFunctionSet GetExtractFunction();
	static ScalarFunctionSet GetExtractStringFunction();

	static ScalarFunctionSet GetArrayFunction();
	static ScalarFunctionSet GetObjectFunction();
	static ScalarFunctionSet GetToVMFFunction();
	static ScalarFunctionSet GetArrayToVMFFunction();
	static ScalarFunctionSet GetRowToVMFFunction();
	static ScalarFunctionSet GetMergePatchFunction();

	static ScalarFunctionSet GetStructureFunction();
	static ScalarFunctionSet GetTransformFunction();
	static ScalarFunctionSet GetTransformStrictFunction();

	static ScalarFunctionSet GetArrayLengthFunction();
	static ScalarFunctionSet GetContainsFunction();
	static ScalarFunctionSet GetExistsFunction();
	static ScalarFunctionSet GetKeysFunction();
	static ScalarFunctionSet GetTypeFunction();
	static ScalarFunctionSet GetValidFunction();
	static ScalarFunctionSet GetValueFunction();
	static ScalarFunctionSet GetSerializeSqlFunction();
	static ScalarFunctionSet GetDeserializeSqlFunction();
	static ScalarFunctionSet GetSerializePlanFunction();

	static ScalarFunctionSet GetPrettyPrintFunction();

	static PragmaFunctionSet GetExecuteVmfSerializedSqlPragmaFunction();

	template <class FUNCTION_INFO>
	static void AddAliases(const vector<string> &names, FUNCTION_INFO fun, vector<FUNCTION_INFO> &functions) {
		for (auto &name : names) {
			fun.name = name;
			functions.push_back(fun);
		}
	}

private:
	// Table functions
	static TableFunctionSet GetReadVMFObjectsFunction();
	static TableFunctionSet GetReadNDVMFObjectsFunction();
	static TableFunctionSet GetReadVMFObjectsAutoFunction();

	static TableFunctionSet GetReadVMFFunction();
	static TableFunctionSet GetReadNDVMFFunction();
	static TableFunctionSet GetReadVMFAutoFunction();
	static TableFunctionSet GetReadNDVMFAutoFunction();

	static TableFunctionSet GetExecuteVmfSerializedSqlFunction();
};

} // namespace duckdb
