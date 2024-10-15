#include "vmf_functions.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

using VMFPathType = VMFCommon::VMFPathType;

static VMFPathType CheckPath(const Value &path_val, string &path, size_t &len) {
	if (path_val.IsNull()) {
		throw BinderException("VMF path cannot be NULL");
	}
	const auto path_str_val = path_val.DefaultCastAs(LogicalType::VARCHAR);
	auto path_str = path_str_val.GetValueUnsafe<string_t>();
	len = path_str.GetSize();
	const auto ptr = path_str.GetData();
	// Empty strings and invalid $ paths yield an error
	if (len == 0) {
		throw BinderException("Empty VMF path");
	}
	VMFPathType path_type = VMFPathType::REGULAR;
	// Copy over string to the bind data
	if (*ptr == '/' || *ptr == '$') {
		path = string(ptr, len);
	} else if (path_val.type().IsIntegral()) {
		path = "$[" + string(ptr, len) + "]";
	} else if (memchr(ptr, '"', len)) {
		path = "/" + string(ptr, len);
	} else {
		path = "$.\"" + string(ptr, len) + "\"";
	}
	len = path.length();
	if (*path.c_str() == '$') {
		path_type = VMFCommon::ValidatePath(path.c_str(), len, true);
	}
	return path_type;
}

VMFReadFunctionData::VMFReadFunctionData(bool constant, string path_p, idx_t len, VMFPathType path_type_p)
    : constant(constant), path(std::move(path_p)), path_type(path_type_p), ptr(path.c_str()), len(len) {
}

unique_ptr<FunctionData> VMFReadFunctionData::Copy() const {
	return make_uniq<VMFReadFunctionData>(constant, path, len, path_type);
}

bool VMFReadFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<VMFReadFunctionData>();
	return constant == other.constant && path == other.path && len == other.len && path_type == other.path_type;
}

unique_ptr<FunctionData> VMFReadFunctionData::Bind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	bool constant = false;
	string path;
	size_t len = 0;
	VMFPathType path_type = VMFPathType::REGULAR;
	if (arguments[1]->IsFoldable()) {
		const auto path_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		if (!path_val.IsNull()) {
			constant = true;
			path_type = CheckPath(path_val, path, len);
		}
	}
	if (arguments[1]->return_type.IsIntegral()) {
		bound_function.arguments[1] = LogicalType::BIGINT;
	} else {
		bound_function.arguments[1] = LogicalType::VARCHAR;
	}
	if (path_type == VMFCommon::VMFPathType::WILDCARD) {
		bound_function.return_type = LogicalType::LIST(bound_function.return_type);
	}
	return make_uniq<VMFReadFunctionData>(constant, std::move(path), len, path_type);
}

VMFReadManyFunctionData::VMFReadManyFunctionData(vector<string> paths_p, vector<size_t> lens_p)
    : paths(std::move(paths_p)), lens(std::move(lens_p)) {
	for (const auto &path : paths) {
		ptrs.push_back(path.c_str());
	}
}

unique_ptr<FunctionData> VMFReadManyFunctionData::Copy() const {
	return make_uniq<VMFReadManyFunctionData>(paths, lens);
}

bool VMFReadManyFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<VMFReadManyFunctionData>();
	return paths == other.paths && lens == other.lens;
}

unique_ptr<FunctionData> VMFReadManyFunctionData::Bind(ClientContext &context, ScalarFunction &bound_function,
                                                        vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("List of paths must be constant");
	}

	vector<string> paths;
	vector<size_t> lens;
	auto paths_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);

	for (auto &path_val : ListValue::GetChildren(paths_val)) {
		paths.emplace_back("");
		lens.push_back(0);
		if (CheckPath(path_val, paths.back(), lens.back()) == VMFPathType::WILDCARD) {
			throw BinderException("Cannot have wildcards in VMF path when supplying multiple paths");
		}
	}

	return make_uniq<VMFReadManyFunctionData>(std::move(paths), std::move(lens));
}

VMFFunctionLocalState::VMFFunctionLocalState(Allocator &allocator) : vmf_allocator(allocator) {
}

VMFFunctionLocalState::VMFFunctionLocalState(ClientContext &context)
    : VMFFunctionLocalState(BufferAllocator::Get(context)) {
}

unique_ptr<FunctionLocalState> VMFFunctionLocalState::Init(ExpressionState &state, const BoundFunctionExpression &expr,
                                                            FunctionData *bind_data) {
	return make_uniq<VMFFunctionLocalState>(state.GetContext());
}

unique_ptr<FunctionLocalState> VMFFunctionLocalState::InitCastLocalState(CastLocalStateParameters &parameters) {
	return parameters.context ? make_uniq<VMFFunctionLocalState>(*parameters.context)
	                          : make_uniq<VMFFunctionLocalState>(Allocator::DefaultAllocator());
}

VMFFunctionLocalState &VMFFunctionLocalState::ResetAndGet(ExpressionState &state) {
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<VMFFunctionLocalState>();
	lstate.vmf_allocator.Reset();
	return lstate;
}

vector<ScalarFunctionSet> VMFFunctions::GetScalarFunctions() {
	vector<ScalarFunctionSet> functions;

	// Extract functions
	AddAliases({"vmf_extract", "vmf_extract_path"}, GetExtractFunction(), functions);
	AddAliases({"vmf_extract_string", "vmf_extract_path_text", "->>"}, GetExtractStringFunction(), functions);

	// Create functions
	functions.push_back(GetArrayFunction());
	functions.push_back(GetObjectFunction());
	AddAliases({"to_vmf", "vmf_quote"}, GetToVMFFunction(), functions);
	functions.push_back(GetArrayToVMFFunction());
	functions.push_back(GetRowToVMFFunction());
	functions.push_back(GetMergePatchFunction());

	// Structure/Transform
	functions.push_back(GetStructureFunction());
	AddAliases({"vmf_transform", "from_vmf"}, GetTransformFunction(), functions);
	AddAliases({"vmf_transform_strict", "from_vmf_strict"}, GetTransformStrictFunction(), functions);

	// Other
	functions.push_back(GetArrayLengthFunction());
	functions.push_back(GetContainsFunction());
	functions.push_back(GetExistsFunction());
	functions.push_back(GetKeysFunction());
	functions.push_back(GetTypeFunction());
	functions.push_back(GetValidFunction());
	functions.push_back(GetValueFunction());
	functions.push_back(GetSerializePlanFunction());
	functions.push_back(GetSerializeSqlFunction());
	functions.push_back(GetDeserializeSqlFunction());

	functions.push_back(GetPrettyPrintFunction());

	return functions;
}

vector<PragmaFunctionSet> VMFFunctions::GetPragmaFunctions() {
	vector<PragmaFunctionSet> functions;
	functions.push_back(GetExecuteVmfSerializedSqlPragmaFunction());
	return functions;
}

vector<TableFunctionSet> VMFFunctions::GetTableFunctions() {
	vector<TableFunctionSet> functions;

	// Reads VMF as string
	functions.push_back(GetReadVMFObjectsFunction());
	functions.push_back(GetReadNDVMFObjectsFunction());
	functions.push_back(GetReadVMFObjectsAutoFunction());

	// Read VMF as columnar data
	functions.push_back(GetReadVMFFunction());
	functions.push_back(GetReadNDVMFFunction());
	functions.push_back(GetReadVMFAutoFunction());
	functions.push_back(GetReadNDVMFAutoFunction());
	functions.push_back(GetExecuteVmfSerializedSqlFunction());

	return functions;
}

unique_ptr<TableRef> VMFFunctions::ReadVMFReplacement(ClientContext &context, ReplacementScanInput &input,
                                                        optional_ptr<ReplacementScanData> data) {
	auto table_name = ReplacementScan::GetFullPath(input);
	if (!ReplacementScan::CanReplace(table_name, {"vmf", "vmfl", "ndvmf"})) {
		return nullptr;
	}
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	table_function->function = make_uniq<FunctionExpression>("read_vmf_auto", std::move(children));

	if (!FileSystem::HasGlob(table_name)) {
		auto &fs = FileSystem::GetFileSystem(context);
		table_function->alias = fs.ExtractBaseName(table_name);
	}

	return std::move(table_function);
}

static bool CastVarcharToVMF(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &lstate = parameters.local_state->Cast<VMFFunctionLocalState>();
	lstate.vmf_allocator.Reset();
	auto alc = lstate.vmf_allocator.GetYYAlc();

	bool success = true;
	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
	    source, result, count, [&](string_t input, ValidityMask &mask, idx_t idx) {
		    auto data = input.GetDataWriteable();
		    const auto length = input.GetSize();

		    yyvmf_read_err error;
		    auto doc = VMFCommon::ReadDocumentUnsafe(data, length, VMFCommon::READ_FLAG, alc, &error);

		    if (!doc) {
			    mask.SetInvalid(idx);
			    if (success) {
				    HandleCastError::AssignError(VMFCommon::FormatParseError(data, length, error), parameters);
				    success = false;
			    }
		    }

		    return input;
	    });
	StringVector::AddHeapReference(result, source);
	return success;
}

void VMFFunctions::RegisterSimpleCastFunctions(CastFunctionSet &casts) {
	// VMF to VARCHAR is basically free
	casts.RegisterCastFunction(LogicalType::VMF(), LogicalType::VARCHAR, DefaultCasts::ReinterpretCast, 1);

	// VARCHAR to VMF requires a parse so it's not free. Let's make it 1 more than a cast to STRUCT
	auto varchar_to_vmf_cost = casts.ImplicitCastCost(LogicalType::SQLNULL, LogicalTypeId::STRUCT) + 1;
	BoundCastInfo varchar_to_vmf_info(CastVarcharToVMF, nullptr, VMFFunctionLocalState::InitCastLocalState);
	casts.RegisterCastFunction(LogicalType::VARCHAR, LogicalType::VMF(), std::move(varchar_to_vmf_info),
	                           varchar_to_vmf_cost);

	// Register NULL to VMF with a different cost than NULL to VARCHAR so the binder can disambiguate functions
	auto null_to_vmf_cost = casts.ImplicitCastCost(LogicalType::SQLNULL, LogicalTypeId::VARCHAR) + 1;
	casts.RegisterCastFunction(LogicalType::SQLNULL, LogicalType::VMF(), DefaultCasts::TryVectorNullCast,
	                           null_to_vmf_cost);
}

} // namespace duckdb
