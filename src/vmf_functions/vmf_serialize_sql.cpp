#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "vmf_deserializer.hpp"
#include "vmf_functions.hpp"
#include "vmf_serializer.hpp"

namespace duckdb {

struct VmfSerializeBindData : public FunctionData {
	bool skip_if_null = false;
	bool skip_if_empty = false;
	bool skip_if_default = false;
	bool format = false;

	VmfSerializeBindData(bool skip_if_null_p, bool skip_if_empty_p, bool skip_if_default_p, bool format_p)
	    : skip_if_null(skip_if_null_p), skip_if_empty(skip_if_empty_p), skip_if_default(skip_if_default_p),
	      format(format_p) {
	}

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<VmfSerializeBindData>(skip_if_null, skip_if_empty, skip_if_default, format);
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

static unique_ptr<FunctionData> VmfSerializeBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty()) {
		throw BinderException("vmf_serialize_sql takes at least one argument");
	}

	if (arguments[0]->return_type != LogicalType::VARCHAR) {
		throw InvalidTypeException("vmf_serialize_sql first argument must be a VARCHAR");
	}

	// Optional arguments

	bool skip_if_null = false;
	bool skip_if_empty = false;
	bool skip_if_default = false;
	bool format = false;

	for (idx_t i = 1; i < arguments.size(); i++) {
		auto &arg = arguments[i];
		if (arg->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arg->IsFoldable()) {
			throw BinderException("vmf_serialize_sql: arguments must be constant");
		}
		if (arg->alias == "skip_null") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("vmf_serialize_sql: 'skip_null' argument must be a boolean");
			}
			skip_if_null = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else if (arg->alias == "skip_empty") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("vmf_serialize_sql: 'skip_empty' argument must be a boolean");
			}
			skip_if_empty = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else if (arg->alias == "format") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("vmf_serialize_sql: 'format' argument must be a boolean");
			}
			format = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else if (arg->alias == "skip_default") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("vmf_serialize_sql: 'skip_default' argument must be a boolean");
			}
			skip_if_default = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else {
			throw BinderException(StringUtil::Format("vmf_serialize_sql: Unknown argument '%s'", arg->alias));
		}
	}
	return make_uniq<VmfSerializeBindData>(skip_if_null, skip_if_empty, skip_if_default, format);
}

static void VmfSerializeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &local_state = VMFFunctionLocalState::ResetAndGet(state);
	auto alc = local_state.vmf_allocator.GetYYAlc();
	auto &inputs = args.data[0];

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<VmfSerializeBindData>();

	UnaryExecutor::Execute<string_t, string_t>(inputs, result, args.size(), [&](string_t input) {
		auto doc = VMFCommon::CreateDocument(alc);
		auto result_obj = yyjson_mut_obj(doc);
		yyjson_mut_doc_set_root(doc, result_obj);

		try {
			auto parser = Parser();
			parser.ParseQuery(input.GetString());

			auto statements_arr = yyjson_mut_arr(doc);

			for (auto &statement : parser.statements) {
				if (statement->type != StatementType::SELECT_STATEMENT) {
					throw NotImplementedException("Only SELECT statements can be serialized to vmf!");
				}
				auto &select = statement->Cast<SelectStatement>();
				auto vmf =
				    VmfSerializer::Serialize(select, doc, info.skip_if_null, info.skip_if_empty, info.skip_if_default);

				yyjson_mut_arr_append(statements_arr, vmf);
			}

			yyjson_mut_obj_add_false(doc, result_obj, "error");
			yyjson_mut_obj_add_val(doc, result_obj, "statements", statements_arr);
			idx_t len;
			auto data = yyjson_mut_val_write_opts(result_obj,
			                                      info.format ? VMFCommon::WRITE_PRETTY_FLAG : VMFCommon::WRITE_FLAG,
			                                      alc, reinterpret_cast<size_t *>(&len), nullptr);
			if (data == nullptr) {
				throw SerializationException(
				    "Failed to serialize vmf, perhaps the query contains invalid utf8 characters?");
			}
			return StringVector::AddString(result, data, len);

		} catch (std::exception &ex) {
			ErrorData error(ex);
			yyjson_mut_obj_add_true(doc, result_obj, "error");
			yyjson_mut_obj_add_strcpy(doc, result_obj, "error_type",
			                          StringUtil::Lower(Exception::ExceptionTypeToString(error.Type())).c_str());
			yyjson_mut_obj_add_strcpy(doc, result_obj, "error_message", error.RawMessage().c_str());
			// add extra info
			for (auto &entry : error.ExtraInfo()) {
				yyjson_mut_obj_add_strcpy(doc, result_obj, entry.first.c_str(), entry.second.c_str());
			}

			idx_t len;
			auto data = yyjson_mut_val_write_opts(result_obj,
			                                      info.format ? VMFCommon::WRITE_PRETTY_FLAG : VMFCommon::WRITE_FLAG,
			                                      alc, reinterpret_cast<size_t *>(&len), nullptr);
			return StringVector::AddString(result, data, len);
		}
	});
}

ScalarFunctionSet VMFFunctions::GetSerializeSqlFunction() {
	ScalarFunctionSet set("vmf_serialize_sql");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VMF(), VmfSerializeFunction,
	                               VmfSerializeBind, nullptr, nullptr, VMFFunctionLocalState::Init));

	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN}, LogicalType::VMF(),
	                               VmfSerializeFunction, VmfSerializeBind, nullptr, nullptr,
	                               VMFFunctionLocalState::Init));

	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN},
	                               LogicalType::VMF(), VmfSerializeFunction, VmfSerializeBind, nullptr, nullptr,
	                               VMFFunctionLocalState::Init));

	set.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VMF(),
	    VmfSerializeFunction, VmfSerializeBind, nullptr, nullptr, VMFFunctionLocalState::Init));

	return set;
}

//----------------------------------------------------------------------
// VMF DESERIALIZE
//----------------------------------------------------------------------
static unique_ptr<SelectStatement> DeserializeSelectStatement(string_t input, yyjson_alc *alc) {
	auto doc = VMFCommon::ReadDocument(input, VMFCommon::READ_FLAG, alc);
	if (!doc) {
		throw ParserException("Could not parse vmf");
	}
	auto root = doc->root;
	auto err = yyjson_obj_get(root, "error");
	if (err && yyjson_is_true(err)) {
		auto err_type = yyjson_obj_get(root, "error_type");
		auto err_msg = yyjson_obj_get(root, "error_message");
		if (err_type && err_msg) {
			throw ParserException("Error parsing vmf: %s: %s", yyjson_get_str(err_type), yyjson_get_str(err_msg));
		}
		throw ParserException(
		    "Error parsing vmf, expected error property to contain 'error_type' and 'error_message'");
	}

	auto statements = yyjson_obj_get(root, "statements");
	if (!statements || !yyjson_is_arr(statements)) {
		throw ParserException("Error parsing vmf: no statements array");
	}
	auto size = yyjson_arr_size(statements);
	if (size == 0) {
		throw ParserException("Error parsing vmf: no statements");
	}
	if (size > 1) {
		throw ParserException("Error parsing vmf: more than one statement");
	}
	auto stmt_vmf = yyjson_arr_get(statements, 0);
	VmfDeserializer deserializer(stmt_vmf, doc);
	auto stmt = SelectStatement::Deserialize(deserializer);
	if (!stmt->node) {
		throw ParserException("Error parsing vmf: no select node found in vmf");
	}
	return stmt;
}

//----------------------------------------------------------------------
// VMF DESERIALIZE SQL FUNCTION
//----------------------------------------------------------------------
static void VmfDeserializeFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &local_state = VMFFunctionLocalState::ResetAndGet(state);
	auto alc = local_state.vmf_allocator.GetYYAlc();
	auto &inputs = args.data[0];

	UnaryExecutor::Execute<string_t, string_t>(inputs, result, args.size(), [&](string_t input) {
		auto stmt = DeserializeSelectStatement(input, alc);
		return StringVector::AddString(result, stmt->ToString());
	});
}

ScalarFunctionSet VMFFunctions::GetDeserializeSqlFunction() {
	ScalarFunctionSet set("vmf_deserialize_sql");
	set.AddFunction(ScalarFunction({LogicalType::VMF()}, LogicalType::VARCHAR, VmfDeserializeFunction, nullptr,
	                               nullptr, nullptr, VMFFunctionLocalState::Init));
	return set;
}

//----------------------------------------------------------------------
// VMF EXECUTE SERIALIZED SQL (PRAGMA)
//----------------------------------------------------------------------
static string ExecuteVmfSerializedSqlPragmaFunction(ClientContext &context, const FunctionParameters &parameters) {
	VMFFunctionLocalState local_state(context);
	auto alc = local_state.vmf_allocator.GetYYAlc();

	auto input = parameters.values[0].GetValueUnsafe<string_t>();
	auto stmt = DeserializeSelectStatement(input, alc);
	return stmt->ToString();
}

PragmaFunctionSet VMFFunctions::GetExecuteVmfSerializedSqlPragmaFunction() {
	return PragmaFunctionSet(PragmaFunction::PragmaCall(
	    "vmf_execute_serialized_sql", ExecuteVmfSerializedSqlPragmaFunction, {LogicalType::VARCHAR}));
}

//----------------------------------------------------------------------
// VMF EXECUTE SERIALIZED SQL (TABLE FUNCTION)
//----------------------------------------------------------------------
struct ExecuteSqlTableFunction {
	struct BindData : public TableFunctionData {
		shared_ptr<Relation> plan;
		unique_ptr<QueryResult> result;
		unique_ptr<Connection> con;
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {
		VMFFunctionLocalState local_state(context);
		auto alc = local_state.vmf_allocator.GetYYAlc();

		auto result = make_uniq<BindData>();

		result->con = make_uniq<Connection>(*context.db);
		if (input.inputs[0].IsNull()) {
			throw BinderException("vmf_execute_serialized_sql cannot execute NULL plan");
		}
		auto serialized = input.inputs[0].GetValueUnsafe<string>();
		auto stmt = DeserializeSelectStatement(serialized, alc);
		result->plan = result->con->RelationFromQuery(std::move(stmt));

		for (auto &col : result->plan->Columns()) {
			return_types.emplace_back(col.Type());
			names.emplace_back(col.Name());
		}
		return std::move(result);
	}

	static void Function(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		auto &data = (BindData &)*data_p.bind_data;
		if (!data.result) {
			data.result = data.plan->Execute();
		}
		auto result_chunk = data.result->Fetch();
		if (!result_chunk) {
			return;
		}
		output.Move(*result_chunk);
	}
};

TableFunctionSet VMFFunctions::GetExecuteVmfSerializedSqlFunction() {
	TableFunction func("vmf_execute_serialized_sql", {LogicalType::VARCHAR}, ExecuteSqlTableFunction::Function,
	                   ExecuteSqlTableFunction::Bind);
	return TableFunctionSet(func);
}

} // namespace duckdb
