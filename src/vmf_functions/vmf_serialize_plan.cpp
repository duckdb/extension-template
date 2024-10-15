#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "vmf_common.hpp"
#include "vmf_deserializer.hpp"
#include "vmf_functions.hpp"
#include "vmf_serializer.hpp"

namespace duckdb {

//-----------------------------------------------------------------------------
// vmf_serialize_plan
//-----------------------------------------------------------------------------
struct VmfSerializePlanBindData : public FunctionData {
	bool skip_if_null = false;
	bool skip_if_empty = false;
	bool skip_if_default = false;
	bool format = false;
	bool optimize = false;

	VmfSerializePlanBindData(bool skip_if_null_p, bool skip_if_empty_p, bool skip_if_default_p, bool format_p,
	                          bool optimize_p)
	    : skip_if_null(skip_if_null_p), skip_if_empty(skip_if_empty_p), skip_if_default(skip_if_default_p),
	      format(format_p), optimize(optimize_p) {
	}

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<VmfSerializePlanBindData>(skip_if_null, skip_if_empty, skip_if_default, format, optimize);
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

static unique_ptr<FunctionData> VmfSerializePlanBind(ClientContext &context, ScalarFunction &bound_function,
                                                      vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty()) {
		throw BinderException("vmf_serialize_plan takes at least one argument");
	}

	if (arguments[0]->return_type != LogicalType::VARCHAR) {
		throw InvalidTypeException("vmf_serialize_plan first argument must be a VARCHAR");
	}

	// Optional arguments
	bool skip_if_null = false;
	bool skip_if_empty = false;
	bool skip_if_default = false;
	bool format = false;
	bool optimize = false;

	for (idx_t i = 1; i < arguments.size(); i++) {
		auto &arg = arguments[i];
		if (arg->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arg->IsFoldable()) {
			throw BinderException("vmf_serialize_plan: arguments must be constant");
		}
		if (arg->alias == "skip_null") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("vmf_serialize_plan: 'skip_null' argument must be a boolean");
			}
			skip_if_null = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else if (arg->alias == "skip_empty") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("vmf_serialize_plan: 'skip_empty' argument must be a boolean");
			}
			skip_if_empty = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else if (arg->alias == "skip_default") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("vmf_serialize_plan: 'skip_default' argument must be a boolean");
			}
			skip_if_default = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else if (arg->alias == "format") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("vmf_serialize_plan: 'format' argument must be a boolean");
			}
			format = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else if (arg->alias == "optimize") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("vmf_serialize_plan: 'optimize' argument must be a boolean");
			}
			optimize = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else {
			throw BinderException(StringUtil::Format("vmf_serialize_plan: Unknown argument '%s'", arg->alias.c_str()));
		}
	}
	return make_uniq<VmfSerializePlanBindData>(skip_if_null, skip_if_empty, skip_if_default, format, optimize);
}

static bool OperatorSupportsSerialization(LogicalOperator &op, string &operator_name) {
	for (auto &child : op.children) {
		if (!OperatorSupportsSerialization(*child, operator_name)) {
			return false;
		}
	}
	auto supported = op.SupportSerialization();
	if (!supported) {
		operator_name = EnumUtil::ToString(op.type);
	}
	return supported;
}

static void VmfSerializePlanFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &local_state = VMFFunctionLocalState::ResetAndGet(state);
	auto alc = local_state.vmf_allocator.GetYYAlc();
	auto &inputs = args.data[0];

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<VmfSerializePlanBindData>();

	if (!state.HasContext()) {
		throw InvalidInputException("vmf_serialize_plan: No client context available");
	}
	auto &context = state.GetContext();

	UnaryExecutor::Execute<string_t, string_t>(inputs, result, args.size(), [&](string_t input) {
		auto doc = VMFCommon::CreateDocument(alc);
		auto result_obj = yyvmf_mut_obj(doc);
		yyvmf_mut_doc_set_root(doc, result_obj);

		try {
			Parser parser;
			parser.ParseQuery(input.GetString());
			auto plans_arr = yyvmf_mut_arr(doc);

			for (auto &statement : parser.statements) {
				auto stmt = std::move(statement);

				Planner planner(context);
				planner.CreatePlan(std::move(stmt));
				auto plan = std::move(planner.plan);

				if (info.optimize && plan->RequireOptimizer()) {
					Optimizer optimizer(*planner.binder, context);
					plan = optimizer.Optimize(std::move(plan));
				}

				ColumnBindingResolver resolver;
				resolver.Verify(*plan);
				resolver.VisitOperator(*plan);
				plan->ResolveOperatorTypes();

				string operator_name;
				if (!OperatorSupportsSerialization(*plan, operator_name)) {
					throw InvalidInputException("Operator '%s' does not support serialization", operator_name);
				}

				auto plan_vmf =
				    VmfSerializer::Serialize(*plan, doc, info.skip_if_null, info.skip_if_empty, info.skip_if_default);
				yyvmf_mut_arr_append(plans_arr, plan_vmf);
			}

			yyvmf_mut_obj_add_false(doc, result_obj, "error");
			yyvmf_mut_obj_add_val(doc, result_obj, "plans", plans_arr);

			idx_t len;
			auto data = yyvmf_mut_val_write_opts(result_obj,
			                                      info.format ? VMFCommon::WRITE_PRETTY_FLAG : VMFCommon::WRITE_FLAG,
			                                      alc, reinterpret_cast<size_t *>(&len), nullptr);
			if (data == nullptr) {
				throw SerializationException(
				    "Failed to serialize vmf, perhaps the query contains invalid utf8 characters?");
			}

			return StringVector::AddString(result, data, len);

		} catch (std::exception &ex) {
			ErrorData error(ex);
			yyvmf_mut_obj_add_true(doc, result_obj, "error");
			// error type and message
			yyvmf_mut_obj_add_strcpy(doc, result_obj, "error_type",
			                          StringUtil::Lower(Exception::ExceptionTypeToString(error.Type())).c_str());
			yyvmf_mut_obj_add_strcpy(doc, result_obj, "error_message", error.RawMessage().c_str());
			// add extra info
			for (auto &entry : error.ExtraInfo()) {
				yyvmf_mut_obj_add_strcpy(doc, result_obj, entry.first.c_str(), entry.second.c_str());
			}

			idx_t len;
			auto data = yyvmf_mut_val_write_opts(result_obj,
			                                      info.format ? VMFCommon::WRITE_PRETTY_FLAG : VMFCommon::WRITE_FLAG,
			                                      alc, reinterpret_cast<size_t *>(&len), nullptr);
			return StringVector::AddString(result, data, len);
		}
	});
}

ScalarFunctionSet VMFFunctions::GetSerializePlanFunction() {
	ScalarFunctionSet set("vmf_serialize_plan");

	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VMF(), VmfSerializePlanFunction,
	                               VmfSerializePlanBind, nullptr, nullptr, VMFFunctionLocalState::Init));

	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN}, LogicalType::VMF(),
	                               VmfSerializePlanFunction, VmfSerializePlanBind, nullptr, nullptr,
	                               VMFFunctionLocalState::Init));

	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN},
	                               LogicalType::VMF(), VmfSerializePlanFunction, VmfSerializePlanBind, nullptr,
	                               nullptr, VMFFunctionLocalState::Init));

	set.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VMF(),
	    VmfSerializePlanFunction, VmfSerializePlanBind, nullptr, nullptr, VMFFunctionLocalState::Init));
	set.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN},
	    LogicalType::VMF(), VmfSerializePlanFunction, VmfSerializePlanBind, nullptr, nullptr,
	    VMFFunctionLocalState::Init));
	return set;
}

} // namespace duckdb
