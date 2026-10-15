#define DUCKDB_EXTENSION_MAIN

#include "waddle_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void WaddleScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "...........🦆 " + name.GetString());
	});
}

inline void WaddleOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Waddle " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function. Going through CreateScalarFunctionInfo instead of passing the
	// ScalarFunction directly lets us attach a FunctionDescription, which is what makes the function
	// self-documenting: its description, parameter names, examples and categories all show up in
	// duckdb_functions().
	{
		CreateScalarFunctionInfo info(
		    ScalarFunction("waddle", {LogicalType::VARCHAR}, LogicalType::VARCHAR, WaddleScalarFun));

		FunctionDescription desc;
		desc.parameter_names = {"name"};
		desc.description = "Returns `name` prefixed with a waddling duck.";
		desc.examples = {"waddle('Jane')"};
		desc.categories = {"string"};
		info.descriptions.push_back(std::move(desc));

		// ALTER_ON_CONFLICT adds an overload if a function with this name already exists
		info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
		loader.RegisterFunction(std::move(info));
	}

	// Register another scalar function
	{
		CreateScalarFunctionInfo info(ScalarFunction("waddle_openssl_version", {LogicalType::VARCHAR},
		                                             LogicalType::VARCHAR, WaddleOpenSSLVersionScalarFun));

		FunctionDescription desc;
		desc.parameter_names = {"name"};
		desc.description = "Returns `name` together with the OpenSSL version this extension was linked against.";
		desc.examples = {"waddle_openssl_version('Michael')"};
		desc.categories = {"string"};
		info.descriptions.push_back(std::move(desc));

		info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
		loader.RegisterFunction(std::move(info));
	}
}

void WaddleExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string WaddleExtension::Name() {
	return "waddle";
}

std::string WaddleExtension::Version() const {
#ifdef EXT_VERSION_WADDLE
	return EXT_VERSION_WADDLE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(waddle, loader) {
	duckdb::LoadInternal(loader);
}
}
