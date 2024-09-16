#define DUCKDB_EXTENSION_MAIN

#include "quack_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {


// To add a new scalar SQL macro, add a new macro to this array!
// Copy and paste the top item in the array into the 
// second-to-last position and make some modifications. 
// (essentially, leave the last entry in the array as {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr} )

// Keep the DEFAULT_SCHEMA (no change needed)
// Replace "times_two" with a name for your macro
// If your function has parameters without default values, add their names in quotes inside of the {}, with a nullptr at the end
//      If you do not have parameters, simplify to {nullptr}
// If your function has parameters with default values, add their names and values in double quotes inside of {}'s inside of the {}.
//      If you have default values that are strings, wrap them in single quotes inside the double quotes 
//          (Ex: {{"my_string":"'my_default_value'"}, {nullptr, nullptr}} )
//      Be sure to keep {nullptr, nullptr} at the end
//      If you do not have parameters with default values, simplify to {nullptr, nullptr}
// Add the text of your SQL macro as a raw string with the format R"( select 42 )"
// clang-format off
static DefaultMacro quack_macros[] = {
    {DEFAULT_SCHEMA, "times_two", {"x", nullptr}, {{nullptr, nullptr}}, R"( x * 2 )"},
    {DEFAULT_SCHEMA, "default_to_times_two", {"x", nullptr}, {{"multiplier", "2"}, {nullptr, nullptr}}, R"( x * multiplier )"},
    {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
};
// clang-format on

// To add a new SQL table macro, add a new macro to this array!
// Copy and paste the top item in the array into the 
// second-to-last position and make some modifications. 
// (essentially, leave the last entry in the array as {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}

// Keep the DEFAULT_SCHEMA (no change needed)
// Replace "times_two_table" with a name for your macro
// If your function has parameters without default values, add their names in quotes inside of the {}, with a nullptr at the end
//      If you do not have parameters, simplify to {nullptr}
// If your function has parameters with default values, add their names and values in double quotes inside of {}'s inside of the {}.
//      If you have default values that are strings, wrap them in single quotes inside the double quotes 
//          (Ex: {{"my_string":"'my_default_value'"}, {nullptr, nullptr}} )
//      Be sure to keep {nullptr, nullptr} at the end
//      If you do not have parameters with default values, simplify to {nullptr, nullptr}
// Add the text of your SQL macro as a raw string with the format R"( select 42; )" 

// clang-format off 
static const DefaultTableMacro quack_table_macros[] = {
    {DEFAULT_SCHEMA, "times_two_table", {"x", nullptr}, {{nullptr, nullptr}},  R"( SELECT x * 2 as output_column; )"},
    {DEFAULT_SCHEMA, "default_to_times_two_table", {"x", nullptr}, {{"multiplier", "2"}, {nullptr, nullptr}},  R"( SELECT x * multiplier as output_column; )"},
	{nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
};
// clang-format on

inline void QuackScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Quack "+name.GetString()+" 🐥");;
        });
}

inline void QuackOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Quack " + name.GetString() +
                                                     ", my linked OpenSSL version is " +
                                                     OPENSSL_VERSION_TEXT );;
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register Scalar Macros
	for (idx_t index = 0; quack_macros[index].name != nullptr; index++) {
		auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(quack_macros[index]);
		ExtensionUtil::RegisterFunction(instance, *info);
	}
    // Register Table Macros
    for (idx_t index = 0; quack_table_macros[index].name != nullptr; index++) {
		auto table_info = DefaultTableFunctionGenerator::CreateTableMacroInfo(quack_table_macros[index]);
        ExtensionUtil::RegisterFunction(instance, *table_info);
	}
    
    // Register a scalar function
    auto quack_scalar_function = ScalarFunction("quack", {LogicalType::VARCHAR}, LogicalType::VARCHAR, QuackScalarFun);
    ExtensionUtil::RegisterFunction(instance, quack_scalar_function);

    // Register another scalar function
    auto quack_openssl_version_scalar_function = ScalarFunction("quack_openssl_version", {LogicalType::VARCHAR},
                                                LogicalType::VARCHAR, QuackOpenSSLVersionScalarFun);
    ExtensionUtil::RegisterFunction(instance, quack_openssl_version_scalar_function);
}

void QuackExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string QuackExtension::Name() {
	return "quack";
}

std::string QuackExtension::Version() const {
#ifdef EXT_VERSION_QUACK
	return EXT_VERSION_QUACK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void quack_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::QuackExtension>();
}

DUCKDB_EXTENSION_API const char *quack_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
