#define DUCKDB_EXTENSION_MAIN

#include "vmf_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void VmfScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Vmf "+name.GetString()+" 🐥");;
        });
}

inline void VmfOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Vmf " + name.GetString() +
                                                     ", my linked OpenSSL version is " +
                                                     OPENSSL_VERSION_TEXT );;
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto vmf_scalar_function = ScalarFunction("vmf", {LogicalType::VARCHAR}, LogicalType::VARCHAR, VmfScalarFun);
    ExtensionUtil::RegisterFunction(instance, vmf_scalar_function);

    // Register another scalar function
    auto vmf_openssl_version_scalar_function = ScalarFunction("vmf_openssl_version", {LogicalType::VARCHAR},
                                                LogicalType::VARCHAR, VmfOpenSSLVersionScalarFun);
    ExtensionUtil::RegisterFunction(instance, vmf_openssl_version_scalar_function);
}

void VmfExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string VmfExtension::Name() {
	return "vmf";
}

std::string VmfExtension::Version() const {
#ifdef EXT_VERSION_VMF
	return EXT_VERSION_VMF;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void vmf_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::VmfExtension>();
}

DUCKDB_EXTENSION_API const char *vmf_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif