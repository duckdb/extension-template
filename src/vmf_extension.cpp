#define DUCKDB_EXTENSION_MAIN
#include "vmf_extension.hpp"

#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "include/vmf_common.hpp"
#include "vmf_functions.hpp"

namespace duckdb {

static DefaultMacro vmf_macros[] = {
    {DEFAULT_SCHEMA, "vmf_group_array", {"x", nullptr}, {{nullptr, nullptr}}, "to_vmf(list(x))"},
    {DEFAULT_SCHEMA,
     "vmf_group_object",
     {"name", "value", nullptr},
     {{nullptr, nullptr}},
     "to_vmf(map(list(name), list(value)))"},
    {DEFAULT_SCHEMA,
     "vmf_group_structure",
     {"x", nullptr},
     {{nullptr, nullptr}},
     "vmf_structure(vmf_group_array(x))->0"},
    {DEFAULT_SCHEMA, "vmf", {"x", nullptr}, {{nullptr, nullptr}}, "vmf_extract(x, '$')"},
    {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}};

void VmfExtension::Load(DuckDB &db) {
	auto &db_instance = *db.instance;
	// VMF type
	auto vmf_type = LogicalType::VMF();
	ExtensionUtil::RegisterType(db_instance, LogicalType::VMF_TYPE_NAME, std::move(vmf_type));

    // VMF casts
	VMFFunctions::RegisterSimpleCastFunctions(DBConfig::GetConfig(db_instance).GetCastFunctions());
	VMFFunctions::RegisterVMFCreateCastFunctions(DBConfig::GetConfig(db_instance).GetCastFunctions());
	VMFFunctions::RegisterVMFTransformCastFunctions(DBConfig::GetConfig(db_instance).GetCastFunctions());

	// VMF scalar functions
	for (auto &fun : VMFFunctions::GetScalarFunctions()) {
		ExtensionUtil::RegisterFunction(db_instance, fun);
	}

	// VMF table functions
	for (auto &fun : VMFFunctions::GetTableFunctions()) {
		ExtensionUtil::RegisterFunction(db_instance, fun);
	}

	// VMF pragma functions
	for (auto &fun : VMFFunctions::GetPragmaFunctions()) {
		ExtensionUtil::RegisterFunction(db_instance, fun);
	}

	// VMF replacement scan
	auto &config = DBConfig::GetConfig(*db.instance);
	config.replacement_scans.emplace_back(VMFFunctions::ReadVMFReplacement);

	// VMF copy function
	auto copy_fun = VMFFunctions::GetVMFCopyFunction();
	ExtensionUtil::RegisterFunction(db_instance, std::move(copy_fun));

	// VMF macro's
	for (idx_t index = 0; vmf_macros[index].name != nullptr; index++) {
		auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(vmf_macros[index]);
		ExtensionUtil::RegisterFunction(db_instance, *info);
	}
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