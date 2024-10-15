#include "vmf_common.hpp"
#include "vmf_functions.hpp"
#include "vmf_scan.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

unique_ptr<FunctionData> ReadVMFObjectsBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<VMFScanData>();
	bind_data->Bind(context, input);

	bind_data->names.emplace_back("vmf");
	return_types.push_back(LogicalType::VMF());
	names.emplace_back("vmf");

	SimpleMultiFileList file_list(std::move(bind_data->files));
	MultiFileReader().BindOptions(bind_data->options.file_options, file_list, return_types, names,
	                              bind_data->reader_bind);
	bind_data->files = file_list.GetAllFiles();

	return std::move(bind_data);
}

static void ReadVMFObjectsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<VMFGlobalTableFunctionState>().state;
	auto &lstate = data_p.local_state->Cast<VMFLocalTableFunctionState>().state;

	// Fetch next lines
	const auto count = lstate.ReadNext(gstate);
	const auto units = lstate.units;
	const auto objects = lstate.values;

	if (!gstate.names.empty()) {
		// Create the strings without copying them
		const auto col_idx = gstate.column_indices[0];
		auto strings = FlatVector::GetData<string_t>(output.data[col_idx]);
		auto &validity = FlatVector::Validity(output.data[col_idx]);
		for (idx_t i = 0; i < count; i++) {
			if (objects[i]) {
				strings[i] = string_t(units[i].pointer, units[i].size);
			} else {
				validity.SetInvalid(i);
			}
		}
	}

	output.SetCardinality(count);

	if (output.size() != 0) {
		MultiFileReader().FinalizeChunk(context, gstate.bind_data.reader_bind, lstate.GetReaderData(), output, nullptr);
	}
}

TableFunction GetReadVMFObjectsTableFunction(bool list_parameter, shared_ptr<VMFScanInfo> function_info) {
	auto parameter = list_parameter ? LogicalType::LIST(LogicalType::VARCHAR) : LogicalType::VARCHAR;
	TableFunction table_function({parameter}, ReadVMFObjectsFunction, ReadVMFObjectsBind,
	                             VMFGlobalTableFunctionState::Init, VMFLocalTableFunctionState::Init);
	VMFScan::TableFunctionDefaults(table_function);
	table_function.function_info = std::move(function_info);

	return table_function;
}

TableFunctionSet VMFFunctions::GetReadVMFObjectsFunction() {
	TableFunctionSet function_set("read_vmf_objects");
	auto function_info =
	    make_shared_ptr<VMFScanInfo>(VMFScanType::READ_VMF_OBJECTS, VMFFormat::ARRAY, VMFRecordType::RECORDS);
	function_set.AddFunction(GetReadVMFObjectsTableFunction(false, function_info));
	function_set.AddFunction(GetReadVMFObjectsTableFunction(true, function_info));
	return function_set;
}

TableFunctionSet VMFFunctions::GetReadNDVMFObjectsFunction() {
	TableFunctionSet function_set("read_ndvmf_objects");
	auto function_info = make_shared_ptr<VMFScanInfo>(VMFScanType::READ_VMF_OBJECTS, VMFFormat::NEWLINE_DELIMITED,
	                                                   VMFRecordType::RECORDS);
	function_set.AddFunction(GetReadVMFObjectsTableFunction(false, function_info));
	function_set.AddFunction(GetReadVMFObjectsTableFunction(true, function_info));
	return function_set;
}

TableFunctionSet VMFFunctions::GetReadVMFObjectsAutoFunction() {
	TableFunctionSet function_set("read_vmf_objects_auto");
	auto function_info = make_shared_ptr<VMFScanInfo>(VMFScanType::READ_VMF_OBJECTS, VMFFormat::AUTO_DETECT,
	                                                   VMFRecordType::RECORDS);
	function_set.AddFunction(GetReadVMFObjectsTableFunction(false, function_info));
	function_set.AddFunction(GetReadVMFObjectsTableFunction(true, function_info));
	return function_set;
}

} // namespace duckdb
