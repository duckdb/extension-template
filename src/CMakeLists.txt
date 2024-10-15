cmake_minimum_required(VERSION 2.8.12...3.29)

project(VMFExtension)

include_directories(include)
add_subdirectory(vmf_functions)

set(VMF_EXTENSION_FILES
    buffered_vmf_reader.cpp
    vmf_extension.cpp
    vmf_common.cpp
    vmf_enums.cpp
    vmf_functions.cpp
    vmf_scan.cpp
    vmf_serializer.cpp
    vmf_deserializer.cpp
    serialize_vmf.cpp
    vmf_functions/copy_vmf.cpp
    vmf_functions/vmf_array_length.cpp
    vmf_functions/vmf_contains.cpp
    vmf_functions/vmf_exists.cpp
    vmf_functions/vmf_extract.cpp
    vmf_functions/vmf_keys.cpp
    vmf_functions/vmf_merge_patch.cpp
    vmf_functions/vmf_pretty.cpp
    vmf_functions/vmf_structure.cpp
    vmf_functions/vmf_transform.cpp
    vmf_functions/vmf_create.cpp
    vmf_functions/vmf_type.cpp
    vmf_functions/vmf_valid.cpp
    vmf_functions/vmf_value.cpp
    vmf_functions/vmf_serialize_sql.cpp
    vmf_functions/vmf_serialize_plan.cpp
    vmf_functions/read_vmf.cpp
    vmf_functions/read_vmf_objects.cpp)

build_static_extension(vmf ${VMF_EXTENSION_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(vmf ${PARAMETERS} ${VMF_EXTENSION_FILES})
target_link_libraries(vmf_loadable_extension duckdb_yyvmf)

install(
  TARGETS vmf_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
