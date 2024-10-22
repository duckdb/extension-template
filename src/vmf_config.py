import os

# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['src/include']]
# source files
source_files = [
    os.path.sep.join(x.split('/'))
    for x in [
        'extension/vmf/buffered_vmf_reader.cpp',
        'extension/vmf/vmf_enums.cpp',
        'extension/vmf/vmf_extension.cpp',
        'extension/vmf/vmf_common.cpp',
        'extension/vmf/vmf_functions.cpp',
        'extension/vmf/vmf_scan.cpp',
        'extension/vmf/vmf_functions/copy_vmf.cpp',
        'extension/vmf/vmf_functions/vmf_array_length.cpp',
        'extension/vmf/vmf_functions/vmf_contains.cpp',
        'extension/vmf/vmf_functions/vmf_exists.cpp',
        'extension/vmf/vmf_functions/vmf_extract.cpp',
        'extension/vmf/vmf_functions/vmf_keys.cpp',
        'extension/vmf/vmf_functions/vmf_merge_patch.cpp',
        'extension/vmf/vmf_functions/vmf_pretty.cpp',
        'extension/vmf/vmf_functions/vmf_structure.cpp',
        'extension/vmf/vmf_functions/vmf_transform.cpp',
        'extension/vmf/vmf_functions/vmf_create.cpp',
        'extension/vmf/vmf_functions/vmf_type.cpp',
        'extension/vmf/vmf_functions/vmf_valid.cpp',
        'extension/vmf/vmf_functions/vmf_value.cpp',
        'extension/vmf/vmf_functions/read_vmf_objects.cpp',
        'extension/vmf/vmf_functions/read_vmf.cpp',
        'extension/vmf/vmf_functions/vmf_serialize_plan.cpp',
        'extension/vmf/vmf_functions/vmf_serialize_sql.cpp',
        'extension/vmf/vmf_serializer.cpp',
        'extension/vmf/vmf_deserializer.cpp',
        'extension/vmf/serialize_vmf.cpp',
    ]
]
