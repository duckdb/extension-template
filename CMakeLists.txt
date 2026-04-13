cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME duck_geoarrow)

# Configure geoarrow-c
set(GEOARROW_VERSION_MAJOR 0)
set(GEOARROW_VERSION_MINOR 2)
set(GEOARROW_VERSION_PATCH 0)
set(GEOARROW_VERSION "0.2.0-SNAPSHOT")
set(GEOARROW_USE_FAST_FLOAT_DEFINE "#define GEOARROW_USE_FAST_FLOAT 0")
set(GEOARROW_USE_RYU_DEFINE "#define GEOARROW_USE_RYU 1")
set(GEOARROW_NAMESPACE_DEFINE "// #define GEOARROW_NAMESPACE YourNamespaceHere")
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/third-party/geoarrow-c/src/geoarrow/geoarrow_config.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/generated/geoarrow/geoarrow_config.h
)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

include_directories(src/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third-party/geoarrow-c/src)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third-party/geoarrow-c/src/vendor)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/generated)

set(GEOARROW_C_SOURCES
    third-party/geoarrow-c/src/geoarrow/schema.c
    third-party/geoarrow-c/src/geoarrow/schema_view.c
    third-party/geoarrow-c/src/geoarrow/metadata.c
    third-party/geoarrow-c/src/geoarrow/kernel.c
    third-party/geoarrow-c/src/geoarrow/builder.c
    third-party/geoarrow-c/src/geoarrow/array_view.c
    third-party/geoarrow-c/src/geoarrow/util.c
    third-party/geoarrow-c/src/geoarrow/visitor.c
    third-party/geoarrow-c/src/geoarrow/geometry.c
    third-party/geoarrow-c/src/geoarrow/native_writer.c
    third-party/geoarrow-c/src/geoarrow/scalar_udf.c
    third-party/geoarrow-c/src/geoarrow/wkb_reader.c
    third-party/geoarrow-c/src/geoarrow/wkb_writer.c
    third-party/geoarrow-c/src/geoarrow/wkt_reader.c
    third-party/geoarrow-c/src/geoarrow/wkt_writer.c
    third-party/geoarrow-c/src/geoarrow/array_reader.c
    third-party/geoarrow-c/src/geoarrow/array_writer.c
    third-party/geoarrow-c/src/geoarrow/double_parse_std.c
    third-party/geoarrow-c/src/geoarrow/double_print.c
    third-party/geoarrow-c/src/geoarrow/ryu/d2s.c
    third-party/geoarrow-c/src/vendor/nanoarrow/nanoarrow.c
)

set(EXTENSION_SOURCES src/duck_geoarrow_extension.cpp ${GEOARROW_C_SOURCES})

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
