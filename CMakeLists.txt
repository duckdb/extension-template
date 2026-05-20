cmake_minimum_required(VERSION 3.25)

set(EXT_NAME bigtable2)
set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(${EXT_NAME})
include_directories(src/include)

find_package(google_cloud_cpp_bigtable REQUIRED)

set(SOURCES src/bigtable2_extension.cpp src/product.cpp src/search.cpp src/utils.cpp)
build_static_extension(${EXT_NAME} ${SOURCES})
build_loadable_extension(${EXT_NAME} "" ${SOURCES})

# Link google-cloud-cpp in both the static library as the loadable extension
target_link_libraries(${EXT_NAME}_extension google-cloud-cpp::bigtable)
target_link_libraries(${EXT_NAME}_loadable_extension google-cloud-cpp::bigtable)

install(
  TARGETS ${EXT_NAME}_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
