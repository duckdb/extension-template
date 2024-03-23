cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME quack)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/quack_extension.cpp)

function(build_loadable_extension_directory NAME OUTPUT_DIRECTORY PARAMETERS)
  set(TARGET_NAME ${NAME}_loadable_extension)
  # all parameters after output_directory
  set(FILES ${ARGV})
  # remove name
  list(REMOVE_AT FILES 0)
  # remove output_directory
  list(REMOVE_AT FILES 0)
  # remove parameters
  list(REMOVE_AT FILES 0)

  # parse parameters
  string(FIND "${PARAMETERS}" "-no-warnings" IGNORE_WARNINGS)

  add_library(${TARGET_NAME} SHARED ${FILES})
  # this disables the -Dsome_target_EXPORTS define being added by cmake which otherwise trips clang-tidy (yay)
  set_target_properties(${TARGET_NAME} PROPERTIES DEFINE_SYMBOL "")
  set_target_properties(${TARGET_NAME} PROPERTIES OUTPUT_NAME ${NAME})
  set_target_properties(${TARGET_NAME} PROPERTIES PREFIX "")
  if(${IGNORE_WARNINGS} GREATER -1)
    disable_target_warnings(${TARGET_NAME})
  endif()
  # loadable extension binaries can be built two ways:
  # 1. EXTENSION_STATIC_BUILD=1
  #    DuckDB is statically linked into each extension binary. This increases portability because in several situations
  #    DuckDB itself may have been loaded with RTLD_LOCAL. This is currently the main way we distribute the loadable
  #    extension binaries
  # 2. EXTENSION_STATIC_BUILD=0
  #    The DuckDB symbols required by the loadable extensions are left unresolved. This will reduce the size of the binaries
  #    and works well when running the DuckDB cli directly. For windows this uses delay loading. For MacOS and linux the
  #    dynamic loader will look up the missing symbols when the extension is dlopen-ed.
  if(WASM_LOADABLE_EXTENSIONS)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -sSIDE_MODULE=1 -DWASM_LOADABLE_EXTENSIONS")
  elseif (EXTENSION_STATIC_BUILD)
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR "${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
      if (APPLE)
        set_target_properties(${TARGET_NAME} PROPERTIES CXX_VISIBILITY_PRESET hidden)
        # Note that on MacOS we need to use the -exported_symbol whitelist feature due to a lack of -exclude-libs flag in mac's ld variant
        set(WHITELIST "-Wl,-exported_symbol,_${NAME}_init -Wl,-exported_symbol,_${NAME}_version -Wl,-exported_symbol,_${NAME}_storage_ini*")
        target_link_libraries(${TARGET_NAME} duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS} -Wl,-dead_strip ${WHITELIST})
      elseif (ZOS)
        target_link_libraries(${TARGET_NAME} duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS})
      else()
        # For GNU we rely on fvisibility=hidden to hide the extension symbols and use -exclude-libs to hide the duckdb symbols
        set_target_properties(${TARGET_NAME} PROPERTIES CXX_VISIBILITY_PRESET hidden)
        target_link_libraries(${TARGET_NAME} duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS} -Wl,--gc-sections -Wl,--exclude-libs,ALL)
      endif()
    elseif (WIN32)
      target_link_libraries(${TARGET_NAME} duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS})
    else()
      error("EXTENSION static build is only intended for Linux and Windows on MVSC")
    endif()
  else()
    if (WIN32)
      target_link_libraries(${TARGET_NAME} duckdb ${DUCKDB_EXTRA_LINK_FLAGS})
    elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang$")
      if (APPLE)
        set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
      endif()
    endif()
  endif()


  target_compile_definitions(${TARGET_NAME} PUBLIC -DDUCKDB_BUILD_LOADABLE_EXTENSION)
  set_target_properties(${TARGET_NAME} PROPERTIES SUFFIX
          ".duckdb_extension")

  if(MSVC)
    set_target_properties(
            ${TARGET_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_DEBUG
            "${CMAKE_BINARY_DIR}/${OUTPUT_DIRECTORY}")
    set_target_properties(
            ${TARGET_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_RELEASE
            "${CMAKE_BINARY_DIR}/${OUTPUT_DIRECTORY}")
  endif()
endfunction()

function(build_loadable_extension NAME PARAMETERS)
  # all parameters after name
  set(FILES ${ARGV})
  list(REMOVE_AT FILES 0)
  list(REMOVE_AT FILES 0)

  build_loadable_extension_directory(${NAME} "extension/${NAME}" "${PARAMETERS}" ${FILES})
endfunction()

function(build_static_extension NAME PARAMETERS)
  # all parameters after name
  set(FILES ${ARGV})
  list(REMOVE_AT FILES 0)
  add_library(${NAME}_extension STATIC ${FILES})
  target_link_libraries(${NAME}_extension duckdb_static)
endfunction()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
