# Config to build this extension
duckdb_extension_load(quack
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    INCLUDE_PATH ${CMAKE_CURRENT_LIST_DIR}src/include
    TEST_PATH ${CMAKE_CURRENT_LIST_DIR}src/include
    LOAD_TESTS
    DONT_LINK
)

# Load more extensions in your builds by adding them here. E.g.:
# duckdb_extension_load(json)