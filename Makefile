PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Use ninja for faster builds
GEN ?= ninja

# Configuration of extension
EXT_NAME=duck_geoarrow
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile