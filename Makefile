PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
CC = g++
CFLAGS = -Wall -g
# Configuration of extension
EXT_NAME=vmf
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile