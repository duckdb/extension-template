PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXT_NAME=quack
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

include out-of-tree-extension.Makefile