#!/bin/bash

# Usage: ./extension-upload.sh <name> <extension_version> <duckdb_version> <architecture> <s3_bucket> <copy_to_latest>
# <name>                : Name of the extension
# <extension_version>   : Version (commit / version tag) of the extension
# <duckdb_version>      : Version (commit / version tag) of DuckDB
# <architecture>        : Architecture target of the extension binary
# <output_dir>          : Output directory to write files to
# <copy_to_latest>      : Set this as the latest version ("true" / "false", default: "false")

set -e

ext="/tmp/extension_download/$1.duckdb_extension"

# compress extension binary
gzip < "${ext}" > "$ext.gz"

# upload compressed extension binary to S3
cp $ext.gz $5/$1/$2/$3/$4/$1.duckdb_extension.gz

# upload to latest if copy_to_latest is set to true
if [[ $6 = 'true' ]]; then
  cp $ext.gz $5/$1/latest/$3/$4/$1.duckdb_extension.gz
fi