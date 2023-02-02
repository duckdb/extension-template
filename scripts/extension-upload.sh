#!/bin/bash

# Usage: ./extension-upload.sh <name> <Extension commithash or version_tag> <DuckDB commithash or version_tag> <architecture> <s3_bucket>

set -e

ext="build/release/extension/$1/$1.duckdb_extension"

# compress extension binary
gzip < $ext > "$1.duckdb_extension.gz"

# upload compressed extension binary to S3
aws s3 cp $1.duckdb_extension.gz s3://$5/$1/$2/$3/$4/$1.duckdb_extension.gz --acl public-read