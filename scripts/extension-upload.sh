#!/bin/bash

# Extension upload script

# Usage: ./extension-upload.sh <name> <extension_version> <duckdb_version> <architecture> <s3_bucket> <copy_to_latest> <copy_to_versioned>
# <name>                : Name of the extension
# <extension_version>   : Version (commit / version tag) of the extension
# <duckdb_version>      : Version (commit / version tag) of DuckDB
# <architecture>        : Architecture target of the extension binary
# <s3_bucket>           : S3 bucket to upload to
# <copy_to_latest>      : Set this as the latest version ("true" / "false", default: "false")
# <copy_to_latest>      : Set this as a versioned version that will prevent its deletion

set -e

ext="/tmp/extension/$1.duckdb_extension"

script_dir="$(dirname "$(readlink -f "$0")")"

# calculate SHA256 hash of extension binary
cat $ext > $ext.append

# (Optionally) Sign binary
if [ "$DUCKDB_EXTENSION_SIGNING_PK" != "" ]; then
  echo "$DUCKDB_EXTENSION_SIGNING_PK" > private.pem
  $script_dir/../duckdb/scripts/compute-extension-hash.sh $ext.append > $ext.hash
  openssl pkeyutl -sign -in $ext.hash -inkey private.pem -pkeyopt digest:sha256 -out $ext.sign
  rm -f private.pem
fi

# Signature is always there, potentially defaulting to 256 zeros
truncate -s 256 $ext.sign

# append signature to extension binary
cat $ext.sign >> $ext.append

# compress extension binary
gzip < $ext.append > "$ext.gz"
set -e

# Abort if AWS key is not set
if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo "No AWS key found, skipping.."
    exit 0
fi

# upload versioned version
if [[ $7 = 'true' ]]; then
  aws s3 cp $ext.gz s3://$5/$1/$2/$3/$4/$1.duckdb_extension.gz --acl public-read
fi

# upload to latest version
if [[ $6 = 'true' ]]; then
  aws s3 cp $ext.gz s3://$5/$3/$4/$1.duckdb_extension.gz --acl public-read
fi
