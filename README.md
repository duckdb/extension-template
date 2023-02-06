# WIP Disclaimer
This template is currently work-in-progress. Feel free to play around with it and give us feedback. Note also that this template depends on a development version of DuckDB. Follow https://duckdb.org/news for more information on official launch.

# DuckDB Extension Template
The main goal of this template is to allow users to easily develop, test and distribute their own DuckDB extension.

## Build
To build the extension:
```sh
make
```
**Note:** If you just cloned the repository, don't forget to run `git submodule init && git submodule update` to fetch the appropriate duckdb version.

The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/<extension_name>/<extension_name>.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded. 
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `<extension_name>.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. The template contains a single scalar function `do_a_boilerplate` that takes no arguments and returns a string:
```
D select do_a_boilerplate() as result;
┌────────────────────┐
│       result       │
│      varchar       │
├────────────────────┤
│ I'm a boilerplate! │
└────────────────────┘
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

## Getting started with your own extension
After creating a repository from this template, the first step is to name your extension. To rename the extension, run:
```
python3 ./scripts/set_extension_name.py <extension_name_you_want>
```
Feel free to delete the script after this step.

Now you're good to go! After a (re)build, you should now be able to use your duckdb extension:
```
./build/release/duckdb
D select do_a_<extension_name_you_chose>() as result;
┌───────────────────────────────────┐
│                result             │
│               varchar             │
├───────────────────────────────────┤
│ I'm a <extension_name_you_chose>! │
└───────────────────────────────────┘
```

For inspiration/examples on how to extend DuckDB in a more meaningful way, check out the in-tree [extensions](https://github.com/duckdb/duckdb/tree/master/extension) (or in your `duckdb` submodule) and the out-of-tree extensions in [duckdblabs](https://github.com/duckdblabs)! 

## Distributing your extension
Easy distribution of extensions built with this template is facilitated using a similar process used by DuckDB itself. Binaries are generated for various versions/platforms allowing duckdb to automatically install the correct binary.

This step requires that you pass the following 4 parameters to your github repo as action secrets:

| -secret name   | description                         |
| -------------- | ----------------------------------- |
| -S3_REGION     | s3 region holding your bucket       |
| -S3_BUCKET     | the name of the bucket to deploy to |
| -S3_DEPLOY_ID  | the S3 key id                       |
| -S3_DEPLOY_KEY | the S3 key secret                   |

After setting these variables, all pushes to master will trigger a new (dev) release.
