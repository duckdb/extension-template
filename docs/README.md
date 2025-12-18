# DuckDB Extension Template
This repository contains a template for creating a DuckDB extension. The main goal of this template is to allow users to easily develop, test and distribute their own DuckDB extension. The main branch of the template is always based on the latest stable DuckDB allowing you to try out your extension right away.

## Getting started
First step to getting started is to create your own repo from this template by clicking `Use this template`. Then clone your new repository using 
```sh
git clone --recurse-submodules https://github.com/<you>/<your-new-extension-repo>.git
```
Note that `--recurse-submodules` will ensure DuckDB is pulled which is required to build the extension.

## Building
### Managing dependencies
> [!IMPORTANT]  
> The example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
cd <your-working-dir-not-the-plugin-repo>
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg && git checkout ce613c41372b23b1f51333815feb3edd87ef8a8b
sh ./scripts/bootstrap.sh -disableMetrics
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

> [!NOTE]
> VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. 

### Updating Submodules
DuckDB extensions use two submodules that are included in your forked extension repo when you use the `--recurse-submodules` flag. These modules are:

| Name                  | Repository                                      | Description |
|-----------------------|-------------------------------------------------|-------------|
| duckdb                | https://github.com/duckdb/duckdb                | This repository contains core DuckDB code required for building extensions.            |
| extension-ci-tools    | https://github.com/duckdb/extension-ci-tools    | This repository contains reusable components for building, testing and deploying DuckDB extensions.            |


> [!IMPORTANT]  
> It is recommended that you update your submodules at least once every other major LTS release to avoid CI/CD pipeline build errors caused by remaining pinned to a stale commit of these submodules.

To update all submodules to the latest commit hash:
```bash
git submodule update --init --recursive
```

To update your submodules to a specific commit hash, for example to update duckdb to the hash `8e146474d7adb960c5a2941142fe4482cc7dfc08`:
```bash
cd duckdb 
git fetch --all
git checkout 8e146474d7adb960c5a2941142fe4482cc7dfc08   # or any tag/branch/commit hash
cd ..
git add duckdb
git commit -m "Pin DuckDB submodule to cc7dfc08"
git push HEAD:update-submodule-branch
```

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/<extension_name>/<extension_name>.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded. 
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `<extension_name>.duckdb_extension` is the loadable binary as it would be distributed.

### Tips for speedy builds
DuckDB extensions currently rely on DuckDB's build system to provide easy testing and distributing. This does however come at the downside of requiring the template to build DuckDB and its unittest binary every time you build your extension. To mitigate this, we highly recommend installing [ccache](https://ccache.dev/) and [ninja](https://ninja-build.org/). This will ensure you only need to build core DuckDB once and allows for rapid rebuilds.

To build using ninja and ccache ensure both are installed and run:

```sh
GEN=ninja make
```

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`. This shell will have the extension pre-loaded.  

Now we can use the features from the extension directly in DuckDB. The template contains a single scalar function `quack()` that takes a string arguments and returns a string:
```
D select quack('Jane') as result;
┌───────────────┐
│    result     │
│    varchar    │
├───────────────┤
│ Quack Jane 🐥 │
└───────────────┘
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

## Getting started with your own extension
After creating a repository from this template, the first step is to name your extension. To rename the extension, run:
```sh
# Note: This will rewrite this file!
python3 ./scripts/bootstrap-template.py <extension_name_you_want>
```
Feel free to delete the script after this step.

Now you're good to go! After a (re)build, you should now be able to use your duckdb extension:
```
./build/release/duckdb
D select <extension_name_you_chose>('Jane') as result;
┌─────────────────────────────────────┐
│                result               │
│               varchar               │
├─────────────────────────────────────┤
│ <extension_name_you_chose> Jane 🐥  │
└─────────────────────────────────────┘
```

For inspiration/examples on how to extend DuckDB in a more meaningful way, check out the [test extensions](https://github.com/duckdb/duckdb/blob/main/test/extension),
the [in-tree extensions](https://github.com/duckdb/duckdb/tree/main/extension), and the [out-of-tree extensions](https://github.com/duckdblabs).

## Distributing your extension
To distribute your extension binaries, there are a few options.

### Community extensions
The recommended way of distributing extensions is through the [community extensions repository](https://github.com/duckdb/community-extensions).
This repository is designed specifically for extensions that are built using this extension template, meaning that as long as your extension can be
built using the default CI in this template, submitting it to the community extensions is a very simple process. The process works similarly to popular
package managers like homebrew and vcpkg, where a PR containing a descriptor file is submitted to the package manager repository. After the CI in the 
community extensions repository completes, the extension can be installed and loaded in DuckDB with:
```SQL
INSTALL <my_extension> FROM community;
LOAD <my_extension>
```
For more information, see the [community extensions documentation](https://duckdb.org/community_extensions/documentation).

### Downloading artifacts from GitHub
The default CI in this template will automatically upload the binaries for every push to the main branch as GitHub Actions artifacts. These
can be downloaded manually and then loaded directly using:
```SQL
LOAD '/path/to/downloaded/extension.duckdb_extension';
```
Note that this will require starting DuckDB with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. For the CLI it is done like:
```shell
duckdb -unsigned
```

### Uploading to a custom repository
If for some reason distributing through community extensions is not an option, extensions can also be uploaded to a custom extension repository.
This will give some more control over where and how the extensions are distributed, but comes with the downside of requiring the `allow_unsigned_extensions`
option to be set. For examples of how to configure a manual GitHub Actions deploy pipeline, check out the extension deploy script in https://github.com/duckdb/extension-ci-tools.
Some examples of extensions that use this CI/CD workflow check out [spatial](https://github.com/duckdblabs/duckdb_spatial) or [aws](https://github.com/duckdb/duckdb_aws).

Extensions in custom repositories can be installed and loaded using:
```SQL
INSTALL <my_extension> FROM 'http://my-custom-repo'
LOAD <my_extension>
```

### Versioning of your extension
Extension binaries will only work for the specific DuckDB version they were built for. The version of DuckDB that is targeted 
is set to the latest stable release for the main branch of the template so initially that is all you need. As new releases 
of DuckDB are published however, the extension repository will need to be updated. The template comes with a workflow set-up
that will automatically build the binaries for all DuckDB target architectures that are available in the corresponding DuckDB
version. This workflow is found in `.github/workflows/MainDistributionPipeline.yml`. It is up to the extension developer to keep
this up to date with DuckDB. Note also that its possible to distribute binaries for multiple DuckDB versions in this workflow 
by simply duplicating the jobs.

## Setting up CLion 

### Opening project
Configuring CLion with the extension template requires a little work. Firstly, make sure that the DuckDB submodule is available. 
Then make sure to open `./duckdb/CMakeLists.txt` (so not the top level `CMakeLists.txt` file from this repo) as a project in CLion.
Now to fix your project path go to `tools->CMake->Change Project Root`([docs](https://www.jetbrains.com/help/clion/change-project-root-directory.html)) to set the project root to the root dir of this repo.

### Debugging
To set up debugging in CLion, there are two simple steps required. Firstly, in `CLion -> Settings / Preferences -> Build, Execution, Deploy -> CMake` you will need to add the desired builds (e.g. Debug, Release, RelDebug, etc). There's different ways to configure this, but the easiest is to leave all empty, except the `build path`, which needs to be set to `../build/{build type}`, and CMake Options to which the following flag should be added, with the path to the extension CMakeList:

```
-DDUCKDB_EXTENSION_CONFIGS=<path_to_the_exentension_CMakeLists.txt>
```

The second step is to configure the unittest runner as a run/debug configuration. To do this, go to `Run -> Edit Configurations` and click `+ -> Cmake Application`. The target and executable should be `unittest`. This will run all the DuckDB tests. To specify only running the extension specific tests, add `--test-dir ../../.. [sql]` to the `Program Arguments`. Note that it is recommended to use the `unittest` executable for testing/development within CLion. The actual DuckDB CLI currently does not reliably work as a run target in CLion.
