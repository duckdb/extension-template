[macos]
deploy DUCKDB_VERSION:
    #!/usr/bin/env sh
    cd duckdb
    git checkout "tags/v{{DUCKDB_VERSION}}" || exit 1
    cd ..

    VCPKG_TOOLCHAIN_PATH=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make
    cat build/release/extension/bigtable2/bigtable2.duckdb_extension | gzip | gsutil cp - gs://di_duckdb_extension/v{{DUCKDB_VERSION}}/osx_arm64/bigtable2.duckdb_extension.gz

[linux]
deploy DUCKDB_VERSION:
    #!/usr/bin/env sh
    git reset --hard origin
    git pull
    git submodule update --init --recursive
    git fetch --tags --recurse-submodules

    cd duckdb
    git checkout "tags/v{{DUCKDB_VERSION}}" || exit 1
    cd ..

    docker build -f Dockerfile_linux_amd64_musl -t duckdb_extension_linux_amd64_musl .
    docker run -i -v /home/dataimpact/gs.json:/app/gs.json duckdb_extension_linux_amd64_musl bash <<EOF
        gcloud auth activate-service-account --key-file /app/gs.json
        gsutil cp bigtable2.duckdb_extension.gz gs://di_duckdb_extension/v{{DUCKDB_VERSION}}/linux_amd64_musl/bigtable2.duckdb_extension.gz
    EOF

    docker build -f Dockerfile_linux_amd64 -t duckdb_extension_linux_amd64 .
    docker run -i -v /home/dataimpact/gs.json:/app/gs.json duckdb_extension_linux_amd64 bash <<EOF
        gcloud auth activate-service-account --key-file /app/gs.json
        gsutil cp bigtable2.duckdb_extension.gz gs://di_duckdb_extension/v{{DUCKDB_VERSION}}/linux_amd64/bigtable2.duckdb_extension.gz
    EOF

debug:
    VCPKG_TOOLCHAIN_PATH=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make debug

release:
    VCPKG_TOOLCHAIN_PATH=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make

test: test_product test_search
    
test_product: debug
    ./build/debug/duckdb --init /dev/null -c "FROM product(2024_20, 2024_20, [1124000100000])"

test_search: debug
    ./build/debug/duckdb --init /dev/null -c "FROM search(2024_48, 2024_48, [130000]) ORDER BY keyword_id, shop_id, date, position"

bench: release
    ./build/release/duckdb --init /dev/null -c ".timer on" \
        -c "FROM product(2024_20, 2024_20, [1124000100000])" \
        -c "FROM search(2024_45, 2024_45, [98334])"

test_filter_pushdown: debug
    ./build/debug/duckdb --init /dev/null -c "SELECT pe_id, price FROM product(2024_20, 2024_20, [1124000100000])"
    ./build/debug/duckdb --init /dev/null -c "FROM product(2024_20, 2024_20, [1124000100000])"
    ./build/debug/duckdb --init /dev/null -c "SELECT pe_id FROM product(2024_20, 2024_20, [1124000100000])"

test_thread: debug
    ./build/debug/duckdb --init /dev/null -c "SET worker_threads TO 10; SET external_threads TO 10; SELECT pe_id, price FROM product(2024_20, 2024_20, [1124000100000, 1124000100001, 1124000100002, 1124000100003, 1124000100004, 1124000100005, 1124000100006, 1124000100007, 1124000100008, 1124000100009, 1124000100010, 1124000100011, 1124000100012, 1124000100013, 1124000100014, 1124000100015, 1124000100016, 1124000100017, 1124000100018, 1124000100019, 1124000100020, 1124000100021, 1124000100022, 1124000100023, 1124000100024, 1124000100025, 1124000100026, 1124000100027, 1124000100028, 1124000100029, 1124000100030, 1124000100031, 1124000100032, 1124000100033, 1124000100034, 1124000100035, 1124000100036, 1124000100037, 1124000100038, 1124000100039, 1124000100040, 1124000100041, 1124000100042, 1124000100043, 1124000100044, 1124000100045, 1124000100046, 1124000100047, 1124000100048, 1124000100049, 1124000100050, 1124000100051, 1124000100052, 1124000100053, 1124000100054, 1124000100055, 1124000100056, 1124000100057, 1124000100058, 1124000100059, 1124000100060, 1124000100061, 1124000100062, 1124000100063, 1124000100064, 1124000100065, 1124000100066, 1124000100067, 1124000100068, 1124000100069, 1124000100070, 1124000100071])"
    ./build/debug/duckdb --init /dev/null -c "SET worker_threads TO 10; SELECT current_setting('external_threads') AS threads"

test_so: debug
    DYLD_INSERT_LIBRARIES=/Library/Developer/CommandLineTools/usr/lib/clang/17/lib/darwin/libclang_rt.asan_osx_dynamic.dylib duckdb -unsigned -init /dev/null -c "INSTALL '{{justfile_directory()}}/build/debug/extension/bigtable2/bigtable2.duckdb_extension'; LOAD bigtable2; FROM product(2024_20, 2024_20, [1124000100000]) LIMIT 1;"
