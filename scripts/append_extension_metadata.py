import argparse
import shutil

def padded_byte_string(input):
    encoded_string = input.encode('ascii')
    encoded_string += b'\x00' * (32 - len(encoded_string))
    return encoded_string

def main():
    arg_parser = argparse.ArgumentParser(description='Append extension metadata to loadable DuckDB extensions')

    arg_parser.add_argument('-l','--library-file', type=str, help='Path to the raw shared library', required=True)
    arg_parser.add_argument('-n', '--extension-name', type=str, help='Extension name to use', required=True)

    arg_parser.add_argument('-o', '--out-file', type=str, help='Explicit path for the output file', default='')

    arg_parser.add_argument('-p', '--duckdb-platform', type=str, help='The DuckDB platform to encode', required=True)
    arg_parser.add_argument('-dv', '--duckdb-version', type=str, help='The DuckDB version to encode, depending on the ABI type '
                                                               'this encodes the duckdb version or the C API version', required=True)
    arg_parser.add_argument('-ev', '--extension-version', type=str, help='The Extension version to encode', required=True)
    arg_parser.add_argument('--abi-type', type=str, help='The ABI type to encode, set to C_STRUCT by default', default='C_STRUCT')

    args = arg_parser.parse_args()

    OUTPUT_FILE = args.out_file if args.out_file else args.extension_name + '.duckdb_extension'
    OUTPUT_FILE_TMP = OUTPUT_FILE + ".tmp"

    print("Creating extension binary:")

    # Start with copying the library to a tmp file
    print(f" - Input file: {args.library_file}")
    print(f" - Output file: {OUTPUT_FILE}")
    shutil.copyfile(args.library_file, OUTPUT_FILE_TMP)

    # Then append the metadata to the tmp file
    print(f" - Metadata:")
    with open(OUTPUT_FILE_TMP, 'ab') as file:
        print(f"   - FIELD8 (unused)            = EMPTY")
        file.write(padded_byte_string(""))
        print(f"   - FIELD7 (unused)            = EMPTY")
        file.write(padded_byte_string(""))
        print(f"   - FIELD6 (unused)            = EMPTY")
        file.write(padded_byte_string(""))
        print(f"   - FIELD5 (abi_type)          = {args.abi_type}")
        file.write(padded_byte_string(args.abi_type))
        print(f"   - FIELD4 (extension_version) = {args.extension_version}")
        file.write(padded_byte_string(args.extension_version))
        print(f"   - FIELD3 (duckdb_version)    = {args.duckdb_version}")
        file.write(padded_byte_string(args.duckdb_version))
        print(f"   - FIELD2 (duckdb_platform)   = {args.duckdb_platform}")
        file.write(padded_byte_string(args.duckdb_platform))
        print(f"   - FIELD1 (header signature)  = 4 (special value to identify a duckdb extension)")
        file.write(padded_byte_string("4"))

        # Write some empty space for the signature
        file.write(b"\x00" * 256)


    # Finally we mv the tmp file to complete the process
    shutil.move(OUTPUT_FILE_TMP, OUTPUT_FILE)

if __name__ == '__main__':
    main()