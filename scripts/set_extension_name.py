#!/usr/bin/python3

import sys, os, shutil
from pathlib import Path

shutil.copyfile(f'docs/NEXT_README.md', f'README.md')

if (len(sys.argv) != 3):
    raise Exception('usage: python3 set_extension_name.py <name_for_extension> <name_for_function>')

name_extension = sys.argv[1]
name_function = sys.argv[2]

if ('_' in name_extension or name_extension[0].islower()):
    raise Exception('Currently only extension names in CamelCase are allowed')

def replace(file_name, to_find, to_replace):
    with open(file_name, 'r', encoding="utf8") as file :
        filedata = file.read()
    filedata = filedata.replace(to_find, to_replace)
    with open(file_name, 'w', encoding="utf8") as file:
        file.write(filedata)

files_to_search = []
files_to_search.extend(Path('./.github').rglob('./**/*.yml'))
files_to_search.extend(Path('./test').rglob('./**/*.py'))
files_to_search.extend(Path('./test').rglob('./**/*.test'))
files_to_search.extend(Path('./test').rglob('./**/*.js'))
files_to_search.extend(Path('./src').rglob('./**/*.hpp'))
files_to_search.extend(Path('./src').rglob('./**/*.cpp'))
files_to_search.extend(Path('./src').rglob('./**/*.txt'))
files_to_search.extend(Path('./src').rglob('./*.md'))

def replace_everywhere(to_find, to_replace):
    for path in files_to_search:
        replace(path, to_find, to_replace)
        replace(path, to_find.capitalize(), to_replace.capitalize())
    
    replace("./CMakeLists.txt", to_find, to_replace)
    replace("./Makefile", to_find, to_replace)
    replace("./Makefile", to_find.capitalize(), to_replace.capitalize())
    replace("./Makefile", to_find.upper(), to_replace.upper())
    replace("./README.md", to_find, to_replace)

replace_everywhere("quack", name_function)
replace_everywhere("<extension_name>", name_extension)

string_to_replace = name_function
string_to_find = "quack"

# rename files
os.rename(f'test/python/{string_to_find}_test.py', f'test/python/{string_to_replace}_test.py')
os.rename(f'test/sql/{string_to_find}.test', f'test/sql/{string_to_replace}.test')
os.rename(f'src/{string_to_find}_extension.cpp', f'src/{string_to_replace}_extension.cpp')
os.rename(f'src/include/{string_to_find}_extension.hpp', f'src/include/{string_to_replace}_extension.hpp')
os.rename(f'test/nodejs/{string_to_find}_test.js', f'test/nodejs/{string_to_replace}_test.js')

# remove template-specific files
os.remove('.github/workflows/ExtensionTemplate.yml')