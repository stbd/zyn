#!/bin/bash

set -euo pipefail

path_script="$(python -c "import os; print(os.path.realpath('$0'))")"
path_dir="$(dirname "$path_script")"
path_tool=$path_dir/create-error-codes.py
path_package="$(dirname "$path_dir")"

"$path_tool" python "$path_package/errors.py"
