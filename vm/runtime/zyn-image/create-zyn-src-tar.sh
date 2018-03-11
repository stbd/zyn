#!/bin/bash
set -euo pipefail

path_script="$(python -c "import os; print(os.path.realpath('$0'))")"
path_dir="$(dirname "$path_script")"
target=$path_dir/zyn-src.tar.gz

tar \
    czf "$target" \
    --exclude zyn/target \
    -C "$path_dir"/../../../  \
    zyn
