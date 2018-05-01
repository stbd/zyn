#!/bin/bash
set -euo pipefail

path_script="$(python -c "import os; print(os.path.realpath('$0'))")"
path_dir="$(dirname "$path_script")"
target=$path_dir/zyn-web-src.tar.gz

tar \
    czf "$target" \
    --exclude *pyc \
    --exclude *__pycache__* \
    --exclude *tests* \
    -C "$path_dir"/../../../tests \
    zyn_util setup.py
