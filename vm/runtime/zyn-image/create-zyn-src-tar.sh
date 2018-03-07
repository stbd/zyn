#!/bin/bash
set -euo pipefail

path_script="$(pwd -P)"/$0
path_dir="$(dirname "$path_script")"

tar \
    czf "$path_dir/zyn-src.tar.gz" \
    --exclude zyn/target \
    -C "$path_dir"/../../../  \
    zyn
