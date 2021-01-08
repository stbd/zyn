#!/usr/bin/env bash

set -euo pipefail

path_project=$ZYN_ROOT/zyn
result=0
pushd "$path_project" &> /dev/null
cargo build "$@" || result=1
popd &> /dev/null
exit "$result"
