#/usr/bin/env bash

set -euo pipefail

path_project=$ZYN_ROOT/zyn
result=0
pushd "$path_project" &> /dev/null
cargo test "$@" || result=1
popd &> /dev/null
exit "$result"
