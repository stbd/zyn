#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
path_project=$zyn_project_root/zyn
result=0
pushd "$path_project" &> /dev/null
cargo test "$@" || result=1
popd &> /dev/null
exit "$result"
