#!/usr/bin/env bash
set -euo pipefail

function usage() {
    echo "Usage: $(basename $0) <release-type> <version>"
    echo
    echo "where <release-type> is one of"
    echo "* py - create release of Python package"
}

if [ $# -ne 2 ]; then
    usage
    exit 1
fi

release_type=$1
version=$2

source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"

if [ "$release_type" == "py" ] ; then

    path_workdir="$(mktemp -d)"
    echo "Using workdir $path_workdir"
    ZYN_PY_VERSION=$version pip wheel --no-deps -w "$path_workdir" "$zyn_project_root/py"
    generated_file="$(find "$path_workdir" -name 'PyZyn*whl')"

    path_output="$PWD/$(basename $generated_file)"
    mv "$generated_file" "$path_output"
    rm -rf "$path_workdir"

    echo "PyZyn generated to \"$path_output\""
fi
