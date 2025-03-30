#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"

function usage() {
    echo "Usage: <cmd> <params>"
    echo
    echo "where <cmd> is one of"
    echo "* python"
    echo "* bash"
}

if [ $# -lt 1 ]; then
    usage
    exit 1
fi

cmd=$1
shift 1

if [ "$cmd" == "python" ]; then

    result=0
    flake8 --config "$(zyn_project_root)/setup.cfg" "$(zyn_project_root)/py" || result=1
    flake8 --config "$(zyn_project_root)/setup.cfg" "$(zyn_project_root)/tests" || result=1
    exit "$result"

elif [ "$cmd" == "bash" ]; then

    result=0
    for file in $(git ls-files "*.sh"); do
        shellcheck "$file" || result=1
    done
    exit "$result"

else
    echo "ERROR: Unknown command \"$cmd\""
    echo
    echo
    usage
fi
