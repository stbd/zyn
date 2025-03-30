#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"

system_test_files=( \
    "test_basic_cases.py" \
    "test_client.py" \
    "test_edit_files.py" \
    "test_multiple_connections.py" \
    "test_util.py" \
)

function usage() {
    echo "Usage: <cmd> <params>"
    echo
    echo "where <cmd> is one of"
    echo "* server-unit-tests"
    echo "* server-system-tests"
    echo "* py-unit-tests"
    echo "* js-unit-tests"
}

function zyn_system_tests() {

    result=0
    pushd "$(zyn_project_root)/tests" >/dev/null 2>&1
    echo
    echo "Running Zyn system tests"
    echo
    echo "Add --log-level=debug -s --log-cli-level=debug to increase logging"
    echo "Add -k <filter> to specific test cases"
    echo "Add --collect-only to list cases without running them"
    echo
    pytest "${system_test_files[@]}" "$@"
    popd &> /dev/null || exit 1
    return "$result"
}


function py_unit_tests() {

    result=0
    pushd "$(zyn_project_root)/py/zyn" >/dev/null 2>&1
    pytest
    popd &> /dev/null || exit 1
    return "$result"
}

function js_unit_tests() {

    result=0
    pushd "$(zyn_project_root)/js" >/dev/null 2>&1
    npm test "$@"
    popd &> /dev/null || exit 1
    return "$result"
}

function zyn_unit_tests() {

    result=0
    pushd "$(zyn_project_root)/zyn" >/dev/null 2>&1
    cargo test || result=1
    popd >/dev/null 2>&1
    return "$result"
}

if [ $# -lt 1 ]; then
    usage
    exit 1
fi

cmd=$1
shift 1

if [ "$cmd" == "server-unit-tests" ]; then

    zyn_unit_tests "$@"

elif [ "$cmd" == "server-system-tests" ]; then

    zyn_system_tests "$@"

elif [ "$cmd" == "py-unit-tests" ]; then

    py_unit_tests "$@"

elif [ "$cmd" == "js-unit-tests" ]; then

    js_unit_tests "$@"

else
    echo "ERROR: Unknown command \"$cmd\""
    echo
    echo
    usage
fi
