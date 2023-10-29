#!/usr/bin/env bash

sourced=0
if [ "${BASH_SOURCE[0]}" != "${0}" ]; then
    sourced=1
fi

if [ "$sourced" -ne 1 ]; then
    echo "This script is meant to be sourced, not executed"
    exit 1
fi

path_test_user_gpg_files=/home/vagrant
zyn_project_root=/zyn
path_scripts=$zyn_project_root/vm/development/files
system_test_files=( \
    "test_basic_cases.py" \
    "test_client.py" \
    "test_edit_files.py" \
    "test_multiple_connections.py" \
    "test_util.py" \
)

function zyn-system-tests() {
    path_project=$zyn_project_root/tests/zyn_util/tests
    result=0
    pushd "$zyn_project_root/tests" &> /dev/null || exit 1
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
