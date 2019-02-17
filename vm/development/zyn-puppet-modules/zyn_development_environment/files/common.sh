#/usr/bin/env bash

sourced=0
if [ "${BASH_SOURCE[0]}" != "${0}" ]; then
    sourced=1
fi

if [ "$sourced" -ne 1 ]; then
    echo "This script is meant to be sourced, not executed"
    exit 1
fi

system_test_files=( \
    "test_basic_cases.py" \
    "test_edit_files.py" \
    "test_client.py" \
    "test_util.py" \
)

function zyn-system-tests() {
    path_project=$ZYN_ROOT/tests/zyn_util/tests
    result=0
    pushd "$path_project" &> /dev/null
    nosetests "${system_test_files[@]}" --nologcapture --nocapture -vv "$@" || result=1
    popd &> /dev/null
    return "$result"
}
