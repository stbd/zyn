#!/usr/bin/env bash

sourced=0
if [ "${BASH_SOURCE[0]}" != "${0}" ]; then
    sourced=1
fi

if [ "$sourced" -ne 1 ]; then
    echo "This script is meant to be sourced, not executed"
    exit 1
fi

path_scripts="$(dirname "${BASH_SOURCE[0]}")"

function zyn_project_root() {
    echo "${ZYN_PROJECT_ROOT:-"$path_scripts/../../../"}"
}

function zyn_test_data() {
    if [ -z "${ZYN_PROJECT_ROOT:-}" ]; then
        if [ -z "${ZYN_TEST_DATA:-}" ]; then
            echo "Zyn test data not found, please set ZYN_TEST_DATA"
            exit 1
        fi
        echo "$ZYN_TEST_DATA"
    else
        echo "/home/vagrant"
    fi
}
