#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

function usage() {
    echo "Usage: <cmd> <params>"
    echo
    echo "where <cmd> is one of"
    echo "* server"
    echo "* docker-server"
    echo "* docker-web-client"
}

if [ $# -lt 1 ]; then
    usage
    exit 1
fi

cmd=$1
shift 1

if [ "$cmd" == "server" ]; then

    echo "Building server"
    result=0
    pushd "$(zyn_project_root)/zyn" >/dev/null 2>&1
    cargo build "$@" || result=1
    popd >/dev/null 2>&1
    exit "$result"

elif [ "$cmd" == "docker-server" ]; then

    if [ $# -ne 1 ]; then
        echo "Usage: <docker-tag-name>"
        exit
    fi

    docker build -t "$1" -f "$(zyn_project_root)/docker/dockerfile-zyn" "$(zyn_project_root)"

elif [ "$cmd" == "docker-web-client" ]; then

    if [ $# -ne 1 ]; then
        echo "Usage: <docker-tag-name>"
        exit
    fi

    docker build -t "$1" -f "$(zyn_project_root)/docker/dockerfile-web-client" "$(zyn_project_root)"

else
    echo "ERROR: Unknown command \"$cmd\""
    echo
    echo
    usage
fi
