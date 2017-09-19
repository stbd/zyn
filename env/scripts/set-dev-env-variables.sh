#!/bin/bash

if [ "$0" = "$BASH_SOURCE" ]; then
    echo "Please source this file instead of running it"
    exit 1
fi

path_project=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/../..

export PATH=$PATH:$path_project/bin/rust/bin
