#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: [path-folder-to-create-carticates]"
    exit 1
fi

path_mount=$1

if [ "${path_mount:0:1}" != "/" ]; then
    echo "Path to mounted folder must be absolute"
    exit 1
fi

path_script="$(python -c "import os; print(os.path.realpath('$0'))")"
path_folder="$(dirname $path_script)"
docker build -t test-creadentials "$path_folder"/gpg
docker run --rm -it -v "$path_mount:/gpg-keys" -v /dev/urandom:/dev/urandom test-creadentials:latest
