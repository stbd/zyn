#!/usr/bin/env bash
set -euo pipefail

path_project_root="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/../../.."

if [ $# -ne 1 ]; then
    echo "Usage: <docker-tag-name>"
    exit
fi

path_files=$HOME
docker run \
     -v "$path_files/.zyn-test-user-gpg-secret-key:/run/secrets/zyn_gpg_secret_key" \
     -v "$path_files/.zyn-test-user-gpg-password:/run/secrets/zyn_gpg_password" \
     -v "$path_files/.zyn-test-user-gpg-keygrip:/run/secrets/zyn_gpg_keygrip" \
     -v "$path_files/.zyn-test-user-gpg-fingerprint:/run/secrets/zyn_gpg_fingerprint" \
     -v "$path_project_root:/zyn" \
     -it \
     --env RUST_LOG=trace \
     -p 8085:80 \
     "$1"
     #--detach \
