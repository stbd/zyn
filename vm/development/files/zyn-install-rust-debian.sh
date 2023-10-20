#!/bin/bash
set -euo pipefail

path_project_root="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/../../.."
rust_version="$(grep 'ZYN_RUST_VERSION=' "$path_project_root/docker/dockerfile-zyn" | cut -d '=' -f 2)"

echo "Installing Rust version \"$rust_version\""

curl https://sh.rustup.rs -sSf \
    | sh -s -- -y \
         --default-host x86_64-unknown-linux-gnu \
         --default-toolchain "$rust_version"
