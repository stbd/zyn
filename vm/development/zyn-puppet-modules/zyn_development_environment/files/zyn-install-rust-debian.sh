#!/bin/bash
set -euo pipefail

# There seems to be issues with latest rust on virtual box
# https://github.com/rust-lang/rust/issues/49710
# For now, install 1.23.0
curl https://sh.rustup.rs -sSf \
    | sh -s -- -y \
         --default-host x86_64-unknown-linux-gnu \
         --default-toolchain 1.38.0
