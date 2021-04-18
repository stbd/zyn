#!/bin/bash
set -euo pipefail

curl https://sh.rustup.rs -sSf \
    | sh -s -- -y \
         --default-host x86_64-unknown-linux-gnu \
         --default-toolchain 1.47.0
