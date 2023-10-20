#!/usr/bin/env bash

set -euo pipefail

RUST_LOG=trace /zyn/zyn/target/debug/zyn \
        --local-port 8080 \
        --local-address 10.0.2.15 \
        --default-user-name admin \
        --default-user-password admin \
        --gpg-fingerprint "$(< "$HOME"/.zyn-test-user-gpg-fingerprint)" \
        "$@"
