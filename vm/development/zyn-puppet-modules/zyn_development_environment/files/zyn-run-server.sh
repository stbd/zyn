#/usr/bin/env bash

set -euo pipefail

RUST_LOG=trace "$ZYN_ROOT"/zyn/target/debug/zyn \
        --local-port 4433 \
        --local-address 127.0.0.1 \
        --default-user-name admin \
        --default-user-password admin \
        --path-cert "$HOME"/.zyn-certificates/cert.pem \
        --path-key "$HOME"/.zyn-certificates/key.pem \
        --gpg-fingerprint "$(< "$HOME"/.zyn-test-user-gpg-fingerprint)" \
        "$@"
