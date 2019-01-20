#/usr/bin/env bash

set -euo pipefail

zyn-cli \
    admin \
    127.0.0.1 \
    4433 \
    --path-to-cert "$HOME"/.zyn-certificates/cert.pem \
    -p admin \
    --debug-protocol \
    --remote-hostname zyn \
    -vv \
    "$@"
