#!/usr/bin/env bash

set -euo pipefail

zyn-web-server \
    8080 \
    10.0.2.15 \
    4433 \
    --server-websocket-address wss://localhost:4433 \
    --zyn-server-path-to-cert /etc/ssl/certs/zyn-test.pem \
    --remote-hostname zyn \
    --debug-protocol \
    --debug-tornado \
    -vv \
    "$@"
