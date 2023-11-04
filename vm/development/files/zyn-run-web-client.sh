#!/usr/bin/env bash

set -euo pipefail

zyn-webserver \
    8081 \
    10.0.2.15 \
    8080 \
    --no-tls \
    --server-websocket-address ws://localhost:8080 \
    --debug-protocol \
    --debug-tornado \
    -vv \
    "$@"
