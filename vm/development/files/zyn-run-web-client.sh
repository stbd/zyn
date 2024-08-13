#!/usr/bin/env bash

set -euo pipefail

server_address=${ZYN_SERVER_ADDRESS:-ws://localhost:8080}

echo "Starting webclient using server address \"$server_address\""
echo "Customize server address with environment variable ZYN_SERVER_ADDRESS"

zyn-webserver \
    8081 \
    10.0.2.15 \
    8080 \
    --no-tls \
    --server-websocket-address "$server_address" \
    --debug-protocol \
    --debug-tornado \
    -vv \
    "$@"
