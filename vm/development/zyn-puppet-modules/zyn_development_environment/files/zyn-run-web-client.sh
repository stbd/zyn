#/usr/bin/env bash

set -euo pipefail

zyn-web-server \
    8080 \
    127.0.0.1 \
    4433 \
    --zyn-server-path-to-cert /etc/ssl/certs/zyn-test.pem \
    --remote-hostname zyn \
    --debug-protocol \
    --debug-tornado \
    -vv \
    "$@"
