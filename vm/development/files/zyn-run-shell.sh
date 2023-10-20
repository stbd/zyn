#!/usr/bin/env bash

set -euo pipefail

echo
echo "Pass \"init admin <path-data> 10.0.2.15 8080\" to initialise client"
echo
zyn-shell \
    -vv \
    --debug-protocol \
    --password admin \
    --no-tls \
    "$@"
