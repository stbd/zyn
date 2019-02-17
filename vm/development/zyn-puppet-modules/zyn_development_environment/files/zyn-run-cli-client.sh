#/usr/bin/env bash

set -euo pipefail

echo
echo "Pass \"--init admin <path-data> 127.0.0.1 4433\" to initialise client"
echo
zyn-cli \
    --path-to-cert "$HOME"/.zyn-certificates/cert.pem \
    -vv \
    --remote-hostname zyn \
    --debug-protocol \
    "$@"
