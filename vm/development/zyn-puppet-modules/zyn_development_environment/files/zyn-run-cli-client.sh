#/usr/bin/env bash

set -euo pipefail

echo
echo "Pass \"--init admin <path-data> 127.0.0.1 4433\" to initialise client"
echo
zyn-cli \
    --path-to-cert /etc/ssl/certs/zyn-test.pem \
    -vv \
    --remote-hostname zyn \
    --debug-protocol \
    --password admin \
    "$@"
