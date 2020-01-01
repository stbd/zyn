#!/usr/bin/env bash

set -euo pipefail

"$ZYN_ROOT"/vm/runtime/run-zyn-server \
           "$HOME"/.zyn-test-user-gpg-secret-key \
           "$(cat "$HOME/.zyn-test-user-gpg-fingerprint")" \
           /etc/ssl/certs/zyn-test.pem \
           /etc/ssl/private/zyn-test.key \
           --init \
           --log-level trace \
           --bind-to "127.0.0.1:4433" \
           -- \
           --detach
