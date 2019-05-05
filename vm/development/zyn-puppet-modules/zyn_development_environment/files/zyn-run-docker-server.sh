#!/usr/bin/env bash

set -euo pipefail

"$ZYN_ROOT"/vm/runtime/run-zyn-server \
           "$HOME"/.zyn-test-user-gpg-secret-key \
           "$(cat "$HOME/.zyn-test-user-gpg-fingerprint")" \
           "$HOME"/.zyn-certificates/cert.pem \
           "$HOME"/.zyn-certificates/key.pem \
           --init \
           --log-level trace \
           --bind-to "127.0.0.1:4433" \
           -- \
           --detach
