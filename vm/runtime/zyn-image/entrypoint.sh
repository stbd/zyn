#!/usr/bin/env bash

set -euo pipefail

path_import_configuration=/zyn-configuration
gpg_agent_cache_expires=$((60 * 60 * 24 * 365 * 10))
path_password=$path_import_configuration/gpg-password
path_private_key=$path_import_configuration/gpg-private.key
path_fingerprint=$path_import_configuration/gpg-fingerprint
path_pem_cert=$path_import_configuration/cert.pem
path_pem_key=$path_import_configuration/key.pem

echo
echo "Starting GPG agent"
echo

eval "$(gpg-agent \
     --default-cache-ttl $gpg_agent_cache_expires \
     --max-cache-ttl $gpg_agent_cache_expires  \
     --allow-preset-passphrase \
     --write-env-file /zyn-gpg-agent-env-settings \
      --daemon  \
      )"

echo
echo "Importing private key"
echo

< "$path_password" base64 | \
    gpg \
        --batch \
        --passphrase-fd 0 \
        --import "$path_private_key"

# Geting the key id is probably not the safest
expect -c "spawn gpg --edit-key \
       $(< $path_fingerprint base64 -d | rev | cut -c 1-16 | rev) trust quit; \
       send \"5\\ry\\r\"; \
       expect eof"

echo "use-agent" > /root/.gnupg/gpg.conf

/usr/lib/gnupg2/gpg-preset-passphrase \
    --preset \
    --passphrase "$(< $path_password base64 -d)" \
    -v \
    "$(< $path_fingerprint base64 -d)"

/zyn \
    --init \
    --path-data-dir /zyn-data \
    --gpg-fingerprint "$(< $path_fingerprint base64 -d)" \
    --path-cert "$path_pem_cert" \
    --path-key "$path_pem_key" \
