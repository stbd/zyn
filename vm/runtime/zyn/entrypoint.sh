#!/usr/bin/env bash

set -euo pipefail

path_import_configuration=/zyn-configuration
gpg_agent_cache_expires=$((60 * 60 * 24 * 365 * 10))
path_password=$path_import_configuration/gpg-password
path_private_key=$path_import_configuration/gpg-private-key
path_fingerprint=$path_import_configuration/gpg-fingerprint
path_keygrip=$path_import_configuration/gpg-keygrip
path_pem_cert=$path_import_configuration/cert.pem
path_pem_key=$path_import_configuration/key.pem

echo "Starting GPG agent"

# Call GPG with some command to make sure .gnupg is generated to user home
gpg --list-keys

cat <<EOF > "$HOME/.gnupg/gpg.conf"
use-agent
EOF

cat <<EOF > "$HOME/.gnupg/gpg-agent.conf"
default-cache-ttl $gpg_agent_cache_expires
max-cache-ttl $gpg_agent_cache_expires
allow-preset-passphrase
EOF

gpg-connect-agent 'RELOADAGENT' /bye

echo "Importing private key"

< "$path_password" base64 -d | \
    gpg \
        --batch \
        --passphrase-fd 0 \
        --import "$path_private_key" \
    &> /dev/null

# Geting the key id is probably not the safest
expect -c "spawn gpg --edit-key \
       $(< $path_fingerprint base64 -d | rev | cut -c 1-16 | rev) trust quit; \
       send \"5\\ry\\r\"; \
       expect eof" \
    &> /dev/null

/usr/lib/gnupg2/gpg-preset-passphrase \
    --preset \
    --passphrase "$(< $path_password base64 -d)" \
    -v \
    "$(< $path_keygrip base64 -d)" \
    &> /dev/null

# Password is not needed anymore, write garbage over it
head -c 500 /dev/urandom | base64 > $path_password

echo "Starting Zyn server"

exec /zyn \
    --path-data-dir /zyn-data \
    --gpg-fingerprint "$(< $path_fingerprint base64 -d)" \
    --path-cert "$path_pem_cert" \
    --path-key "$path_pem_key" \
    "$@"
