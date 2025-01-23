#!/usr/bin/env bash
set -euo pipefail

gpg_agent_cache_expires=$((60 * 60 * 24 * 365 * 10))
path_secrets=/run/secrets
path_secret_gpg_secret_key=$path_secrets/zyn_gpg_secret_key
path_secret_gpg_password=$path_secrets/zyn_gpg_password
path_secret_gpg_keygrip=$path_secrets/zyn_gpg_keygrip
path_secret_gpg_fingerprint=$path_secrets/zyn_gpg_fingerprint
path_data=/data

# Call GPG with some command to make sure .gnupg is generated to user home
gpg --list-keys >/dev/null 2>&1

cat <<EOF > "$HOME/.gnupg/gpg.conf"
use-agent
EOF

cat <<EOF > "$HOME/.gnupg/gpg-agent.conf"
default-cache-ttl $gpg_agent_cache_expires
max-cache-ttl $gpg_agent_cache_expires
allow-preset-passphrase
EOF

gpg-connect-agent 'RELOADAGENT' /bye

echo "GPG agent running, importing private key"
< "$path_secret_gpg_password" | \
    gpg \
        --batch \
        --passphrase-fd 0 \
        --import "$path_secret_gpg_secret_key" \
    &> /dev/null

echo "Trusting importing key"
expect -c "spawn gpg --edit-key \
       $(cat $path_secret_gpg_fingerprint | rev | cut -c 1-16 | rev) trust quit; \
       send \"5\\ry\\r\"; \
       expect eof" \
    &> /dev/null

echo "Presetting password for key"
/usr/libexec/gpg-preset-passphrase \
    --preset \
    --passphrase "$(cat $path_secret_gpg_password)" \
    -v \
    "$(< $path_secret_gpg_keygrip)" \
    &> /dev/null

echo "GPG configuration completed"

args=("--local-address" "0.0.0.0" "--local-port" "80")
args+=("--gpg-fingerprint" $(cat "$path_secret_gpg_fingerprint"))
args+=("--path-data-dir" "$path_data")

if [ -z "$(ls -A "$path_data")" ]; then
   echo "Data directory is empty, adding initialization flag"
   args+=("--init")
fi

if [ -n "${ZYN_DEFAULT_USERNAME:-""}" ]; then
    args+=("--default-user-name" "$ZYN_DEFAULT_USERNAME")
fi

if [ -n "${ZYN_DEFAULT_USER_PASSWORD:-""}" ]; then
    args+=("--default-user-password" "$ZYN_DEFAULT_USER_PASSWORD")
fi

if [ -n "${ZYN_MAX_NUMBER_OF_FILESYSTEM_ELEMENTS:-""}" ]; then
    args+=("--filesystem-capacity" "$ZYN_MAX_NUMBER_OF_FILESYSTEM_ELEMENTS")
fi

if [ -n "${ZYN_MAX_SIZE_BLOB_FILE:-""}" ]; then
    args+=("--max-page-size-for-blob" "$ZYN_MAX_SIZE_BLOB_FILE")
fi

if [ -n "${ZYN_MAX_SIZE_RANDON_ACCESS:-""}" ]; then
    args+=("--max-page-size-for-random-access" "$ZYN_MAX_SIZE_RANDON_ACCESS")
fi

if [ -n "${ZYN_MAX_INACTIVITY_SECONDS:-""}" ]; then
    args+=("--max-inactivity-duration-seconds" "$ZYN_MAX_INACTIVITY_SECONDS")
fi

if [ -n "${ZYN_TOKEN_DURATION_SECONDS:-""}" ]; then
    args+=("--authentication-token-duration" "$ZYN_TOKEN_DURATION_SECONDS")
fi

echo "Starting server"
exec /opt/zyn/zyn "${args[@]}"
