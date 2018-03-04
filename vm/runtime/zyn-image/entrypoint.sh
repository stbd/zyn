#!/usr/bin/env bash

set -euo pipefail

echo "Running"

echo $(ls /zyn-configuration)

path_import_configuration=/zyn-configuration
path_import_ownertrust="$path_import_configuration/exported-ownertrust-key.txt"
path_import_public_key="$path_import_configuration/public.key"
path_import_private_key="$path_import_configuration/private.key"
import_path_cert="$path_import_configuration/cert.pem"
import_path_key="$path_import_configuration/key.pem"
import_path_pgp_password="$path_import_configuration/gpg-password"
import_path_pgp_fingerprint="$path_import_configuration/gpg-fingerprint"
gpg_agent_cache_expires=$((60 * 60 * 24 * 365 * 10))

password=$(cat "$import_path_pgp_password" | base64 -d)
gpg_fingerprint=$(cat "$import_path_pgp_fingerprint" | base64 -d)
echo "Password: $password, fingerprint: $gpg_fingerprint"

echo "Starting GPG agent"
eval "$(gpg-agent \
           --homedir /root/.gnupg \
           --default-cache-ttl $gpg_agent_cache_expires \
           --max-cache-ttl $gpg_agent_cache_expires  \
           --allow-preset-passphrase \
           --write-env-file /zyn-gpg-agent-env-settings \
           --daemon  \
           )"
#-vv --debug-level 9 \


# exit 1
gpg-connect-agent /bye

echo "Importing key"
gpg --homedir /root/.gnupg --import "$path_import_public_key"
echo "$password" | gpg --homedir /root/.gnupg --batch --passphrase-fd 0 --import "$path_import_private_key"
gpg --homedir /root/.gnupg --import-ownertrust "$path_import_ownertrust"

#expect -c "spawn gpg --homedir /root/.gnupg --edit-key F9026003FE518DAC trust quit; send \"5\ry\r\"; expect eof"
expect -c "spawn gpg --homedir /root/.gnupg --edit-key CA88446874DA32CA trust quit; send \"5\ry\r\"; expect eof"
#gpg --homedir /root/.gnupg --edit-key F9026003FE518DAC trust quit

echo "Testing: fingerprints"
gpg --homedir /root/.gnupg --fingerprint --fingerprint
echo "Testing: lies"
gpg --homedir /root/.gnupg --list-keys

echo "use-agent" > /root/.gnupg/gpg.conf
# echo "info: $GPG_AGENT_INFO"
echo "--"
echo "$(ls /$HOME/.gnupg)"
#echo "$(find / -name gpg.conf)"
#sed -i "s/# use-agent/use-agent/g" "$HOME//.gnupg/gpg.conf"

# /usr/lib/gnupg2/gpg-preset-passphrase  -h

/usr/lib/gnupg2/gpg-preset-passphrase \
    --homedir /root/.gnupg \
    --preset \
    --passphrase "$password" \
    "$gpg_fingerprint"

# exit 1

path_runtime=/zyn-runtime
path_runtime_cert=$path_runtime/cert.pem
path_runtime_key=$path_runtime/key.pem
mkdir "$path_runtime"
cp "$import_path_cert" "$path_runtime_cert"
cp "$import_path_key" "$path_runtime_key"

# exit 1

#while :; do sleep 10; done;

/zyn \
    --init \
    --path-data-dir /zyn-data \
    --gpg-fingerprint "$gpg_fingerprint" \
    --path-cert "$path_runtime_cert" \
    --path-key "$path_runtime_key" \
