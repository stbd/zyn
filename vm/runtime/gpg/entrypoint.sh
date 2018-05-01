#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: [path-exported-key]"
    exit
fi

path_exported_key=$1/exported-ownertrust-key.txt
path_cert_key=$1/key.pem
path_cert_pem=$1/cert.pem
path_public_key=$1/public.key
path_private_key=$1/private.key

user=tester
user_email=$user@invalid.com
password=password

key_settings=$(tempfile)
cat <<EOF > "$key_settings"
Key-Type: RSA
KeY-Length: 2048
Key-Usage: auth
Subkey-Type: RSA
Subkey-Length: 2048
Name-Real: $user
Name-Comment: Key for testing
Name-Email: $user_email
Expire-Date: 0
Passphrase: $password
%commit
%echo Test key generated
EOF

echo "Generating gpg key"
gpg --batch --gen-key "$key_settings"

echo "Exporting keys to $path_exported_key"
gpg --export-ownertrust > "$path_exported_key"
gpg --export $user_email > "$path_public_key"
gpg --export-secret-key $user_email > "$path_private_key"

echo "Generating certificates"
openssl req \
        -newkey rsa:4096 \
        -nodes \
        -sha512 \
        -x509 \
        -days 3650 \
        -out "$path_cert_pem" \
        -keyout "$path_cert_key" \
        -subj "/C=gb/O=zyn/CN=zyn/emailAddress=$user_email"
