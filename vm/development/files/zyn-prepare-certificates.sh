#!/bin/bash
set -euo pipefail

function create_certificate() {
    path_cert=/etc/ssl/certs/zyn-test.pem
    path_key=/etc/ssl/private/zyn-test.key

    if [ -f "$path_cert" ]; then
        rm "$path_cert"
    fi
    if [ -f "$path_key" ]; then
        rm "$path_key"
    fi

    echo "Generating Zyn test certificates"
    echo "Cert: $path_cert"
    echo "Key: $path_key"

    path_tmp_file="$(mktemp)"
    cat <<EOF > "$path_tmp_file"
RANDFILE                = /dev/urandom

[ req ]
default_bits            = 2048
default_keyfile         = privkey.pem
distinguished_name      = req_distinguished_name
prompt                  = no
policy                  = policy_anything
req_extensions          = v3_req
x509_extensions         = v3_req

[ req_distinguished_name ]
commonName              = zyn

[ v3_req ]
basicConstraints        = CA:FALSE
EOF

    openssl req \
            -config "$path_tmp_file" \
            -new \
            -x509 \
            -days 3650 \
            -nodes \
            -sha256 \
            -out  "$path_cert" \
            -keyout "$path_key"

    rm "$path_tmp_file"

    chown root:ssl-cert "$path_key"
    cert_filename="$(basename "$path_cert")"
    pushd /etc/ssl/certs
    ln -sf "$cert_filename" "$(openssl x509 -hash -noout -in "$cert_filename")"
    popd

    # openssl x509 -text -noout -in test-cert.pem
    # openssl rsa -check -in privateKey.key
}

create_certificate
