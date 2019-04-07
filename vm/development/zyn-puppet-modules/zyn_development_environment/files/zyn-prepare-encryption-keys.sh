#!/bin/bash
set -euo pipefail

function generate_gpg_keys() {

    exists=true
    gpg --fingerprint --fingerprint "$username" &> /dev/null || exists=false
    if ! "$exists" ; then
	echo "GPG Keys for user \"$username\" not found, creating key and subkey"

	# Generate non-expiring key, and a subkey that can be used for encryption
	key_settings=$(tempfile)
	cat <<EOF > "$key_settings"
Key-Type: RSA
KeY-Length: 2048
Key-Usage: auth
Subkey-Type: RSA
Subkey-Length: 2048
Name-Real: tester
Name-Comment: Key for testing
Name-Email: $username@invalid.com
Expire-Date: 0
Passphrase: $password
%commit
%echo Test key generated
EOF

	gpg --batch --gen-key "$key_settings"
	echo "Key generation done, killing stress process"
    else
	echo "GPG keys for test user \"$username\" already exists"
    fi

}

function install_gpg_fingerprint() {

    fingerprint_output="$(gpg --fingerprint --fingerprint "$username")"
    number_of_instances=$(echo "$fingerprint_output" | grep -wc "$username")
    if [ ! "$number_of_instances" -eq 1 ]; then
	echo "Error: Multiple test users found"
	exit 1
    fi

    # Seems to work, but could be written in Python for safer implementation
    fingerprint=$(echo "$fingerprint_output" | grep -A2 sub | grep fingerprint | tr -s ' ' | cut -d ' ' -f 5- | sed 's/ //g')
    echo "Installing gpg fingerprint to $path_gpg_fingerprint"
    echo "$fingerprint" > "$path_gpg_fingerprint"
}


function install_gpg_agent_start_command() {

    cat <<EOF > "$path_gpg_agent_start_cmd"
eval \$(gpg-agent \\
    --default-cache-ttl $gpg_agent_cache_expires \\
    --max-cache-ttl $gpg_agent_cache_expires  \\
    --allow-preset-passphrase \\
    --daemon \\
    --write-env-file $path_gpg_agent_env_settings)

/usr/lib/gnupg2/gpg-preset-passphrase --preset --passphrase pass \$(cat "$path_gpg_fingerprint")
EOF
    chmod 744 "$path_gpg_agent_start_cmd"
}

function install_gpg_agent_start_trigger() {
    path_file=$path_user_home/.bashrc
    echo "Updating GPG agent trigger in $path_file"
    sed -i "/$tag/,/$tag/d" "$path_file"

    cat <<EOF >> "$path_file"
# $tag
if [ -f "$path_gpg_agent_env_settings" ]; then
    source "$path_gpg_agent_env_settings"
    export GPG_AGENT_INFO
fi
zyn_gpg_agent_running=true
gpg-connect-agent /bye &> /dev/null || zyn_gpg_agent_running=false
if ! "\$zyn_gpg_agent_running" ; then
    "$path_gpg_agent_start_cmd"
    source "$path_gpg_agent_env_settings"
    export GPG_AGENT_INFO
fi
# /$tag
EOF

}

function update_gpg_conf() {
    echo "Updating GPG conf at $path_gpg_conf"
    sed -i "s/# use-agent/use-agent/g" "$path_gpg_conf"
}

function create_certificate() {
    path_key="$path_certificates_folder"/key.key
    path_cert="$path_certificates_folder"/cert.pem

    if [ ! -f "$path_key" ] || [ ! -f "$path_cert" ]; then
        echo "Creating certificates to $path_certificates_folder"
        mkdir  -p "$path_certificates_folder"
        openssl req \
                -newkey rsa:4096 \
                -nodes \
                -sha512 \
                -x509 \
                -days 3650 \
                -out "$path_certificates_folder"/cert.pem \
                -keyout "$path_certificates_folder"/key.pem \
                -subj "/C=gb/O=$hostname/CN=$hostname/emailAddress=tester@invalid.com"
    fi

}

if [ "$#" -ne 1 ]; then
    echo "Usage: [path-to-user-home]"
    exit 1
fi

path_user_home=$1
username=tester
password=pass
hostname=zyn
path_gpg_agent_env_settings=$path_user_home/.zyn-gpg-agent-env-settings
path_gpg_fingerprint=$path_user_home/.zyn-test-user-gpg-fingerprint
path_gpg_agent_start_cmd=$path_user_home/.zyn-gpg-agent-start-cmd
path_certificates_folder=$path_user_home/.zyn-certificates
path_gpg_conf=$path_user_home/.gnupg/gpg.conf
gpg_agent_cache_expires=$((60 * 60 * 24 * 365 * 10))
tag="ZYN-GPG-SETTINGS"

generate_gpg_keys
install_gpg_fingerprint
install_gpg_agent_start_command
install_gpg_agent_start_trigger
update_gpg_conf
create_certificate

# Cheat cheet
# http://irtfweb.ifa.hawaii.edu/~lockhart/gpg/

# To list keys
# gpg --list-keys
# gpg --fingerprint --fingerprint # Fingerprint twice to also print subkey fingerprints

# To delete keys
# gpg --batch --delete-secret-and-public-key "[fingeprint]"

# To encrypt and decrypt
# gpg --encrypt -r 'tester@invalid.com' data.txt
# gpg --decrypt  data.txt.gpg

# gpg-agent for caching passphrase
# gpg-agent --default-cache-ttl 100 --max-cache-ttl 100 --allow-preset-passphrase --daemon --write-env-file $HOME/gpg-agent-env-settings
# GPG_AGENT_INFO??p/gpg-RRKwTo/S.gpg-agent:9053:1; export GPG_AGENT_INFO;

# /usr/lib/gnupg2/gpg-preset-passphrase --preset --passphrase pass 5B4AFFE9E029E5C020B6D35EB7D333BC9571F0DF
# gpg-connect-agent
# GET_PASSPHRASE --no-ask 988CDD7E4D586CF95B6FC095CA88446874DA32CA Err Pmt Des

# To debug gpg use env variable
# GPGME_DEBUG=9:/home/user/mygpgme.log

# Exporting
# gpg --export-ownertrust > "$path_exported_key"
# gpg --export $user_email > "$path_public_key"
# gpg --export-secret-key $user_email > "$path_private_key"
