#!/bin/bash
set -euo pipefail

source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"

function generate_gpg_keys() {

    exists=true
    gpg --fingerprint --fingerprint "$username" &> /dev/null || exists=false
    if ! "$exists" ; then
	echo "GPG Keys for user \"$username\" not found, creating key and subkey"

	# Generate non-expiring key, and a subkey that can be used for encryption
	key_settings=$(mktemp)
	cat <<EOF > "$key_settings"
Key-Type: RSA
KeY-Length: 2048
Key-Usage: auth
Subkey-Type: RSA
Subkey-Length: 2048
Name-Real: tester
Name-Comment: Key for testing
Name-Email: $email
Expire-Date: 0
Passphrase: $password
%commit
%echo Test key generated
EOF

	gpg --batch --gen-key "$key_settings"
	echo "Key generation done"
    else
	echo "GPG keys for test user \"$username\" already exists"
    fi
}

function install_gpg_development_environment() {

    fingerprint_output="$(gpg --fingerprint --fingerprint --with-keygrip "$username")"
    number_of_instances=$(echo "$fingerprint_output" | grep -wc "$username")
    if [ ! "$number_of_instances" -eq 1 ]; then
	echo "Error: Multiple test users found"
	exit 1
    fi

    # Seems to work, but could be written in Python for safer implementation
    fingerprint=$(echo "$fingerprint_output" | grep -A2 sub | sed -n '2p' | tr -s ' ' | sed 's/ //g')
    keygrip=$(echo "$fingerprint_output" | grep -A3 sub | sed -n '3p' | tr -s ' ' | sed 's/ Keygrip = //g')

    echo "Installing gpg fingerprint to \"$path_gpg_fingerprint\" and keygrip to \"$path_gpg_keygrip\""
    echo "$fingerprint" > "$path_gpg_fingerprint"
    echo "$keygrip" > "$path_gpg_keygrip"
    echo "$password" > "$path_gpg_password"

    echo "Installing secret key to $path_gpg_private_key"
    gpg --export-secret-key --pinentry-mode loopback --passphrase "$password" "$email" > "$path_gpg_private_key"
}

function configure_gpg() {

    cat <<EOF > "$path_user_home/.gnupg/gpg.conf"
use-agent
EOF
    cat <<EOF > "$path_user_home/.gnupg/gpg-agent.conf"
default-cache-ttl $gpg_agent_cache_expires
max-cache-ttl $gpg_agent_cache_expires
allow-preset-passphrase
EOF

    gpg-connect-agent 'RELOADAGENT' /bye

    cat <<EOF > "$path_gpg_login_trigger"
#!/usr/bin/env bash
set -euo pipefail
/usr/lib/gnupg2/gpg-preset-passphrase --preset --passphrase "$password" \$(cat "$path_gpg_keygrip")
EOF
    chmod 744 "$path_gpg_login_trigger"
    "$path_gpg_login_trigger"

    path_file=$path_user_home/.bashrc
    echo "Updating GPG agent trigger in $path_file"
    sed -i "/$tag/,/$tag/d" "$path_file"

    # The following adds gpg-preset-passphrase to be called when bashrc is executed.
    # That is, when user logs in or starts a new shell. It's probably not optimal
    # and gpg-preset-passphrase is called too often, but it does not seem to
    # cause any problems. Could not find a way to ask if password for a key is set,
    # or configure script to be run only once after boot for specific user.
    cat <<EOF >> "$path_file"
# $tag
"$path_gpg_login_trigger"
# /$tag
EOF

}

if [ $# -ne 1 ]; then
    echo "Usage: <path-test-folder>"
    exit 1
fi

path_user_home="$1"
username=tester
password=password
email=$username@invalid.com
path_gpg_fingerprint=$path_user_home/.zyn-test-user-gpg-fingerprint
path_gpg_keygrip=$path_user_home/.zyn-test-user-gpg-keygrip
path_gpg_password=$path_user_home/.zyn-test-user-gpg-password
path_gpg_private_key=$path_user_home/.zyn-test-user-gpg-secret-key
path_gpg_login_trigger=$path_user_home/.zyn-gpg-agent-start-cmd
gpg_agent_cache_expires=$((60 * 60 * 24 * 365 * 10))
tag="ZYN-GPG-SETTINGS"

generate_gpg_keys
install_gpg_development_environment
configure_gpg

# Cheat sheet
# http://irtfweb.ifa.hawaii.edu/~lockhart/gpg/

# To list keys
# gpg --list-keys

# Fingerprint twice to also print subkey fingerprints
# gpg --fingerprint --fingerprint --with-keygrip

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
# KEYINFO --list

# To debug gpg use env variable
# GPGME_DEBUG=9:/home/user/mygpgme.log

# Exporting
# gpg --export-ownertrust > "$path_exported_key"
# gpg --export $user_email > "$path_public_key"
# gpg --export-secret-key $user_email > "$path_private_key"

# systemctl restart --user gpg-agent
#  --supervised -vv --debug-level expert --log-file /home/vagrant/gpg.log
# pgconf --list-dirs
