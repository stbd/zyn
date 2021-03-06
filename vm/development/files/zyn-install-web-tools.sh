#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: [path-home]"
    exit 1
fi

path_user_home=$1
url=https://nodejs.org/dist/v15.9.0/node-v15.9.0-linux-x64.tar.xz
path_target=/opt/node
tag=ZYN-NODE-SETTINGS

if [ -d "$path_target" ]; then
    echo "Node already installed"
else
    workdir="$(mktemp -d)"
    echo "Using workdir: $workdir"
    pushd "$workdir" > /dev/null 2>&1
    wget "$url"

    filename="$(basename "$url")"
    tar -xf "$filename"
    if [ ! -f "$filename" ]; then
        echo "Failed to find unpacked tar content"
        exit 1
    fi
    # shellcheck disable=SC2010
    node_package="$(ls -1 | grep -v "$filename")"
    mkdir -p "$path_target"
    mv "$node_package" "$path_target"

    sed -i "/$tag/,/$tag/d" "$path_user_home/.bashrc"
    cat <<EOF >> "$path_user_home/.bashrc"
# $tag
PATH=\$PATH:$path_target/$node_package/bin
# / $tag
EOF

    popd > /dev/null 2>&1
    rm -rf "$workdir"
fi

yarn_installed=1
dpkg -l yarn || yarn_installed=0

if [ "$yarn_installed" -eq 0 ]; then

    curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | sudo apt-key add -
    echo "deb https://dl.yarnpkg.com/debian/ stable main" | sudo tee /etc/apt/sources.list.d/yarn.list
    sudo apt-get update && sudo apt-get install yarn -y

else
    echo "Yarn already installed"
fi

# npm install
# yarn install
# npm run build:node
