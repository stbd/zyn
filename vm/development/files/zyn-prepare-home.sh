#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: [path-user-home]"
    exit 1
fi

path_user_home=$1
zyn_project_root=/zyn
path_scripts=$zyn_project_root/vm/development/files
path_cargo_build=/tmp/cargo-build

tag=ZYN-DEV-ENV
sed -i "/$tag/,/$tag/d" "$path_user_home/.bashrc"
cat <<EOF >> "$path_user_home/.bashrc"
# $tag
mkdir -p $path_cargo_build
export CARGO_TARGET_DIR=$path_cargo_build
export ZYN_PROJECT_ROOT=$zyn_project_root
export ZYN_TEST_DATA=$path_user_home
export ZYN_TESTS_PATH_SERVER_BINARY=$path_cargo_build/debug/zyn
echo -e "
\\tZyn - Development environment

Project repository is mounted to \"\$ZYN_PROJECT_ROOT\"

There is folder for client and server data under \"/data\"

Available commands:"

for script in "$path_scripts"/zyn-*; do
    echo -e "\\t\$(basename "\$script")"
done

echo
echo "Make sure to run \"npm install\" once under \"js\" to download web client dependencies"
echo

cd \$ZYN_PROJECT_ROOT
# /$tag
EOF

echo "\"$path_user_home\" configured"
