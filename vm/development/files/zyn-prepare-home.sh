#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: [path-user-home]"
    exit 1
fi

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
path_user_home=$1

tag=ZYN-DEV-ENV
sed -i "/$tag/,/$tag/d" "$path_user_home/.bashrc"
cat <<EOF >> "$path_user_home/.bashrc"
# $tag
echo -e "
\\tZyn - Development environment

Project repository is mounted to \"$zyn_project_root\"

There is folder for client and server datas under \"/data\"

Available commands:"

for script in "$path_scripts"/zyn-*; do
    echo -e "\\t\$(basename "\$script")"
done
# /$tag
EOF

echo "\"$path_user_home\" configured"
