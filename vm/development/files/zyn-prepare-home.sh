#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: [path-user-home]"
    exit 1
fi

path_user_home=$1
path_project_root=/zyn
path_files_source=$path_project_root/vm/development/files

tag=ZYN-DEV-ENV
sed -i "/$tag/,/$tag/d" "$path_user_home/.bashrc"
cat <<EOF >> "$path_user_home/.bashrc"
# $tag
echo -e "
\\tZyn - Development environment

Project repository is mounted to \$ZYN_ROOT

Available commands:"

for script in "$path_files_source"/zyn-*; do
    echo -e "\\t\$(basename "\$script")"
done
# /$tag
EOF

echo "Configured"
