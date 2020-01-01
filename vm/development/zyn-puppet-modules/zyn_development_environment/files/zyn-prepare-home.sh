#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
    echo "Usage: [username] [path-user-home] [path-module-files]"
    exit 1
fi

username=$1
path_user_home=$2
path_scripts_source="$(realpath $3)"
path_scripts_dest=$path_user_home/.zyn-scripts

# Make sure home has all files from skeleton
for path in /etc/skel/.*; do
    path_in_home="$path_user_home/$(basename "$path")"
    if [ -f "$path" ] && [ ! -f "$path_in_home" ]; then
        cp "$path" "$path_in_home"
        chown "$username:$username" "$path_in_home"
    fi;
done

tag=ZYN-DEV-ENV
sed -i "/$tag/,/$tag/d" "$path_user_home/.bashrc"
cat <<EOF >> "$path_user_home/.bashrc"
# $tag
export ZYN_ROOT=$path_user_home/zyn
function zyn-reload-home() {
    $(realpath "$0") $username $path_user_home $path_scripts_source
}
PATH=\$PATH:$path_scripts_dest
echo -e "
\tZyn - Development environment

Project repository is mounted to \$ZYN_ROOT

Use user \"vagrant\" to have sudo access to the machine
su vagrant  # password: vagrant

Available commands:"

for script in "$path_scripts_dest"/zyn-*; do
    echo -e "\t\$(basename "\$script")"
done
# /$tag
EOF

declare -a scripts=(
    "zyn-build.sh"
    "zyn-unittests.sh"
    "zyn-system-tests.sh"
    "zyn-system-tests-slow.sh"
    "zyn-all-tests.sh"
    "zyn-run-cli-client.sh"
    "zyn-run-web-client.sh"
    "zyn-run-server.sh"
    "zyn-static-analysis.sh"
    "zyn-install-web-tools.sh"
    "zyn-run-docker-server.sh"
    "common.sh"
)

mkdir -p "$path_scripts_dest"
for script in "${scripts[@]}"; do
    cp "$path_scripts_source/$script" "$path_scripts_dest"
done

chown -R "$username:$username" "$path_user_home"
