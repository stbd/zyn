#/usr/bin/env bash

if [ "$#" -ne 2 ]; then
    echo "Usage: [username] [path-user-home]"
    exit 1
fi

username=$1
path_user_home=$2

# Make sure home has all files from skeleton
for path in /etc/skel/.*; do
    path_in_home="$path_user_home/$(basename "$path")"
    if [ -f "$path" ] && [ ! -f "$path_in_home" ]; then
        cp "$path" "$path_in_home"
        chown "$username:$username" "$path_in_home"
    fi;
done
chown -R "$username:$username" "$path_user_home"

tag=ZYN-DEV-ENV
sed -i "/$tag/,/$tag/d" "$path_user_home/.bashrc"
cat <<EOF >> "$path_user_home/.bashrc"
# $tag
export ZYN_ROOT=$path_user_home/zyn
source "$path_user_home/.zyn-dev-env.sh"
# /$tag
EOF
