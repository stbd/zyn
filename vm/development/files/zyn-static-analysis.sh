#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"

result=0
pushd "$zyn_project_root" &> /dev/null
for file in $(git ls-files); do

    if [[ "$file" = *".py" ]]; then
        flake8 --config "$zyn_project_root/setup.cfg" "$file" || result=1
    elif [[ "$file" = *".sh" ]]; then
        shellcheck "$file" || result=1
    else
        # Other file types are currently ignored
        # echo "Unchecked file: $file"
        :
    fi
done
popd &> /dev/null
exit "$result"
