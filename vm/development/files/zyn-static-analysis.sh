#!/usr/bin/env bash

set -euo pipefail

result=0
path_project=$ZYN_ROOT
pushd "$path_project" &> /dev/null
for file in $(git ls-files); do

    if [[ "$file" = *".py" ]]; then
        flake8 --config /zyn/tests/zyn_util/setup.cfg "$file" || result=1
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
