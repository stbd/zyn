#!/usr/bin/env bash

set -euo pipefail

stress --cpu 2 --io 2 --vm 2 --hdd 2 &

echo "Generating gpg key"
# gpg --gen-key

# killall stress &> /dev/null

echo "Exporting keys"
#gpg --export-ownertrust > /gpg-keys/keys.txt

touch /gpg-keys/keys-.txt
touch /gpg-keys/qweqwe
echo $(ls /gpg-keys/)
