#/usr/bin/env bash

set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
zyn-system-tests -a '!slow' "$@"
