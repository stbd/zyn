#!/usr/bin/env bash

set -euo pipefail

# shellcheck disable=SC1090
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
zyn-system-tests -a '!slow' "$@"
