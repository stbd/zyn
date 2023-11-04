#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"
zyn-system-tests "$@"
