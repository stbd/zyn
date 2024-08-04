#!/usr/local/env bash
set -euo pipefail

curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash

export NVM_DIR="$HOME/.nvm"
source $HOME/.nvm/nvm.sh

nvm install 20.11.0
npm install -g esbuild standard mocha
