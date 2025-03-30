#!/usr/bin/env bash
set -euo pipefail

curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash

export NVM_DIR="$HOME/.nvm"
source $HOME/.nvm/nvm.sh

nvm install --default v22.14.0
npm install -g esbuild standard mocha

touch "$HOME/.zyn-npm-installed"
