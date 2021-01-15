#!/bin/bash
set -euo pipefail

version=3.2.3
workdir="$(mktemp -d)"

pushd "$workdir"
wget https://ftp.openbsd.org/pub/OpenBSD/LibreSSL/libressl-"$version".tar.gz
tar xf libressl-"$version".tar.gz
pushd libressl-"$version"/
./configure --prefix=/usr/
make check

echo "Installing library (requires sudo)"
sudo make install

popd
popd
