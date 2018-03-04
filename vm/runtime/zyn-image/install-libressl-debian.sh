#!/bin/bash
set -euo pipefail

version=2.4.3
workdir="$(mktemp -d)"

pushd "$workdir"
wget https://ftp.openbsd.org/pub/OpenBSD/LibreSSL/libressl-"$version".tar.gz
tar xf libressl-"$version".tar.gz
pushd libressl-"$version"/
./configure --prefix=/usr/
make check

echo "Installing library (requires sudo)"
make install

popd
popd
