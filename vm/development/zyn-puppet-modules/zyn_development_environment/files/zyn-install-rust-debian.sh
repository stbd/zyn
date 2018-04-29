#!/bin/bash
set -euo pipefail

package_dir=rust-1.18.0-x86_64-unknown-linux-gnu
package_file=${package_dir}.tar.gz
url=https://static.rust-lang.org/dist/$package_file
workdir="$(mktemp -d)"

pushd "${workdir}"
wget "${url}"
tar -xf "${package_file}"
sudo "${package_dir}"/install.sh
popd
