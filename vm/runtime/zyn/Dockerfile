from debian:buster as build_libressl

workdir /workdir

run apt-get update && apt-get install \
       wget \
       build-essential \
       -y

run /bin/bash -euc " \
libressl_version=3.2.3; \
wget https://ftp.openbsd.org/pub/OpenBSD/LibreSSL/libressl-\${libressl_version}.tar.gz && \
tar xf libressl-\${libressl_version}.tar.gz && \
pushd libressl-\${libressl_version} && \
./configure --prefix=/usr/libressl && \
make check install && \
popd \
"

from debian:buster as build_zyn

workdir /workdir

run apt-get update && apt-get install \
       wget \
       curl \
       build-essential \
       -y

run /bin/bash -euc " \
curl https://sh.rustup.rs -sSf \
    | sh -s -- -y \
         --default-host x86_64-unknown-linux-gnu \
         --default-toolchain 1.47.0 \
"

copy --from=build_libressl /usr/libressl /usr
copy zyn-src.tar.gz /workdir/zyn-src.tar.gz

run /bin/bash -euc " \
source $HOME/.cargo/env && \
pushd /workdir && \
tar -xf zyn-src.tar.gz && \
pushd zyn && \
cargo build && \
popd && \
popd \
"

run cp /workdir/zyn/target/debug/zyn /zyn

from debian:buster

copy --from=build_libressl /usr/libressl /usr
copy --from=build_zyn /zyn /zyn

run apt-get update \
    && apt-get install \
       expect \
       gnupg2 \
       gnupg-agent \
       haveged \
       -y

run mkdir /zyn-data

# It looks like files that will mounted to container
# need to exist before mounting
run mkdir /zyn-configuration
run touch /zyn-configuration/gpg-private-key
run touch /zyn-configuration/gpg-fingerprint
run touch /zyn-configuration/gpg-password
run touch /zyn-configuration/cert.pem
run touch /zyn-configuration/key.pem

volume /zyn-data
copy entrypoint.sh /entrypoint.sh

entrypoint ["/entrypoint.sh"]
