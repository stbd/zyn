from debian:jessie as build_libressl

workdir /workdir

run apt-get update && apt-get install \
       wget \
       build-essential \
       -y

run /bin/bash -euc " \
libressl_version=2.4.3; \
wget https://ftp.openbsd.org/pub/OpenBSD/LibreSSL/libressl-\${libressl_version}.tar.gz && \
tar xf libressl-\${libressl_version}.tar.gz && \
pushd libressl-\${libressl_version} && \
./configure --prefix=/usr/libressl && \
make check install && \
popd \
"

from debian:jessie as build_zyn

workdir /workdir

run apt-get update && apt-get install \
       wget \
       libgpgme11-dev \
       build-essential \
       -y

run /bin/bash -euc " \
package_dir=rust-1.18.0-x86_64-unknown-linux-gnu; \
package_file=\${package_dir}.tar.gz; \
url=https://static.rust-lang.org/dist/\$package_file; \
wget \${url} && \
tar -xf \${package_file} && \
./\${package_dir}/install.sh \
"

copy --from=build_libressl /usr/libressl /usr
copy zyn-src.tar.gz /workdir/zyn-src.tar.gz

run /bin/bash -euc " \
pushd /workdir && \
tar -xf zyn-src.tar.gz && \
pushd zyn && \
cargo build && \
popd && \
popd \
"

run cp /workdir/zyn/target/debug/zyn /zyn

from debian:jessie

copy --from=build_libressl /usr/libressl /usr
copy --from=build_zyn /zyn /zyn

run apt-get update \
    && apt-get install \
       libgpgme11-dev \
       expect \
       -y

run mkdir /zyn-data

volume /zyn-data
volume /zyn-configuration
copy entrypoint.sh /entrypoint.sh

cmd ["/entrypoint.sh"]
