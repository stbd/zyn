FROM debian:jessie

run apt-get update \
    && apt-get install \
       libgpgme11-dev \
       wget \
       build-essential \
       expect \
       -y


run mkdir /zyn-data
copy install-libressl-debian.sh /install-libressl-debian.sh
run /install-libressl-debian.sh

volume /zyn-data
volume /zyn-configuration
copy entrypoint.sh /entrypoint.sh
copy zyn /zyn


cmd ["/entrypoint.sh"]