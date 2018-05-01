FROM debian:latest

run apt-get update \
    && apt-get install \
       gpg \
       openssl \
       haveged \
       -y

run mkdir /gpg-keys
copy entrypoint.sh /entrypoint.sh

cmd ["/entrypoint.sh", "/gpg-keys"]