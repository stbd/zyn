FROM debian:latest

run apt-get update \
    && apt-get install \
       gpg \
       openssl \
       -y

run mkdir /gpg-keys
copy entrypoint.sh /entrypoint.sh

cmd ["/entrypoint.sh", "/gpg-keys"]