FROM debian:latest

run apt-get update \
    && apt-get install \
       gpg \
       stress \
       -y

run mkdir /gpg-keys
copy run.sh /run.sh

cmd ["/run.sh"]