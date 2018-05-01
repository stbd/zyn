from debian:jessie

workdir /workdir

run apt-get update && apt-get install \
       build-essential \
       python-dev \
       python3 \
       python3-pip \
       -y

workdir /workdir

run touch /workdir/cert.pem
run touch /workdir/key.pem

copy zyn-web-src.tar.gz /workdir/
run /bin/bash -euc " \
pushd /workdir && \
tar -xf zyn-web-src.tar.gz && \
pip3 install -e . && \
popd \
"
entrypoint ["python3", "zyn_util/web-client.py"]
