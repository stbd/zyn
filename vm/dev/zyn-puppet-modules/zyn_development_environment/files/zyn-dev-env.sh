echo -e "
\tZYN - Development environment

Zyn project is mounted to $HOME/zyn

To have sudo access to VM, switch to \"vagrant\" user
su vagrant  # password: vagrant

Available commands:
zyn-build  # Builds server
zyn-unittests  # Builds and runs unittest
zyn-system-tests  # Runs system tests (requires server to be build)
zyn-static-analysis  # Runs static analysis for all files in project

Other commands:
zyn-run-cli-client
zyn-run-web-client
zyn-run-server
"

function zyn-build() {
    project_path=$HOME/zyn/zyn
    pushd $project_path &> /dev/null
    r=0
    cargo build $@ || r=1
    popd &> /dev/null
    return "$r"
}

function zyn-unittests() {
    project_path=$HOME/zyn/zyn
    pushd $project_path &> /dev/null
    r=0
    cargo test $@ || r=1
    popd &> /dev/null
    return "$r"
}

function zyn-system-tests() {
    project_path=$HOME/zyn/tests/zyn_util/tests
    pushd $project_path &> /dev/null
    default_arguments="--nologcapture --nocapture"
    r=0
    nosetests test_basic_cases.py $default_arguments $@ || r=1
    popd &> /dev/null
    return "$r"
}

function zyn-static-analysis() {
    project_path=$HOME/zyn/tests/zyn_util/
    pushd $project_path &> /dev/null
    r=0
    flake8 || r=1
    popd &> /dev/null
    return "$r"
}

# todo: zyn-all-tests

function zyn-run-cli-client() {
    python3 $HOME/zyn/tests/zyn_util/cli-client.py \
            admin \
            127.0.0.1 \
            4433 \
            $HOME/.zyn-certificates/key.pem \
            $HOME/.zyn-certificates/cert.pem \
            -p admin \
            $@
}

function zyn-run-web-client() {
    python3 $HOME/zyn/tests/zyn_util/web-client.py \
            8080 \
            127.0.0.1 \
            4433 \
            $HOME/.zyn-certificates/key.pem \
            $HOME/.zyn-certificates/cert.pem \
            $@
}

function zyn-run-server() {
    RUST_LOG=trace $HOME/zyn/zyn/target/debug/zyn \
            --local-port 4433 \
            --local-address 127.0.0.1 \
            --default-user-name admin \
            --default-user-password admin \
            --path-cert $HOME/.zyn-certificates/cert.pem \
            --path-key $HOME/.zyn-certificates/key.pem \
            --gpg-fingerprint $(< $HOME/.zyn-test-user-gpg-fingerprint) \
            $@
}
