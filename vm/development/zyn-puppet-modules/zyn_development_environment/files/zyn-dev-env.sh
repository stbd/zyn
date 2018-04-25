echo -e "
\tZyn - Development environment

Project repository is mounted to $ZYN_ROOT
asd
Use user \"vagrant\" to have sudo access to the machine
su vagrant  # password: vagrant

Available commands:
zyn-build            # Build server
zyn-unittests        # Build and run unittest
zyn-system-tests     # Run system tests (requires server to be build)
zyn-static-analysis  # Run static analysis for all files in project

Other commands:
zyn-run-cli-client
zyn-run-web-client
zyn-run-server
"

function zyn-build() {
    path_project=$ZYN_ROOT/zyn
    result=0
    pushd "$path_project" &> /dev/null
    cargo build "$@" || result=1
    popd &> /dev/null
    return "$result"
}

function zyn-unittests() {
    path_project=$ZYN_ROOT/zyn
    result=0
    pushd "$path_project" &> /dev/null
    cargo test "$@" || result=1
    popd &> /dev/null
    return "$result"
}

system_test_files=( \
    "test_basic_cases.py" \
    "test_edit_files.py" \
)

function _zyn-system-tests() {
    path_project=$ZYN_ROOT/tests/zyn_util/tests
    result=0
    pushd "$path_project" &> /dev/null
    nosetests "${system_test_files[@]}" --nologcapture --nocapture -vv "$@" || result=1
    popd &> /dev/null
    return "$result"
}

function zyn-system-tests() {
    _zyn-system-tests -a '!slow' "$@"
    return "$?"
}

function zyn-static-analysis() {
    # todo: iterate over all files in repo and also check .sh files
    path_project=$HOME/zyn/tests/zyn_util/
    pushd "$path_project" &> /dev/null
    r=0
    flake8 || r=1
    popd &> /dev/null
    return "$r"
}

# todo: zyn-all-tests

function zyn-run-cli-client() {
    python3 "$ZYN_ROOT"/tests/zyn_util/cli_client.py \
            admin \
            127.0.0.1 \
            4433 \
            --path-to-cert "$HOME"/.zyn-certificates/cert.pem \
            -p admin \
            --debug-protocol \
            --remote-hostname zyn \
            "$@"
}

function zyn-run-web-client() {
    python3 "$ZYN_ROOT"/tests/zyn_util/web-client.py \
            8080 \
            127.0.0.1 \
            4433 \
            "$HOME"/.zyn-certificates/key.pem \
            "$HOME"/.zyn-certificates/cert.pem \
            --remote-hostname zyn \
            "$@"
}

function zyn-run-server() {
    RUST_LOG=trace "$ZYN_ROOT"/zyn/target/debug/zyn \
            --local-port 4433 \
            --local-address 127.0.0.1 \
            --default-user-name admin \
            --default-user-password admin \
            --path-cert "$HOME"/.zyn-certificates/cert.pem \
            --path-key "$HOME"/.zyn-certificates/key.pem \
            --gpg-fingerprint "$(< "$HOME"/.zyn-test-user-gpg-fingerprint)" \
            "$@"
}
