echo -e "
\tZyn - Development environment

Project repository is mounted to $ZYN_ROOT

Use user \"vagrant\" to have sudo access to the machine
su vagrant  # password: vagrant

Available commands:
zyn-build            # Build server
zyn-unittests        # Build and run unittest
zyn-system-tests     # Run system tests (requires server to be build)
zyn-static-analysis  # Run static analysis for all files in project
zyn-all-tests        # Runs all tests and analysis, should be run before commit

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
    result=0
    path_project=$ZYN_ROOT
    pushd "$path_project" &> /dev/null
    for file in $(git ls-files); do

        if [[ "$file" = *".py" ]]; then
            flake8 "$file" || result=1
        elif [[ "$file" = *".sh" ]]; then
            shellcheck "$file" || result=1
        else
            # Other file types are currently ignored
            # echo "Unchecked file: $file"
            :
        fi
    done
    popd &> /dev/null
    return "$result"
}

function zyn-all-tests() {
    cmd_static_analysis=zyn-static-analysis
    cmd_build=zyn-build
    cmd_unittests=zyn-unittests
    cmd_system_tests=zyn-system-tests

    result=0

    "$cmd_build"
    result_build=$?

    if [ "$result_build" -ne 0 ]; then
        echo "Build failed, replicate the problem with:"
        echo "$cmd_build"
        return 1
    fi

    "$cmd_static_analysis"
    result_static_analysis=$?

    "$cmd_unittests"
    result_unittests=$?

    "$cmd_system_tests"
    result_system_tests=$?

    if [ "$result_static_analysis" -ne 0 ]; then
        echo "Code static analysis failed, replicate the problem with:"
        echo "$cmd_static_analysis"
        result=1
    fi

    if [ "$result_unittests" -ne 0 ]; then
        echo "Unittests failed, replicate the problem with:"
        echo "$cmd_unittests"
        result=1
    fi

    if [ "$result_system_tests" -ne 0 ]; then
        echo "System tests failed, replicate the problem with:"
        echo "$cmd_system_tests"
        result=1
    fi

    if [ "$result" -eq 0 ]; then
        echo
        echo "--------------------------------"
        echo "All tests completed successfully"
    fi
    return "$result"
}

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
