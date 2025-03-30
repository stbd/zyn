#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"

function usage() {
    echo "Usage: <cmd> <params>"
    echo
    echo "where <cmd> is one of"
    echo "* server"
    echo "* docker-server"
    echo "* web-client"
    echo "* web-client-js - compile web-client js in watch mode"
    echo "* web-client-css - compile web-client css in watch mode"
    echo "* shell"
    echo "* docker-swarm"
}

if [ $# -lt 1 ]; then
    usage
    exit 1
fi

cmd=$1
shift 1

if [ "$cmd" == "server" ]; then

    path_server_data="${ZYN_DATA_PATH:-/data/server}"
    echo "Running server with data from \"$path_server_data\""

    RUST_LOG=trace /zyn/zyn/target/debug/zyn \
            --local-port 8080 \
            --local-address 10.0.2.15 \
            --default-user-name admin \
            --default-user-password admin \
            --gpg-fingerprint "$(< "$HOME"/.zyn-test-user-gpg-fingerprint)" \
            --path-data-dir "$path_server_data" \
            "$@"

elif [ "$cmd" == "docker-server" ]; then

    if [ $# -lt 1 ]; then
        echo "Usage: <docker-tag-name> <params>"
        exit
    fi

    path_test_files="$(zyn_test_data)"
    docker run \
           -v "$path_test_files/.zyn-test-user-gpg-secret-key:/run/secrets/zyn_gpg_secret_key" \
           -v "$path_test_files/.zyn-test-user-gpg-password:/run/secrets/zyn_gpg_password" \
           -v "$path_test_files/.zyn-test-user-gpg-keygrip:/run/secrets/zyn_gpg_keygrip" \
           -v "$path_test_files/.zyn-test-user-gpg-fingerprint:/run/secrets/zyn_gpg_fingerprint" \
           -v "$(zyn_project_root):/zyn" \
           -it \
           --env RUST_LOG=trace \
           -p 8085:80 \
           "$@"

elif [ "$cmd" == "web-client" ]; then

    server_address=${ZYN_SERVER_ADDRESS:-ws://localhost:8080}

    echo "Starting webclient using server address \"$server_address\""
    echo "Customize server address with environment variable ZYN_SERVER_ADDRESS"

    zyn-webserver \
        8081 \
        10.0.2.15 \
        8080 \
        --no-tls \
        --server-websocket-address "$server_address" \
        --debug-protocol \
        --debug-tornado \
        -vv \
        "$@"

elif [ "$cmd" == "web-client-js" ]; then

    pushd "$(zyn_project_root)/js" >/dev/null 2>&1
    npm run-script zyn-compile-js-watch
    popd >/dev/null 2>&1

elif [ "$cmd" == "web-client-css" ]; then

    pushd "$(zyn_project_root)/js" >/dev/null 2>&1
    npm run-script zyn-compile-css-watch
    popd >/dev/null 2>&1

elif [ "$cmd" == "shell" ]; then

    echo
    echo "Pass \"init admin <path-data> 10.0.2.15 8080\" to initialise client"
    echo

    zyn-shell \
        -vv \
        --debug-protocol \
        --password admin \
        --no-tls \
        "$@"

elif [ "$cmd" == "docker-swarm" ]; then

    # Check if node is in swarm mode
    swarm_mode=1
    docker swarm 2>&1 | grep leave >/dev/null || swarm_mode=0

    if [ $swarm_mode -eq 0 ]; then
        echo "Creating local single node swarm instance"
        docker swarm init
        docker service create --name registry --publish published=5000,target=5000 registry:2
    fi

    # Add secrets
    path_test_files="$(zyn_test_data)"
    docker secret create gpg_secret_key "$path_test_files/.zyn-test-user-gpg-secret-key" || :
    docker secret create gpg_password "$path_test_files/.zyn-test-user-gpg-password" || :
    docker secret create gpg_keygrip "$path_test_files/.zyn-test-user-gpg-keygrip" || :
    docker secret create gpg_fingerprint "$path_test_files/.zyn-test-user-gpg-fingerprint" || :

    echo
    echo "Docker swarm ready!"
    echo
    echo "Build Zyn server and webclient"
    echo
    echo "docker build -t 127.0.0.1:5000/zyn -f docker/dockerfile-zyn ."
    echo "docker build -t 127.0.0.1:5000/zyn-web-client -f docker/dockerfile-web-client ."
    echo
    echo "Push images to 127.0.0.1:5000/zyn and 127.0.0.1:5000/zyn-web-client to registry"
    echo "docker push 127.0.0.1:5000/zyn"
    echo "docker push 127.0.0.1:5000/zyn-web-client"
    echo
    echo "(If needed retag image with \"docker tag <old-tag> 127.0.0.1:5000/<service>\")"
    echo
    echo "To deploy, run:"
    echo "docker stack deploy --compose-file docker/docker-compose.yml zyn"
    echo
    echo "To cleanup:"
    echo "Delete service: \"docker service ls\" to list them and \"docker service rm <service>\" to delete"
    echo "Leave docker swarm: \"docker swarm leave --force\""
    echo

else
    echo "ERROR: Unknown command \"$cmd\""
    echo
    echo
    usage
fi
