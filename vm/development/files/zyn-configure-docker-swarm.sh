#!/usr/bin/env bash

set -euo pipefail

# shellcheck disable=SC1090
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

# Check if node is in swarm mode
swarm_mode=1
docker swarm 2>&1 | grep leave >/dev/null || swarm_mode=0

if [ $swarm_mode -eq 0 ]; then
    echo "Creating local single node swarm instance"
    docker swarm init
    docker service create --name registry --publish published=5000,target=5000 registry:2
fi

# Add secrets
docker secret create gpg_secret_key "$path_test_user_gpg_files/.zyn-test-user-gpg-secret-key" || :
docker secret create gpg_password "$path_test_user_gpg_files/.zyn-test-user-gpg-password" || :
docker secret create gpg_keygrip "$path_test_user_gpg_files/.zyn-test-user-gpg-keygrip" || :
docker secret create gpg_fingerprint "$path_test_user_gpg_files/.zyn-test-user-gpg-fingerprint" || :

echo
echo "Docker swarm ready!"
echo
echo "Push image to 127.0.0.1:5000/zyn and 127.0.0.1:5000/zyn-web-client to registry"
echo "If needed retag image with \"docker tag <old-image> 127.0.0.1:5000/zyn\""
echo "Then push with \"docker push 127.0.0.1:5000/zyn\""
echo
echo "To deploy, run \"docker stack deploy --compose-file docker/docker-compose.yml zyn\""
echo
echo "To cleanup:"
echo "Delete service: \"docker service ls\" to list them and \"docker service rm <service>\" to delete"
echo "Leave docker swarm: \"docker swarm leave --force\""
echo
