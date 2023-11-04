# Zyn

Simple fileserver with fopen/fclose like access over network.

## Architehture

## Deployment on Docker Swarm

1. Create a copy of docker/docker-compose-prod-base.yaml and customize it to your environment 
2. Make sure you have GPG key with subkey that can be used for encryption
* [How to generate key](https://docs.github.com/en/authentication/managing-commit-signature-verification/generating-a-new-gpg-key)
* [What is subkey](https://wiki.debian.org/Subkeys)
3. Create Docker secrets
```
gpg --list-keys --fingerprint --fingerprint --with-keygrip   # Print information about keys
                                                             # Subkey fingerprint, keygrip, password need to converted to secrets
docker secret create zyn_gpg_fingerprint ...
docker secret create zyn_gpg_keygrip ...
docker secret create zyn_gpg_password ...
gpg --export-secret-key <key-email>| docker secret create zyn_gpg_secret_key -
``` 

docker stack deploy --compose-file docker-compose-prod-<customized>.yml <stack-name>

## Development Enviroment

```bash
cd vm/deveplopment
vagrant up              # Create virtual machines from Vagrant configuration

vagrant ssh             # Connect to the VM

# See usage printed by environment
# Few example commands to try
zyn-build
zyn-unittests
zyn-system-tests

```