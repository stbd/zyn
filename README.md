# ZYN

Simple fileserver with fopen/fclose like access.

## Development enviroment

```bash
cd vm/dev
vagrant up     # This will create a virtual machine which has all dependencies installed

vagrant ssh
source /vagrant_data/env/scripts/zyn-dev-env.sh # Temporary step
zyn-build      # Build system
zyn-unittests  # Run unittests

# Optionally run system tests which requires Python and dependecies (this installation is not yet automated)
su vagrant     # With password vagrant
sudo apt-get install python3 python3-pip
sudo pip3 install -e zyn/tests/zyn_util/
exit
zyn-system-tests

```