#!/bin/bash

set -e
set -u

export DISTRIB_CODENAME=$(lsb_release -sc)

echo "deb https://dl.bintray.com/rebirthdb/apt $DISTRIB_CODENAME main" | sudo tee /etc/apt/sources.list.d/rebirthdb.list
wget -qO- https://dl.bintray.com/rebirthdb/keys/pubkey.gpg | sudo apt-key add -

sudo apt-get update --option Acquire::Retries=100 --option Acquire::http::Timeout="300"
sudo apt-get --allow-unauthenticated install rebirthdb --option Acquire::Retries=100 --option Acquire::http::Timeout="300"
