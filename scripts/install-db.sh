#!/bin/bash

set -e
set -u

export DISTRIB_CODENAME=$(lsb_release -sc)

echo "This currently will not work for rethinkdb. It is in the process of being fixed."
exit 1
echo "deb https://dl.bintray.com/rethinkdb/apt $DISTRIB_CODENAME main" | sudo tee /etc/apt/sources.list.d/rethinkdb.list
wget -qO- https://dl.bintray.com/rethinkdb/keys/pubkey.gpg | sudo apt-key add -

sudo apt-get update --option Acquire::Retries=100 --option Acquire::http::Timeout="300"
sudo apt-get --allow-unauthenticated install rethinkdb --option Acquire::Retries=100 --option Acquire::http::Timeout="300"
