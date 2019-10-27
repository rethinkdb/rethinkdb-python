#!/bin/bash

set -e
set -u

export DISTRIB_CODENAME=$(lsb_release -sc)

source /etc/lsb-release && echo "deb https://download.rethinkdb.com/apt $DISTRIB_CODENAME main" | sudo tee /etc/apt/sources.list.d/rethinkdb.list
wget -qO- https://download.rethinkdb.com/apt/pubkey.gpg | sudo apt-key add -

sudo apt-get update --option Acquire::Retries=100 --option Acquire::http::Timeout="300"
sudo apt-get install -y --option Acquire::Retries=100 --option Acquire::http::Timeout="300" rethinkdb
