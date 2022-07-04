#!/bin/bash

set -euo pipefail

DISTRIB_CODENAME="$(lsb_release -sc)"

sudo apt-key adv --keyserver keys.openpgp.org --recv-keys "539A3A8C6692E6E3F69B3FE81D85E93F801BB43F"
echo "deb https://download.rethinkdb.com/repository/ubuntu-${DISTRIB_CODENAME} ${DISTRIB_CODENAME} main" | sudo tee /etc/apt/sources.list.d/rethinkdb.list

sudo apt-get update --option Acquire::Retries=100 --option Acquire::http::Timeout="300"
sudo apt-get install -y --option Acquire::Retries=100 --option Acquire::http::Timeout="300" rethinkdb
