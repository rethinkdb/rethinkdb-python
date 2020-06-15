#!/bin/bash

set -e
set -u

export DISTRIB_CODENAME=$(lsb_release -sc)

sudo apt-key adv --keyserver keys.gnupg.net --recv-keys "539A 3A8C 6692 E6E3 F69B 3FE8 1D85 E93F 801B B43F"
echo "deb https://download.rethinkdb.com/repository/ubuntu-xenial xenial main" | sudo tee /etc/apt/sources.list.d/rethinkdb.list

sudo apt-get update --option Acquire::Retries=100 --option Acquire::http::Timeout="300"
sudo apt-get install -y --option Acquire::Retries=100 --option Acquire::http::Timeout="300" rethinkdb
