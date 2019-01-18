#!/bin/bash

set -e
set -u

if [ "${TRAVIS_PULL_REQUEST}" != "" ]; then
    echo "Skipping coverage upload for PR"
    exit;
fi

if [ "${CODACY_PROJECT_TOKEN}" = "" ]; then
    echo "Skipping coverage upload for missing CODACY_PROJECT_TOKEN"
    exit;
fi

set -ex

python-codacy-coverage -r coverage.xml
