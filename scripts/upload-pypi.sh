#!/bin/bash

set -e
set -u

export UPLOAD_STAGING=

if [ "${TRAVIS_PULL_REQUEST}" != "" ]; then
    echo 'Using staging pypi upload for PR'
    export UPLOAD_STAGING='yes'
fi

if [ "${TRAVIS_EVENT_TYPE}" = "cron" ]; then
    echo 'Using staging pypi upload for cron job'
    export UPLOAD_STAGING='yes'
fi

set -ex

python3 -m pip install --upgrade setuptools wheel

if [ "${UPLOAD_STAGING}" = "yes" ]; then
    export RETHINKDB_VERSION_DESCRIBE=$(git describe --tags --abbrev=0)
else
    export RETHINKDB_VERSION_DESCRIBE=$(git describe --tags --abbrev=8)
fi

python3 setup.py sdist bdist_wheel

python3 -m pip install --upgrade twine

if [ "${UPLOAD_STAGING}" = "yes" ]; then
    export TWINE_PASSWORD="${TWINE_STAGEING_PASSWORD}"
    export TWINE_USERNAME="${TWINE_STAGEING_USERNAME}"

    twine upload --repository-url 'https://test.pypi.org/legacy/' dist/*
    python3 -m pip install --index-url 'https://test.pypi.org/simple/' rethinkdb
else
    twine upload dist/*
    python3 -m pip install rethinkdb
fi
