if [ "${TRAVIS_PULL_REQUEST}" != "" ]; then
    if [ "${CODACY_PROJECT_TOKEN}" = "" ]; then
        echo "Skipping coverage upload for PR or missing CODACY_PROJECT_TOKEN"
        exit;
    fi
fi
pytest -m unit --cov rebirthdb --cov-report xml
python-codacy-coverage -r coverage.xml
