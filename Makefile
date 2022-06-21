# Copyright 2022 RethinkDB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.DEFAULT_GOAL := help

PACKAGE_NAME = rethinkdb

PROTO_FILE_NAME = ql2.proto
PROTO_FILE_URL = https://raw.githubusercontent.com/rethinkdb/rethinkdb/70654faefe29bb0b4f6010fa82bd30a207d014d8/src/rdb_protocol/${PROTO_FILE_NAME}
TARGET_PROTO_FILE = ${PROTO_FILE_NAME}

FILE_CONVERTER_NAME = ./scripts/convert_protofile.py

CONVERTED_PROTO_FILE_NAME = ql2_pb2.py
TARGET_CONVERTED_PROTO_FILE = ${PACKAGE_NAME}/${CONVERTED_PROTO_FILE_NAME}

define BROWSER_PYSCRIPT
import os, webbrowser, sys

try:
	from urllib import pathname2url
except:
	from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := python -c "$$BROWSER_PYSCRIPT"

.PHONY: help
help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

.PHONY: clean
clean: clean-build clean-pyc clean-test clean-mypy ## remove all build, test, coverage and Python artifacts

.PHONY: clean-build
clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

.PHONY: clean-mypy
clean-mypy: ## remove mypy related artifacts
	rm -rf .mypy_cache

.PHONY: clean-pyc
clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

.PHONY: clean-test
clean-test: ## remove test and coverage artifacts
	rm -fr .tox/ \
	.coverage \
	htmlcov/ \
	.pytest_cache \
	.hypothesis/

.PHONY: docs
docs: ## generate Sphinx HTML documentation, including API docs
	rm -f docs/rethinkdb.rst
	rm -f docs/modules.rst
	poetry export --E all --dev -f requirements.txt > docs/requirements.txt
	sphinx-apidoc -o docs/ rethinkdb
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	$(BROWSER) docs/_build/html/index.html

.PHONY: format
format: ## run formatters on the package
	isort rethinkdb tests
	black rethinkdb tests

.PHONY: lint
lint: ## run linters against the package
	mypy rethinkdb
	bandit -q -r rethinkdb
	pylint rethinkdb
	flake8 rethinkdb --count --ignore=E203,E501,W503 --show-source --statistics

.PHONY: ql2.proto
ql2.proto: ## download and convert protobuf file
	curl -sqo ${TARGET_PROTO_FILE} ${PROTO_FILE_URL}
	python ${FILE_CONVERTER_NAME} -l python -i ${TARGET_PROTO_FILE} -o ${TARGET_CONVERTED_PROTO_FILE}

.PHONY: test-unit
test-unit: ## run unit tests and generate coverage
	coverage run -m pytest -m "not integration" -vv
	coverage report

.PHONY: test-integration
test-integration: ## run unit tests and generate coverage
	coverage run -m pytest -m "integration" -m "not v2_5" -vv
	coverage report

.PHONY: test
test: ## run all tests and generate coverage
	coverage run -m pytest -m "not v2_5" -vv
	coverage report
	coverage xml

.PHONY: download-test-reporter
download-test-reporter:
	curl -L https://codeclimate.com/downloads/test-reporter/test-reporter-latest-linux-amd64 > ./cc-test-reporter
	chmod +x ./cc-test-reporter

.PHONY: test-reporter-before
test-reporter-before:
	./cc-test-reporter before-build

.PHONY: upload-coverage
upload-coverage:
	./cc-test-reporter after-build -t "coverage.py"
