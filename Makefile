# Copyright 2018 RethinkDB
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

.PHONY: default help test-unit test-integration test-remote upload-coverage upload-pypi clean prepare

PACKAGE_NAME = rethinkdb

PROTO_FILE_NAME = ql2.proto
PROTO_FILE_URL = https://raw.githubusercontent.com/rethinkdb/rethinkdb/next/src/rdb_protocol/${PROTO_FILE_NAME}
TARGET_PROTO_FILE = ${PACKAGE_NAME}/${PROTO_FILE_NAME}

FILE_CONVERTER_NAME = ./scripts/convert_protofile.py
REMOTE_TEST_SETUP_NAME = ./scripts/prepare_remote_test.py

CONVERTED_PROTO_FILE_NAME = ql2_pb2.py
TARGET_CONVERTED_PROTO_FILE = ${PACKAGE_NAME}/${CONVERTED_PROTO_FILE_NAME}


default: help

help:
	@echo "Usage:"
	@echo
	@echo "	make help				Print this help message"
	@echo "	make test-unit			Run unit tests"
	@echo "	make test-integration	Run integration tests"
	@echo "	make test-remote		Run tests on digital ocean"
	@echo "	make upload-coverage	Upload unit test coverage"
	@echo "	make upload-pypi		Release ${PACKAGE_NAME} package to PyPi"
	@echo "	make clean				Cleanup source directory"
	@echo "	make prepare			Prepare ${PACKAGE_NAME} for build"

test-unit:
	pytest -v -m unit

test-integration:
	@rebirthdb&
	pytest -v -m integration
	@killall rebirthdb

test-ci:
	@rethinkdb&
	pytest -v --cov rethinkdb --cov-report xml
	@killall rethinkdb

test-remote:
	curl -qo ${REMOTE_TEST_SETUP_NAME} ${REMOTE_TEST_SETUP_URL}
	python ${REMOTE_TEST_SETUP_NAME} pytest -m integration

install-db:
	@sh scripts/install-db.sh

upload-coverage:
	@sh scripts/upload-coverage.sh

upload-pypi: prepare
	@sh scripts/upload-pypi.sh

clean:
	@rm -rf \
		${TARGET_PROTO_FILE} \
		${TARGET_CONVERTED_PROTO_FILE} \
		.pytest_cache \
		.eggs \
		.dist \
		*.egg-info

prepare:
	curl -qo ${TARGET_PROTO_FILE} ${PROTO_FILE_URL}
	python ${FILE_CONVERTER_NAME} -l python -i ${TARGET_PROTO_FILE} -o ${TARGET_CONVERTED_PROTO_FILE}
