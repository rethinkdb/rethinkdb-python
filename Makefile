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


default: help

help:
	@echo "Usage:"
	@echo
	@echo "	make help				Print this help message"
	@echo "	make test-unit			Run unit tests"
	@echo "	make test-integration	Run integration tests"
	@echo "	make test-remote		Run tests on digital ocean"
	@echo "	make upload-coverage	Upload unit test coverage"
	@echo "	make upload-pypi		Release rethinkdb package to PyPi"
	@echo "	make clean				Cleanup source directory"
	@echo "	make prepare			Prepare rethinkdb for build"

test-unit: prepare
	pytest -v -m unit

test-integration: prepare
	@rethinkdb&
	pytest -v -m integration
	@killall rethinkdb

test-ci: prepare
	@rethinkdb&
	pytest -v --cov rethinkdb --cov-report xml
	@killall rethinkdb

test-remote: prepare scripts/prepare_remote_test.py
	python scripts/prepare_remote_test.py pytest -m integration

install-db:
	scripts/install-db.sh

upload-coverage: prepare
	scripts/upload-coverage.sh

upload-pypi: prepare
	scripts/upload-pypi.sh

clean:
	git clean -dfx

rethinkdb/ql2.proto:
	curl -qo $@ https://raw.githubusercontent.com/rethinkdb/rethinkdb/next/src/rdb_protocol/ql2.proto

rethinkdb/ql2_pb2.py: scripts/convert_protofile.py rethinkdb/ql2.proto
	python scripts/convert_protofile.py -l python -i rethinkdb/ql2.proto -o rethinkdb/ql2_pb2.py

prepare: rethinkdb/ql2_pb2.py
