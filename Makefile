# Copyright 2018 RebirthDB
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

.PHONY: default help clean prepare package publish

PACKAGE_NAME = rethinkdb

BUILD_DIR = ./build
PACKAGE_DIR = ${BUILD_DIR}/package

PROTO_FILE_NAME = ql2.proto
PROTO_FILE_URL = https://raw.githubusercontent.com/RebirthDB/rebirthdb/next/src/rdb_protocol/${PROTO_FILE_NAME}
TARGET_PROTO_FILE = ${PACKAGE_NAME}/${PROTO_FILE_NAME}

FILE_CONVERTER_NAME = convert_protofile.py
FILE_CONVERTER_URL = https://raw.githubusercontent.com/RebirthDB/rebirthdb/next/scripts/${FILE_CONVERTER_NAME}

CONVERTED_PROTO_FILE_NAME = ql2_pb2.py
TARGET_CONVERTED_PROTO_FILE = ${PACKAGE_NAME}/${CONVERTED_PROTO_FILE_NAME}


default: help

help:
	@echo "Usage:"
	@echo
	@echo "	make help	Print this help message"
	@echo "	make test	Run unit tests"
	@echo "	make clean	Cleanup source directory"
	@echo "	make prepare	Prepare ${PACKAGE_NAME} for build"
	@echo "	make package	Build ${PACKAGE_NAME} package"
	@echo "	make publish	Publish ${PACKAGE_NAME} package on PyPi"

clean:
	@rm -rf \
		${TARGET_PROTO_FILE} \
		${TARGET_CONVERTED_PROTO_FILE} \
		${BUILD_DIR}

prepare:
	curl -qo ${TARGET_PROTO_FILE} ${PROTO_FILE_URL}
	curl -qo ${FILE_CONVERTER_NAME} ${FILE_CONVERTER_URL}
	python ./${FILE_CONVERTER_NAME} -l python -i ${TARGET_PROTO_FILE} -o ${TARGET_CONVERTED_PROTO_FILE}
	rsync -av ./ ${BUILD_DIR} --filter=':- .gitignore'
	cp ${TARGET_PROTO_FILE} ${BUILD_DIR}/${PACKAGE_NAME}

package: prepare
	cd ${BUILD_DIR} && python ./setup.py sdist --dist-dir=$(abspath ${PACKAGE_DIR})

publish:
	cd ${BUILD_DIR} && python ./setup.py register upload

