# Copyright 2018 RethinkDB
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This file incorporates work covered by the following copyright:
# Copyright 2010-2016 RethinkDB, all rights reserved.


import os
import re

import setuptools

from rethinkdb.version import VERSION

try:
    import asyncio

    CONDITIONAL_PACKAGES = ['rethinkdb.asyncio_net']
except ImportError:
    CONDITIONAL_PACKAGES = []


RETHINKDB_VERSION_DESCRIBE = os.environ.get("RETHINKDB_VERSION_DESCRIBE")
VERSION_RE = r"^v(?P<version>\d+\.\d+)\.(?P<patch>\d+)?(\.(?P<post>\w+))?$"

if RETHINKDB_VERSION_DESCRIBE:
    MATCH = re.match(VERSION_RE, RETHINKDB_VERSION_DESCRIBE)

    if MATCH:
        VERSION = MATCH.group("version")

        if MATCH.group("patch"):
            VERSION += "." + MATCH.group("patch")

        if MATCH.group("post"):
            VERSION += "." + MATCH.group("post")

        with open("rethinkdb/version.py", "w") as f:
            f.write('VERSION = {0}'.format(repr(VERSION)))
    else:
        raise RuntimeError("{!r} does not match version format {!r}".format(
            RETHINKDB_VERSION_DESCRIBE, VERSION_RE))


setuptools.setup(
    name='rethinkdb',
    zip_safe=True,
    version=VERSION,
    description='Python driver library for the RethinkDB database server.',
    long_description=open('README.md', 'r').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/RethinkDB/rethinkdb-python',
    maintainer='RethinkDB.',
    maintainer_email='bugs@rethinkdb.com',
    classifiers=[
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    packages=[
        'rethinkdb',
        'rethinkdb.tornado_net',
        'rethinkdb.twisted_net',
        'rethinkdb.gevent_net',
        'rethinkdb.trio_net',
        'rethinkdb.backports',
        'rethinkdb.backports.ssl_match_hostname'
    ] + CONDITIONAL_PACKAGES,
    package_dir={'rethinkdb': 'rethinkdb'},
    package_data={'rethinkdb': ['backports/ssl_match_hostname/*.txt']},
    entry_points={
        'console_scripts': [
            'rethinkdb-import = rethinkdb._import:main',
            'rethinkdb-dump = rethinkdb._dump:main',
            'rethinkdb-export = rethinkdb._export:main',
            'rethinkdb-restore = rethinkdb._restore:main',
            'rethinkdb-index-rebuild = rethinkdb._index_rebuild:main',
            'rethinkdb-repl = rethinkdb.__main__:startInterpreter'
        ]
    },
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*",
    install_requires=[
        'six'
    ],
    test_suite='tests'
)
