# Copyright 2018 RebirthDB
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


import setuptools

try:
    import asyncio

    CONDITIONAL_PACKAGES = ['rebirthdb.asyncio_net']
except ImportError:
    CONDITIONAL_PACKAGES = []

from rebirthdb.version import VERSION


setuptools.setup(
    name='rebirthdb',
    zip_safe=True,
    version=VERSION,
    description='Python driver library for the RethinkDB database server.',
    long_description=__doc__,
    url='https://github.com/RebirthDB/rebirthdb-python',
    maintainer='RebirthDB.',
    maintainer_email='bugs@rebirthdb.com',
    classifiers=[
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    packages=[
        'rebirthdb',
        'rebirthdb.tornado_net',
        'rebirthdb.twisted_net',
        'rebirthdb.gevent_net',
        'rebirthdb.backports',
        'rebirthdb.backports.ssl_match_hostname'
    ] + CONDITIONAL_PACKAGES,
    package_dir={'rebirthdb': 'rebirthdb'},
    package_data={'rebirthdb': ['backports/ssl_match_hostname/*.txt']},
    entry_points={
        'console_scripts': [
            'rebirthdb-import = rebirthdb._import:main',
            'rebirthdb-dump = rebirthdb._dump:main',
            'rebirthdb-export = rebirthdb._export:main',
            'rebirthdb-restore = rebirthdb._restore:main',
            'rebirthdb-index-rebuild = rebirthdb._index_rebuild:main',
            'rebirthdb-repl = rebirthdb.__main__:startInterpreter'
        ]
    },
    setup_requires=['pytest-runner'],
    test_suite='tests',
    tests_require=['pytest']
)
