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

'''

'''

import setuptools

try:
    import asyncio
    conditional_packages = ['rethinkdb.asyncio_net']
except ImportError:
    conditional_packages = []

from rethinkdb.version import version


setuptools.setup(
    name='rebirthdb',
    zip_safe=True,
    version=version,
    description='Python driver library for the RethinkDB database server.',
    long_description=__doc__,
    url='https://github.com/RebirthDB/rebirthdb-python',
    maintainer='RebirthDB.',
    maintainer_email='bugs@rethinkdb.com',
    classifiers=[
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    packages=[
        'rethinkdb',
        'rethinkdb.tornado_net',
        'rethinkdb.twisted_net',
        'rethinkdb.gevent_net',
        'rethinkdb.backports',
        'rethinkdb.backports.ssl_match_hostname'
    ] + conditional_packages,
    package_dir={'rethinkdb':'rethinkdb'},
    package_data={ 'rethinkdb':['backports/ssl_match_hostname/*.txt'] },
    entry_points={
        'console_scripts':[
            'rethinkdb-import = rethinkdb._import:main',
            'rethinkdb-dump = rethinkdb._dump:main',
            'rethinkdb-export = rethinkdb._export:main',
            'rethinkdb-restore = rethinkdb._restore:main',
            'rethinkdb-index-rebuild = rethinkdb._index_rebuild:main',
            'rethinkdb-repl = rethinkdb.__main__:startInterpreter'
        ]
    },
    setup_requires=['pytest-runner'],
    test_suite='tests',
    tests_require=['pytest']
)
