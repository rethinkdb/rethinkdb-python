RethinkDB Python Client
***********************

.. image:: https://img.shields.io/pypi/v/rethinkdb.svg
    :target: https://pypi.python.org/pypi/rethinkdb
    :alt: PyPi Package

.. image:: https://github.com/rethinkdb/rethinkdb-python/actions/workflows/build/badge.svg?branch=master
    :target: https://github.com/rethinkdb/rethinkdb-python/actions/workflows/build.yml
    :alt: Build Status

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/ambv/black
    :alt: Black Formatted

RethinkDB is the first open-source scalable database built for realtime applications.
It exposes a new database access model -- instead of polling for changes, the developer
can tell the database to continuously push updated query results to applications in realtime.
RethinkDB allows developers to build scalable realtime apps in a fraction of the time with
less effort.

Utility Scripts
===============
RethinkDB Python Client prodvices a set of utility scripts to help you manage your RethinkDB
database.

* `rethinkdb dump` / `rethinkdb-dump` - Dump a database to a file
* `rethinkdb export` / `rethinkdb-export` - Export a table to a file
* `rethinkdb import` / `rethinkdb-import` - Import a table from a file
* `rethinkdb index-rebuild` / `rethinkdb-index-rebuild` - Rebuild all indexes
* `rethinkdb repl` / `rethinkdb-repl` - Start a REPL session
* `rethinkdb restore` / `rethinkdb-restore` - Restore a database from a file

Installation
============

RethinkDB's Python Client can be installed by running ``pip install rethinkdb`` and it requires
Python 3.10+ to run. This is the preferred method to install RethinkDB Python client, as it will
always install the most recent stable release. If you don't have `pip`_ installed, this
`Python installation guide`_ can guide you through the process.

.. _pip: https://pip.pypa.io
.. _Python installation guide: http://docs.python-guide.org/en/latest/starting/installation/

Installing extras
-----------------

RethinkDB's Python Client tries to be as tiny as its possible, hence some functionalities
are requiring extra dependencies to be installed.

To install `rethinkdb` with an extra package run ``pip install rethinkdb[<EXTRA>]``,
where ``<EXTRA>`` is the name of the extra option. To install multiple extra packages
list the extra names separated by comma as described in `pip's examples`_ section point
number six.

+---------------------+--------------------------------------------+
| Extra               | Description                                |
+=====================+============================================+
| all                 | alias to install all the extras available  |
+---------------------+--------------------------------------------+

.. _`pip's examples`: https://pip.pypa.io/en/stable/reference/pip_install/#examples

Usage examples
==============

TODO

Contributing
============

Hurray, You reached this section, which means you are ready
to contribute.

Please read our contibuting guideline_. This guideline will
walk you through how can you successfully contribute to
RethinkDB Python client.

.. _guideline: https://github.com/rethinkdb/rethinkdb-python/blob/master/CONTRIBUTING.rst

Installation
------------

For development you will need poetry_, pre-commit_ and shellcheck_. After poetry installed,
simply run `poetry install -E all`. This command will both create the virtualenv
and install all development dependencies for you.

.. _poetry: https://python-poetry.org/docs/#installation
.. _pre-commit: https://pre-commit.com/#install
.. _shellcheck: https://www.shellcheck.net/


Useful make Commands
--------------------

+---------------------+---------------------------------------------------------------+
| Command             | Description                                                   |
+=====================+===============================================================+
| help                | show help message and exit                                    |
+---------------------+---------------------------------------------------------------+
| clean               | remove all build, test, coverage and Python artifacts         |
+---------------------+---------------------------------------------------------------+
| clean-build         | remove build artifacts                                        |
+---------------------+---------------------------------------------------------------+
| clean-mypy          | remove mypy related artifacts                                 |
+---------------------+---------------------------------------------------------------+
| clean-pyc           | remove Python file artifacts                                  |
+---------------------+---------------------------------------------------------------+
| clean-test          | remove test and coverage artifacts                            |
+---------------------+---------------------------------------------------------------+
| docs                | generate Sphinx HTML documentation, including API docs        |
+---------------------+---------------------------------------------------------------+
| format              | run formatters on the package                                 |
+---------------------+---------------------------------------------------------------+
| generate-init-pyi   | generate __init__.pyi file                                    |
+---------------------+---------------------------------------------------------------+
| lint                | run linters against the package                               |
+---------------------+---------------------------------------------------------------+
| protobuf            | download and convert protobuf file                            |
+---------------------+---------------------------------------------------------------+
| test                | run all tests and generate coverage                           |
+---------------------+---------------------------------------------------------------+
| test-integration    | run unit tests and generate coverage                          |
+---------------------+---------------------------------------------------------------+
| test-unit           | run unit tests and generate coverage                          |
+---------------------+---------------------------------------------------------------+
