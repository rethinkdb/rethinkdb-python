RethinkDB Python Client
***********************

.. image:: https://img.shields.io/pypi/v/rethinkdb.svg
    :target: https://pypi.python.org/pypi/rethinkdb
    :alt: PyPi Package

.. image:: https://github.com/rethinkdb/rethinkdb-python/actions/workflows/build/badge.svg?branch=master
    :target: https://github.com/rethinkdb/rethinkdb-python/actions/workflows/build.yml
    :alt: Build Status

.. image:: https://api.codeclimate.com/v1/badges/e5023776401a5f0e82f1/maintainability
   :target: https://codeclimate.com/github/rethinkdb/rethinkdb-python/maintainability
   :alt: Maintainability

.. image:: https://api.codeclimate.com/v1/badges/e5023776401a5f0e82f1/test_coverage
   :target: https://codeclimate.com/github/rethinkdb/rethinkdb-python/test_coverage
   :alt: Test Coverage

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/ambv/black
    :alt: Black Formatted

RethinkDB is the first open-source scalable database built for realtime applications.
It exposes a new database access model -- instead of polling for changes, the developer
can tell the database to continuously push updated query results to applications in realtime.
RethinkDB allows developers to build scalable realtime apps in a fraction of the time with
less effort.

Utility Tools
============
If you came here looking for an utility tool, this functionality has been dropped during the porting from python 2 to 3.
Instead, you should look (and we suggest to use) https://github.com/BOOMfinity-Developers/GoThink


Installation
============

RethinkDB's Python Client can be installed by running ``pip install rethinkdb`` and it requires
Python 3.7.0+ to run. This is the preferred method to install RethinkDB Python client, as it
will always install the most recent stable release. If you don't have `pip`_
installed, this `Python installation guide`_ can guide
you through the process.

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

+------------------+-------------------------------------+
| Command          | Description                         |
+==================+=====================================+
| help             | Print available make commands       |
+------------------+-------------------------------------+
| clean            | Remove all artifacts                |
+------------------+-------------------------------------+
| clean-build      | Remove build artifacts              |
+------------------+-------------------------------------+
| clean-mypy       | Remove mypy artifacts               |
+------------------+-------------------------------------+
| clean-pyc        | Remove Python artifacts             |
+------------------+-------------------------------------+
| clean-test       | Remove test artifacts               |
+------------------+-------------------------------------+
| docs             | Generate Sphinx documentation       |
+------------------+-------------------------------------+
| format           | Run several formatters              |
+------------------+-------------------------------------+
| lint             | Run several linters after format    |
+------------------+-------------------------------------+
| protobuf         | Download and convert protobuf file  |
+------------------+-------------------------------------+
| test             | Run all tests with coverage         |
+------------------+-------------------------------------+
| test-unit        | Run unit tests with coverage        |
+------------------+-------------------------------------+
| test-integration | Run integration tests with coverage |
+------------------+-------------------------------------+
