RethinkDB Python Client
***********************

.. image:: https://img.shields.io/pypi/v/rethinkdb.svg
    :target: https://pypi.python.org/pypi/rethinkdb
    :alt: PyPi Package

.. image:: https://github.com/rethinkdb/rethinkdb-python/actions/workflows/ci.yml/badge.svg?branch=master
    :target: https://github.com/rethinkdb/rethinkdb-python/actions/workflows/ci.yml
    :alt: CI Status

.. image:: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json
    :target: https://github.com/astral-sh/ruff
    :alt: Ruff

RethinkDB is the first open-source scalable database built for realtime applications.
It exposes a new database access model -- instead of polling for changes, the developer
can tell the database to continuously push updated query results to applications in realtime.
RethinkDB allows developers to build scalable realtime apps in a fraction of the time with
less effort.

Utility Scripts
===============
RethinkDB Python Client provides a set of utility scripts to help you manage your RethinkDB
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
Python 3.12+ to run. This is the preferred method to install RethinkDB Python client, as it will
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

Please read our contributing guideline_. This guideline will
walk you through how can you successfully contribute to
RethinkDB Python client.

.. _guideline: https://github.com/rethinkdb/rethinkdb-python/blob/master/CONTRIBUTING.rst

Installation
------------

Install `mise`_, then install the pinned toolchain and project dependencies::

    $ mise install
    $ mise run setup

``mise run setup`` creates ``.venv`` and installs the package, extras, and development tools with uv.

.. _mise: https://mise.jdx.dev/getting-started.html

Useful mise tasks
-----------------

+---------------------+---------------------------------------------------------------+
| Command             | Description                                                   |
+=====================+===============================================================+
| setup               | create the virtualenv and install extras and dev tools        |
+---------------------+---------------------------------------------------------------+
| format              | format Python sources with Ruff                               |
+---------------------+---------------------------------------------------------------+
| lint                | run Ruff and mypy                                             |
+---------------------+---------------------------------------------------------------+
| test                | run unit and integration tests and report coverage            |
+---------------------+---------------------------------------------------------------+
| test-unit           | run unit tests                                                |
+---------------------+---------------------------------------------------------------+
| test-integration    | run integration tests                                         |
+---------------------+---------------------------------------------------------------+
| docs                | build the Sphinx HTML documentation                           |
+---------------------+---------------------------------------------------------------+
| protobuf            | download and convert the protobuf file                        |
+---------------------+---------------------------------------------------------------+
| generate-init-pyi   | regenerate ``rethinkdb/__init__.pyi``                         |
+---------------------+---------------------------------------------------------------+
| clean               | remove build, test, coverage, and cache artifacts             |
+---------------------+---------------------------------------------------------------+

Run a task with ``mise run <task>``. ``mise tasks`` lists them.
