.. highlight:: shell

============
Contributing
============

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

You can contribute in many ways:

Types of Contributions
----------------------

Report Bugs
~~~~~~~~~~~

Report bugs at https://github.com/rethinkdb/rethinkdb-python/issues.

If you are reporting a bug, please include:

- Your operating system name and version.
- Any details about your local setup that might be helpful in troubleshooting.
- Detailed steps to reproduce the bug.

Fix Bugs
~~~~~~~~

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

Implement Features
~~~~~~~~~~~~~~~~~~

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it. In case you added a
new Rule or Precondition, do not forget to add them to the docs as well.

Write Documentation
~~~~~~~~~~~~~~~~~~~

RethinkDB could always use more documentation, whether as part of the
official RethinkDB docs, in docstrings, or even on the web in blog posts,
articles, and such.

Submit Feedback
~~~~~~~~~~~~~~~

The best way to send feedback is to file an issue at https://github.com/rethinkdb/rethinkdb-python/issues.

If you are proposing a feature:

- Explain in detail how it would work.
- Keep the scope as narrow as possible, to make it easier to implement.
- Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

Get Started!
------------

.. note::

    You may want to see the developer documentation, not that one which is intended for
    our users. To build the documentation, run ``mise run docs``.

Ready to contribute? Here's how to set up `rethinkdb`'s Python client for local development.

As step 0, install `mise`_. Python, uv, and pre-commit are pinned in ``mise.toml``.

.. _mise: https://mise.jdx.dev/getting-started.html

1. Fork the `rethinkdb-python` repo on GitHub.
2. Clone your fork locally::

    $ git clone git@github.com:your_name_here/rethinkdb-python.git

3. Install the toolchain and the local copy::

    $ cd rethinkdb-python/
    $ mise install
    $ mise run setup
    $ pre-commit install

4. Create a branch for local development::

    $ git checkout -b name-of-your-bugfix-or-feature

   Now you can make your changes locally.

5. When you're done making changes, check that your changes pass linters and the tests::

    $ mise run format
    $ mise run lint
    $ mise run test
    $ pre-commit run --all-files

   ``mise run test`` runs integration tests. To make them pass, you need a
   running RethinkDB server on ``localhost:28015``. To run only unit tests, execute
   ``mise run test-unit``.

6. Commit your changes and push your branch to GitHub. Commit messages follow
   `Conventional Commits`_ (``feat``, ``fix``, ``docs``, ``style``, ``refactor``,
   ``perf``, ``test``, ``build``, ``ci``, ``chore``, ``revert``)::

    $ git add .
    $ git commit -m "fix: correct the handshake timeout."
    $ git push origin name-of-your-bugfix-or-feature

   .. _Conventional Commits: https://www.conventionalcommits.org/en/v1.0.0/

7. Submit a pull request through the GitHub website.

Pull Request Guidelines
-----------------------

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.rst.
3. The pull request should work for Python 3.12+.

Dependencies
------------

Renovate opens pull requests for Python packages, GitHub Actions, and mise tools.
The configuration is ``renovate.json``. Minor and patch updates are grouped.
Major updates are opened separately. Install the Renovate GitHub App on the
repository to enable it.

Releasing
---------

Releases are cut by `Release Please`_ from conventional commits on ``master``.
Merging its release pull request tags the version, updates ``CHANGELOG.md`` and
``pyproject.toml``, and publishes a GitHub release. The publish workflow then
builds the package and uploads it to PyPI with trusted publishing.

The manifest currently records ``2.5.0``, which is not yet tagged. The next
conventional commits on ``master`` open a pull request for the following
version. Publishing ``2.5.0`` itself is a deliberate GitHub release; the
publish workflow uploads that release as well.

Before merging a release pull request:

1. ``mise run format`` succeeds.
2. ``mise run lint`` succeeds.
3. ``mise run test`` succeeds.
4. ``mise run docs`` succeeds.

PyPI needs a trusted publisher for this repository, workflow
``.github/workflows/publish.yml``, and the GitHub environment ``pypi``.

.. _Release Please: https://github.com/googleapis/release-please
