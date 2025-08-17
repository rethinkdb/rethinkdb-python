CHANGELOG
=========

All notable changes to this project will be documented in this file.
The format is based on `Keep a Changelog`_, and this project adheres to
`Semantic Versioning`_.

.. _Keep a Changelog: https://keepachangelog.com/en/1.0.0/
.. _Semantic Versioning: https://semver.org/spec/v2.0.0.html

.. Hyperlinks for releases

.. _Unreleased: https://github.com/rethinkdb/rethinkdb-python/compare/master...master
.. .. _2.5.0: https://github.com/rethinkdb/rethinkdb-python/releases/tag/v2.5.0

Unreleased_
-----------

This is a major release that brings significant improvements including Python 3 support, async drivers, enhanced CLI tools, and comprehensive modernization of the codebase. This release is **NOT backward compatible** with 2.4.x.

Added
~~~~~

* **Python 3 Support**: Full Python 3 compatibility with support for Python 3.10+
* **Async Drivers**: Ported async/await support for multiple event loop implementations:
  * `rethinkdb.net_asyncio` - Native asyncio support with async/await syntax and `Connection` API
  * `rethinkdb.net_gevent` - Gevent-based async support with `Connection` API
  * `rethinkdb.net_tornado` - Tornado integration using `IOLoop` and `Connection` API
  * `rethinkdb.net_trio` - Trio support with `Connection` API
  * `rethinkdb.net_twisted` - Twisted integration with `inlineCallbacks` and `Connection` API
* **CLI Tools (Click-based)**:
  * `rethinkdb dump`, `export`, `import`, `index-rebuild`, `repl`, `restore`
  * All commands accept common flags/options: `-q/--quiet`, `--debug`, `-c/--connect`, `--driver-port`, `--host-name`, `-u/--user`, `-p/--password`, `--password-file`, `--tls-cert`
  * `export` supports `--format json|csv|ndjson|jsongz`, `--delimiter` (CSV), `--compression-level` (jsongz), `--fields`, `--clients`, `--read-outdated`
* **Type Hints**: Extensive typing across modules
* **Modern Build/Packaging**: `pyproject.toml` + Poetry
* **Tests**: New integration tests for each async driver: `tests/integration/test_net_{asyncio,gevent,tornado,trio,twisted}.py`
* **Documentation**: Sphinx docs under `docs/` with API, history, and vulnerabilities
* **CI/CD**: GitHub Actions workflows; pre-commit hooks for linting/formatting
* **Protocol Buffer**: `rethinkdb/ql2_pb2.py` now tracked in VCS

Changed
~~~~~~~

API and behavior changes (including breaking changes) compared to 2.4.x:

* **Namespace and Entry Point**:
  * The `r` entry point is now a `SimpleNamespace` exposing query helpers in `rethinkdb/__init__.py`
  * A lightweight `Client` wrapper is provided to manage connection loop types via `set_loop_type()`
* **Exceptions and Error Handling**:
  * Renamed `Rql*` to `Reql*` across the codebase; removed `Rql*` aliases
  * Introduced `InvalidHandshakeStateError` for invalid handshake transitions
  * `ReqlAuthError` raises `ValueError` if only host or only port is provided; includes host:port in message if both are provided
  * `ReqlTimeoutError` now subclasses `TimeoutError` and mirrors `ReqlAuthError` host/port validation and messaging
* **QueryPrinter (errors.QueryPrinter)**:
  * `print_query` -> property `query`
  * `print_carrots` -> property `carets`
  * Error messages now embed composed query and caret markers
* **Handshake (`rethinkdb.handshake.HandshakeV1_0`)**:
  * Uses protected attributes for credentials: `__username`, `__password`
  * Username is escaped as per RFC (`,` => `=2C`, `=` => `=3D`)
  * Accepts optional `json_encoder`/`json_decoder` (defaults to `json.JSONEncoder`/`json.JSONDecoder`)
  * Raises `ReqlAuthError` for auth failures (error_code 10..20), `ReqlDriverError` for other handshake errors
  * Raises `InvalidHandshakeStateError` on unexpected/unknown state and on invalid transitions in `next_message`
  * Validates server protocol version range and server signature per SCRAM-SHA-256
* **Encoder/Decoder (`rethinkdb.encoder`)**:
  * Introduced `ReqlEncoder` and `ReqlDecoder` classes
  * Moved pseudo-type conversions here (TIME, BINARY, GROUPED_DATA)
  * Renamed utility `recursively_make_hashable` -> `make_hashable`
* **Utilities**:
  * Moved `T` to `rethinkdb.utils.EnhancedTuple` and renamed parameter `intsp` -> `int_separator`
  * `utilities.py` renamed/moved to `utils.py`
* **AST (`rethinkdb.ast`)**:
  * Renamed internal `optargs` to `kwargs` consistently across query node implementations
  * Query operators improved for infix precedence; better error hints for misuse (e.g., `a < b | b < c`)
  * `__str__` of queries now uses `QueryPrinter(self).query`
* **Cursor/Connection (`rethinkdb.net`)**:
  * `Connection.resume(cursor)` replaces `_continue` (now public)
  * `Connection.stop(cursor)` replaces `_stop` (now public)
  * `Cursor.extend(...)` made public (from `_extend`)
  * `Cursor.raise_error(...)` replaces `_error` (now public)
  * Improved wait/timeout behavior and state tracking
* **CLI**:
  * Subcommand name standardized to `index-rebuild` (was `index_rebuild`)
  * Logging unified; progress reporting moved to logger-based updates
* **Build/Tooling**:
  * Replaced `setup.py`/`requirements.txt` with Poetry; consistent formatting (black) and linting (flake8, pylint)
  * Updated supported Python versions; CI matrix adjusted

Fixed
~~~~~

* Binary response parsing in network layer (`encoder.py`, `net.py`)
* Numerous linter and mypy findings across `ast`, `net`, and CLI modules
* Circular import issues and import path clean-ups
* Error propagation and context in async operations
* Potential memory leaks in connection handling and cursors

Removed
~~~~~~~

* Python 2 support dropped completely
* Legacy CLI scripts removed; replaced by Click-based `rethinkdb/cli/*`
* Deprecated modules removed:
  * `rethinkdb/backports/*` (SSL hostname matching backports)
  * `rethinkdb/logger.py`, `rethinkdb/helpers.py`, `rethinkdb/docs.py`
* Old dependencies and tooling removed (bandit, legacy CI scripts)
* Travis CI replaced with GitHub Actions
* `setup.py` and `requirements.txt` removed in favor of `pyproject.toml` and Poetry lockfile
* Top-level re-export of errors via package `__all__` removed; import from `rethinkdb.errors` instead
* `auth_key` parameter removed in favor of `password` for connections
* `Rql*` exception aliases removed (use `Reql*`)

Deprecated
~~~~~~~~~~

* `ReqlQuery.to_json_string()` is an alias for `to_json()` and will be removed in a future release

Migration Guide
~~~~~~~~~~~~~~

For users upgrading from 2.4.x to 2.5.0:

1. **Python**: Use Python 3.7+.
2. **Imports**: Update imports for moved modules (e.g., utilities now in `rethinkdb.utils`).
3. **Exceptions**: Replace `Rql*` with `Reql*`. Adjust error handling for `ReqlAuthError`/`ReqlTimeoutError` host/port validation.
4. **Handshake**: If customizing JSON encoding/decoding during handshake, pass `json_encoder`/`json_decoder` to `HandshakeV1_0`.
5. **CLI**: Use `index-rebuild` (hyphen), and the standardized flags listed above.
6. **Connection/Cursor**: Use public `Connection.resume`, `Connection.stop`, `Cursor.extend`, and `Cursor.raise_error`.
7. **Queries**: If relying on `optargs`, update to `kwargs`. Prefer `to_json()` over `to_json_string()`.

.. EXAMPLE CHANGELOG ENTRY

    0.1.0_ - 2020-01-xx
    --------------------

    Added
    ~~~~~

    * TODO.

    Changed
    ~~~~~~~

    * TODO.

    Fixed
    ~~~~~

    * TODO.

    Removed
    ~~~~~~~

    * TODO
