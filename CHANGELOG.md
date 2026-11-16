# Changelog

All notable changes to this project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Release Please writes new sections from conventional commits. The notes below
for 2.5.0 were written by hand.

## [3.0.0](https://github.com/rethinkdb/rethinkdb-python/compare/v2.5.0...v3.0.0) (2026-11-16)


### ⚠ BREAKING CHANGES

* from Python 2 to Python 3 and bump to v2.5 ([#277](https://github.com/rethinkdb/rethinkdb-python/issues/277))

### Features

* implement async context managers ([#318](https://github.com/rethinkdb/rethinkdb-python/issues/318)) ([7c56a00](https://github.com/rethinkdb/rethinkdb-python/commit/7c56a0013f2d8b6cb81787c257906e586c816c72))


### Bug Fixes

* 225, dump fails when reading password from stdin ([#276](https://github.com/rethinkdb/rethinkdb-python/issues/276)) ([bc1a75a](https://github.com/rethinkdb/rethinkdb-python/commit/bc1a75a8a1126999afe3b70112a7fcad8e3b9624))
* 2598 test dump/backup/restore/import ([4b780e6](https://github.com/rethinkdb/rethinkdb-python/commit/4b780e69d8716e8d3fc6bb5b197fded5223a2c3a))
* access option.temp_dir correctly ([1f854b1](https://github.com/rethinkdb/rethinkdb-python/commit/1f854b1b071f12c086998da0e6046ad1fdbddfc0))
* access option.temp_dir correctly ([4ea6719](https://github.com/rethinkdb/rethinkdb-python/commit/4ea6719194099d0ad24de5ae6a3f9340a4c7e836))
* add r strings for regex escape ([#313](https://github.com/rethinkdb/rethinkdb-python/issues/313)) ([339cd9a](https://github.com/rethinkdb/rethinkdb-python/commit/339cd9a8b895ed3b1d74b2d8878eb7f0a5d4a9e9))
* address AST date vs datetime parsing issues ([#319](https://github.com/rethinkdb/rethinkdb-python/issues/319)) ([72bae52](https://github.com/rethinkdb/rethinkdb-python/commit/72bae52feab06026c844a7730ee6aaf6dbd5cc42))
* asyncio ConnectionInstance should yield from on Exception ([#5820](https://github.com/rethinkdb/rethinkdb-python/issues/5820)) ([574c7b6](https://github.com/rethinkdb/rethinkdb-python/commit/574c7b65a92c9bda2ff9188c7cf6b8d72fea758d))
* do not depend on package to get the version ([5b6b193](https://github.com/rethinkdb/rethinkdb-python/commit/5b6b193b0cd61e9aabdbbf30205589bdbd8b601d))
* do not depend on package to get the version ([85410ce](https://github.com/rethinkdb/rethinkdb-python/commit/85410ce8df25a67c8fe140bc5e514e038845e5f3))
* do not drop trio and twisted streaming conn ([#320](https://github.com/rethinkdb/rethinkdb-python/issues/320)) ([9cd3f4c](https://github.com/rethinkdb/rethinkdb-python/commit/9cd3f4c6bf7bb20c206a18e11d71ba855f9d42c7))
* ensure outcome version bind to the correct python version ([ecf1dd6](https://github.com/rethinkdb/rethinkdb-python/commit/ecf1dd6a4ccbec2c57dc73aaf711a148095f9be4))
* ensure streaming queries stay open and can be closed ([#317](https://github.com/rethinkdb/rethinkdb-python/issues/317)) ([a3ae56c](https://github.com/rethinkdb/rethinkdb-python/commit/a3ae56c7dd85f349a2fbd711bd9b163453921b6b))
* queries with constants fail to serialize ([fe1e996](https://github.com/rethinkdb/rethinkdb-python/commit/fe1e996f0f4f0bf8d6daeed58f09dc6d16181fcb))
* queries with constants fail to serialize ([c6a9682](https://github.com/rethinkdb/rethinkdb-python/commit/c6a96822de03b032ec339b2519ebd62f5ffc307c))
* report import workers killed by a signal ([#314](https://github.com/rethinkdb/rethinkdb-python/issues/314)) ([366e7ce](https://github.com/rethinkdb/rethinkdb-python/commit/366e7ceab4f13645dd044bd453439f4566df9ba5))
* syntax warning ([b5d19a0](https://github.com/rethinkdb/rethinkdb-python/commit/b5d19a0e6d703e17f28b675b8c7acefd47a152f7))
* wait for each write to finish ([#321](https://github.com/rethinkdb/rethinkdb-python/issues/321)) ([e02c553](https://github.com/rethinkdb/rethinkdb-python/commit/e02c553603d3a79c577787a38d28fc168d69e0e1))


### Code Refactoring

* from Python 2 to Python 3 and bump to v2.5 ([#277](https://github.com/rethinkdb/rethinkdb-python/issues/277)) ([ce50336](https://github.com/rethinkdb/rethinkdb-python/commit/ce5033650bb2077de00130639be666c11552bce3))

## [2.5.0]

This is a major release that brings significant improvements including Python 3 support, async drivers, enhanced CLI tools, and comprehensive modernization of the codebase. This release is **NOT backward compatible** with 2.4.x.

### Added

- **Python 3 Support**: Full Python 3 compatibility with support for Python 3.12+
- **Async Drivers**: Ported async/await support for multiple event loop implementations:
  - `rethinkdb.net_asyncio` - Native asyncio support with async/await syntax and `Connection` API
  - `rethinkdb.net_gevent` - Gevent-based async support with `Connection` API
  - `rethinkdb.net_tornado` - Tornado integration using `IOLoop` and `Connection` API
  - `rethinkdb.net_trio` - Trio support with `Connection` API
  - `rethinkdb.net_twisted` - Twisted integration with `inlineCallbacks` and `Connection` API
- **CLI Tools (Click-based)**:
  - `rethinkdb dump`, `export`, `import`, `index-rebuild`, `repl`, `restore`
  - All commands accept common flags/options: `-q/--quiet`, `--debug`, `-c/--connect`, `--driver-port`, `--host-name`, `-u/--user`, `-p/--password`, `--password-file`, `--tls-cert`
  - `export` supports `--format json|csv|ndjson|jsongz`, `--delimiter` (CSV), `--compression-level` (jsongz), `--fields`, `--clients`, `--read-outdated`
- **Type Hints**: Extensive typing across modules
- **Modern Build/Packaging**: `pyproject.toml`, uv, and Ruff
- **Tests**: New integration tests for each async driver: `tests/integration/test_net_{asyncio,gevent,tornado,trio,twisted}.py`
- **Documentation**: Sphinx docs under `docs/` with API, history, and vulnerabilities
- **CI/CD**: GitHub Actions matrix for Python 3.12–3.15; pre-commit hooks; Release Please for versioning; Renovate for dependency updates
- **Protocol Buffer**: `rethinkdb/ql2_pb2.py` now tracked in VCS

### Changed

API and behavior changes (including breaking changes) compared to 2.4.x:

- **Namespace and Entry Point**:
  - The `r` entry point is now a `SimpleNamespace` exposing query helpers in `rethinkdb/__init__.py`
  - A lightweight `Client` wrapper is provided to manage connection loop types via `set_loop_type()`
- **Exceptions and Error Handling**:
  - Renamed `Rql*` to `Reql*` across the codebase; removed `Rql*` aliases
  - Introduced `InvalidHandshakeStateError` for invalid handshake transitions
  - `ReqlAuthError` raises `ValueError` if only host or only port is provided; includes host:port in message if both are provided
  - `ReqlTimeoutError` now subclasses `TimeoutError` and mirrors `ReqlAuthError` host/port validation and messaging
- **QueryPrinter (errors.QueryPrinter)**:
  - Error messages now embed composed query and caret markers
- **Handshake (`rethinkdb.handshake.HandshakeV1_0`)**:
  - Uses protected attributes for credentials: `__username`, `__password`
  - Username is escaped as per RFC (`,` => `=2C`, `=` => `=3D`)
  - Accepts optional `json_encoder`/`json_decoder` (defaults to `json.JSONEncoder`/`json.JSONDecoder`)
  - Raises `ReqlAuthError` for auth failures (error_code 10..20), `ReqlDriverError` for other handshake errors
  - Raises `InvalidHandshakeStateError` on unexpected/unknown state and on invalid transitions in `next_message`
  - Validates server protocol version range and server signature per SCRAM-SHA-256
- **Encoder/Decoder (`rethinkdb.encoder`)**:
  - Introduced `ReqlEncoder` and `ReqlDecoder` classes
  - Moved pseudo-type conversions here (TIME, BINARY, GROUPED_DATA)
  - Renamed utility `recursively_make_hashable` -> `make_hashable`
- **Utilities**:
  - Moved `T` to `rethinkdb.utils.EnhancedTuple` and renamed parameter `intsp` -> `int_separator`
  - `utilities.py` renamed/moved to `utils.py`
- **AST (`rethinkdb.ast`)**:
  - Renamed internal `optargs` to `kwargs` consistently across query node implementations
  - Query operators improved for infix precedence; better error hints for misuse (e.g., `a < b | b < c`)
  - `__str__` of queries now uses `QueryPrinter(self).query`
- **Cursor/Connection (`rethinkdb.net`)**:
  - `Connection.resume(cursor)` replaces `_continue` (now public)
  - `Connection.stop(cursor)` replaces `_stop` (now public)
  - `Cursor.extend(...)` made public (from `_extend`)
  - `Cursor.raise_error(...)` replaces `_error` (now public)
  - Improved wait/timeout behavior and state tracking
- **CLI**:
  - Subcommand name standardized to `index-rebuild` (was `index_rebuild`)
  - Logging unified; progress reporting moved to logger-based updates
- **Build/Tooling**:
  - Replaced Poetry, Black, isort, flake8, and pylint with uv, Ruff, and mise
  - Supported Python versions are 3.12, 3.13, 3.14, and 3.15
  - Runtime and development dependencies raised to current releases

### Fixed

- Binary response parsing in network layer (`encoder.py`, `net.py`)
- Numerous linter and mypy findings across `ast`, `net`, and CLI modules
- Circular import issues and import path clean-ups
- Error propagation and context in async operations
- Potential memory leaks in connection handling and cursors
- Twisted extra now requires 26.10.0rc2, which includes fixes for the advisories reported against 26.4.0

### Removed

- Python 2 support dropped completely
- Legacy CLI scripts removed; replaced by Click-based `rethinkdb/cli/*`
- Deprecated modules removed:
  - `rethinkdb/backports/*` (SSL hostname matching backports)
  - `rethinkdb/logger.py`, `rethinkdb/helpers.py`, `rethinkdb/docs.py`
- Old dependencies and tooling removed (bandit, legacy CI scripts)
- Travis CI replaced with GitHub Actions
- `setup.py` and `requirements.txt` removed in favor of `pyproject.toml` and `uv.lock`
- Top-level re-export of errors via package `__all__` removed; import from `rethinkdb.errors` instead
- `auth_key` parameter removed in favor of `password` for connections
- `Rql*` exception aliases removed (use `Reql*`)

### Deprecated

- `RqlQuery.to_json_string()` is an alias for `to_json()` and will be removed in a future release

### Migration Guide

For users upgrading from 2.4.x to 2.5.0:

1. **Python**: Use Python 3.12+.
2. **Imports**: Update imports for moved modules (e.g., utilities now in `rethinkdb.utils`).
3. **Exceptions**: Replace `Rql*` with `Reql*`. Adjust error handling for `ReqlAuthError`/`ReqlTimeoutError` host/port validation.
4. **Handshake**: If customizing JSON encoding/decoding during handshake, pass `json_encoder`/`json_decoder` to `HandshakeV1_0`.
5. **CLI**: Use `index-rebuild` (hyphen), and the standardized flags listed above.
6. **Connection/Cursor**: Use public `Connection.resume`, `Connection.stop`, `Cursor.extend`, and `Cursor.raise_error`.
7. **Queries**: If relying on `optargs`, update to `kwargs`. Prefer `to_json()` over `to_json_string()`.
