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

Added
~~~~~

* `ValueError` raised by `ReqlTimeoutError` and `ReqlAuthError` if only host or port set
* New error type for invalid handshake state: `InvalidHandshakeStateError`

Changed
~~~~~~~

* QueryPrinter's `print_query` became a property and renamed to `query`
* QueryPrinter's `print_carrots` became a property and renamed to `carrots`
* Renamed `ReqlAvailabilityError` to `ReqlOperationError`
* Extract REPL helper class to a separate file
* `HandshakeV1_0` is waiting `bytes` for `username` and `password` attributes instead of `str`
* `HandshakeV1_0` defines `username` and `password` attributes as protected attributes
* `HandshakeV1_0` has a hardcoded `JSONEncoder` and `JSONDecoder` from now on
* `HandshakeV1_0` raises `InvalidHandshakeStateError` when an unrecognized state called in `next_message`
* Moved `ReQLEncoder`, `ReQLDecoder`, `recursively_make_hashable` to `encoder` module
* Moved `T` to `utilities` to module and renamed to `EnhancedTuple`
* Renamed `EnhancedTuple`/`T`'s `intsp` parameter to `int_separator`
* Renamed `recursively_make_hashable` to `make_hashable`
* Renamed `optargs` to `kwargs` in `ast` module
* Renamed internal `_continue` method of connection to `resume` to make it public
* Internal `_stop`, `_continue` methods of `Connection` became public
* Renamed internal `_error` to `raise_error`
* Internal `_extend`, `_error` of `Cursor` became public
* Renamed `Rql*` to `Reql*`

Fixed
~~~~~

* Fixed a potential "no-member" error of `RqlBoolOperatorQuery`
* Fixed variety of quality issues in `ast` module

Removed
~~~~~~~

* Errors are not re-exported as `__all__` for `rethinkdb`
* Removed `Rql*` aliases for `Reql*` exceptions
* Removed `auth_key` in favor of `password` in connection params

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
