# Copyright 2022 RethinkDB
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

from unittest.mock import Mock

import pytest

from rethinkdb.errors import ReqlAuthError, ReqlCursorEmpty, ReqlError, ReqlTimeoutError


def test_reql_error():
    """
    Test raising basic Reql error.
    """

    expected_message = "reql error"
    exception = ReqlError(expected_message)

    with pytest.raises(ReqlError) as exc:
        raise exception

    assert str(exc.value) == expected_message
    assert repr(exception) == f"<ReqlError instance: {str(exception)} >"


def test_reql_error_only_term_is_set():
    """
    Test that both term and frames are required to show detailed errors.
    """

    expected_message = "reql error"
    exception = ReqlError(expected_message, term=Mock())

    with pytest.raises(ReqlError) as exc:
        raise exception

    assert str(exc.value) == expected_message
    assert repr(exception) == f"<ReqlError instance: {str(exception)} >"


def test_reql_error_only_frames_are_set():
    """
    Test that both term and frames are required to show detailed errors.
    """

    expected_message = "reql error"
    exception = ReqlError(expected_message, frames=[1, 2, 3])

    with pytest.raises(ReqlError) as exc:
        raise exception

    assert str(exc.value) == expected_message
    assert repr(exception) == f"<ReqlError instance: {str(exception)} >"


def test_reql_error_terms_and_frames_are_set():
    """
    Test both term and frames are set.
    """

    inner_term = Mock(_args=[], kwargs={})
    inner_term.compose.return_value = ["^"]

    expected_term = Mock(_name="term", _args=[], kwargs={1: inner_term, 2: inner_term})
    expected_term.compose.return_value = ["composed"]

    expected_message = "reql error"
    exception = ReqlError(expected_message, term=expected_term, frames=[1, 2])

    with pytest.raises(ReqlError) as exc:
        raise exception

    assert str(exc.value) == "reql error in:\ncomposed\n "
    assert repr(exception) == f"<ReqlError instance: {str(exception)} >"


def test_auth_error():
    """
    Test auth error raised as expected without using host or port.
    """

    expected_message = "auth error"

    with pytest.raises(ReqlAuthError) as exc:
        raise ReqlAuthError(expected_message)

    assert str(exc.value) == expected_message


def test_auth_error_connection_error():
    """
    Test auth error shows connection error if host and port are set.
    """

    host = "localhost"
    port = 1234
    message = "auth error"

    with pytest.raises(ReqlAuthError) as exc:
        raise ReqlAuthError(message, host, port)

    assert str(exc.value) == f"Could not connect to {host}:{port}, {message}"


def test_auth_error_only_host():
    """
    Test auth error raises error if only host is given.
    """

    with pytest.raises(ValueError) as exc:
        raise ReqlAuthError("auth error", host="localhost")

    assert str(exc.value) == "If host is set, you must set port as well"


def test_auth_error_only_port():
    """
    Test auth error raises error if only port is given.
    """

    with pytest.raises(ValueError) as exc:
        raise ReqlAuthError("auth error", port=1234)

    assert str(exc.value) == "If port is set, you must set host as well"


def test_cursor_empty():
    with pytest.raises(ReqlCursorEmpty) as exc:
        raise ReqlCursorEmpty()

    assert str(exc.value) == "Cursor is empty."


def test_timeout_error():
    """
    Test timeout error raised as expected without using host or port.
    """

    with pytest.raises(ReqlTimeoutError) as exc:
        raise ReqlTimeoutError()

    assert str(exc.value) == "Operation timed out."


def test_timeout_error_connection_error():
    """
    Test timeout error shows connection error if host and port are set.
    """

    host = "localhost"
    port = 1234

    with pytest.raises(ReqlTimeoutError) as exc:
        raise ReqlTimeoutError(host, port)

    assert str(exc.value) == f"Could not connect to {host}:{port}, Operation timed out."


def test_timeout_error_only_host():
    """
    Test timeout error raises error if only host is given.
    """

    with pytest.raises(ValueError) as exc:
        raise ReqlTimeoutError(host="localhost")

    assert str(exc.value) == "If host is set, you must set port as well"


def test_timeout_error_only_port():
    """
    Test timeout error raises error if only port is given.
    """

    with pytest.raises(ValueError) as exc:
        raise ReqlTimeoutError(port=1234)

    assert str(exc.value) == "If port is set, you must set host as well"
