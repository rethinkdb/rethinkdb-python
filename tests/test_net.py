# Copyright 2022 - present RethinkDB
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

from rethinkdb.handshake import HandshakeV1_0
from rethinkdb.net import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_TIMEOUT,
    DEFAULT_USER,
    make_connection,
)


@pytest.fixture
def conn_params() -> dict:
    """
    Return connection parameters
    """

    return {
        "db": "mydb",
        "handshake_version": HandshakeV1_0,
        "host": "test-host",
        "password": "secure",
        "port": 1234,
        "timeout": 20,
        "user": "myuser",
        "ssl": dict(),
    }


def test_make_connection(conn_params):
    """
    Test connecting to the database.
    """

    mock_reconnect = Mock()
    mock_connection_type = Mock()
    mock_connection_type.return_value.reconnect.return_value = mock_reconnect

    conn = make_connection(
        connection_type=mock_connection_type,
        **conn_params,
    )

    assert conn == mock_reconnect
    mock_connection_type.assert_called_once_with(
        conn_params["host"],
        conn_params["port"],
        conn_params["db"],
        conn_params["user"],
        conn_params["password"],
        conn_params["timeout"],
        conn_params["ssl"],
        conn_params["handshake_version"],
    )


def test_connect_with_db_url(conn_params):
    """
    Test connecting to the database using DB URL.
    """

    mock_reconnect = Mock()
    mock_connection_type = Mock()
    mock_connection_type.return_value.reconnect.return_value = mock_reconnect

    url = "rethinkdb://{user}:{password}@{host}:{port}/{db}?timeout={timeout}".format(
        user=conn_params["user"],
        password=conn_params["password"],
        host=conn_params["host"],
        port=conn_params["port"],
        db=conn_params["db"],
        timeout=conn_params["timeout"],
    )

    conn = make_connection(
        connection_type=mock_connection_type,
        url=url,
    )

    assert conn == mock_reconnect
    mock_connection_type.assert_called_once_with(
        conn_params["host"],
        conn_params["port"],
        conn_params["db"],
        conn_params["user"],
        conn_params["password"],
        conn_params["timeout"],
        conn_params["ssl"],
        conn_params["handshake_version"],
    )


def test_make_connection_no_host(conn_params):
    """
    Test connecting to the database using the default hostname.
    """

    mock_reconnect = Mock()
    mock_connection_type = Mock()
    mock_connection_type.return_value.reconnect.return_value = mock_reconnect

    del conn_params["host"]

    conn = make_connection(
        connection_type=mock_connection_type,
        **conn_params,
    )

    assert conn == mock_reconnect
    mock_connection_type.assert_called_once_with(
        DEFAULT_HOST,
        conn_params["port"],
        conn_params["db"],
        conn_params["user"],
        conn_params["password"],
        conn_params["timeout"],
        conn_params["ssl"],
        conn_params["handshake_version"],
    )


def test_make_connection_no_port(conn_params):
    """
    Test connecting to the database using the default port.
    """

    mock_reconnect = Mock()
    mock_connection_type = Mock()
    mock_connection_type.return_value.reconnect.return_value = mock_reconnect

    del conn_params["port"]

    conn = make_connection(
        connection_type=mock_connection_type,
        **conn_params,
    )

    assert conn == mock_reconnect
    mock_connection_type.assert_called_once_with(
        conn_params["host"],
        DEFAULT_PORT,
        conn_params["db"],
        conn_params["user"],
        conn_params["password"],
        conn_params["timeout"],
        conn_params["ssl"],
        conn_params["handshake_version"],
    )


def test_make_connection_no_user(conn_params):
    """
    Test connecting to the database using the default user.
    """

    mock_reconnect = Mock()
    mock_connection_type = Mock()
    mock_connection_type.return_value.reconnect.return_value = mock_reconnect

    del conn_params["user"]

    conn = make_connection(
        connection_type=mock_connection_type,
        **conn_params,
    )

    assert conn == mock_reconnect
    mock_connection_type.assert_called_once_with(
        conn_params["host"],
        conn_params["port"],
        conn_params["db"],
        DEFAULT_USER,
        conn_params["password"],
        conn_params["timeout"],
        conn_params["ssl"],
        conn_params["handshake_version"],
    )


def test_make_connection_no_host(conn_params):
    """
    Test connecting to the database using the default timeout.
    """

    mock_reconnect = Mock()
    mock_connection_type = Mock()
    mock_connection_type.return_value.reconnect.return_value = mock_reconnect

    del conn_params["timeout"]

    conn = make_connection(
        connection_type=mock_connection_type,
        **conn_params,
    )

    assert conn == mock_reconnect
    mock_connection_type.assert_called_once_with(
        conn_params["host"],
        conn_params["port"],
        conn_params["db"],
        conn_params["user"],
        conn_params["password"],
        DEFAULT_TIMEOUT,
        conn_params["ssl"],
        conn_params["handshake_version"],
    )


def test_make_connection_with_ssl(conn_params):
    """
    Test connecting to the database using ssl.
    """

    mock_reconnect = Mock()
    mock_connection_type = Mock()
    mock_connection_type.return_value.reconnect.return_value = mock_reconnect

    conn_params["ssl"] = {"ca_certs": "ca.cert"}

    conn = make_connection(
        connection_type=mock_connection_type,
        **conn_params,
    )

    assert conn == mock_reconnect
    mock_connection_type.assert_called_once_with(
        conn_params["host"],
        conn_params["port"],
        conn_params["db"],
        conn_params["user"],
        conn_params["password"],
        conn_params["timeout"],
        conn_params["ssl"],
        conn_params["handshake_version"],
    )
