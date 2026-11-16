# Copyright 2016-2026 RethinkDB
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
        "ssl": {},
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


def test_make_connection_no_timeout(conn_params):
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


def _assert_sync_context_manager(cls: type) -> None:
    """The class supports ``with`` and not ``async with``."""
    assert callable(getattr(cls, "__enter__", None))
    assert callable(getattr(cls, "__exit__", None))
    assert getattr(cls, "__aenter__", None) is None
    assert getattr(cls, "__aexit__", None) is None


def _assert_async_context_manager(cls: type) -> None:
    """The class supports ``async with`` and not ``with``."""
    assert callable(getattr(cls, "__aenter__", None))
    assert callable(getattr(cls, "__aexit__", None))
    assert getattr(cls, "__enter__", None) is None
    assert getattr(cls, "__exit__", None) is None


def test_sync_context_manager_closes_and_propagates():
    """A synchronous context manager closes on exit and does not swallow errors."""
    from rethinkdb.net import SyncConnectionContextManager, SyncContextManager

    class Cursor(SyncContextManager):
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    cursor = Cursor()
    with cursor:
        assert cursor.closed is False
    assert cursor.closed is True

    cursor.closed = False
    with pytest.raises(RuntimeError, match="boom"), cursor:
        raise RuntimeError("boom")
    assert cursor.closed is True

    class Conn(SyncConnectionContextManager):
        def __init__(self) -> None:
            self.noreply_wait: bool | None = None

        def close(self, noreply_wait: bool = True) -> None:
            self.noreply_wait = noreply_wait

    conn = Conn()
    with pytest.raises(RuntimeError, match="boom"), conn:
        raise RuntimeError("boom")
    assert conn.noreply_wait is False


@pytest.mark.asyncio
async def test_async_context_manager_closes_and_propagates():
    """An asynchronous context manager awaits close and does not swallow errors."""
    from rethinkdb.net import AsyncConnectionContextManager, AsyncContextManager

    class Cursor(AsyncContextManager):
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    cursor = Cursor()
    async with cursor:
        assert cursor.closed is False
    assert cursor.closed is True

    cursor.closed = False
    with pytest.raises(RuntimeError, match="boom"):
        async with cursor:
            raise RuntimeError("boom")
    assert cursor.closed is True

    class Conn(AsyncConnectionContextManager):
        def __init__(self) -> None:
            self.noreply_wait: bool | None = None

        async def close(self, noreply_wait: bool = True) -> None:
            self.noreply_wait = noreply_wait

    conn = Conn()
    async with conn:
        pass
    assert conn.noreply_wait is False

    conn.noreply_wait = None
    with pytest.raises(RuntimeError, match="boom"):
        async with conn:
            raise RuntimeError("boom")
    assert conn.noreply_wait is False


def test_driver_context_manager_protocols():
    """Each driver exposes only the context manager its close method can honor."""
    from rethinkdb.net import DefaultConnection, DefaultCursor
    from rethinkdb.net_asyncio import AsyncioCursor
    from rethinkdb.net_asyncio import Connection as AsyncioConnection
    from rethinkdb.net_gevent import Connection as GeventConnection
    from rethinkdb.net_gevent import GeventCursor
    from rethinkdb.net_tornado import Connection as TornadoConnection
    from rethinkdb.net_tornado import TornadoCursor
    from rethinkdb.net_trio import Connection as TrioConnection
    from rethinkdb.net_trio import TrioCursor
    from rethinkdb.net_twisted import Connection as TwistedConnection
    from rethinkdb.net_twisted import TwistedCursor

    for cls in (DefaultCursor, DefaultConnection, GeventCursor, GeventConnection):
        _assert_sync_context_manager(cls)

    for cls in (
        AsyncioCursor,
        AsyncioConnection,
        TornadoCursor,
        TornadoConnection,
        TrioCursor,
        TrioConnection,
        TwistedCursor,
        TwistedConnection,
    ):
        _assert_async_context_manager(cls)
