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

import os
import socket
import threading

import pytest
import trio

from rethinkdb import net, net_trio, query
from rethinkdb.errors import ReqlRuntimeError
from tests.helpers import Scenario


@pytest.fixture
async def conn(nursery):
    """
    Return a new connection instance.
    """
    return net.make_connection(
        net_trio.Connection,
        host=os.getenv("RDB_TEST_HOST", "localhost"),
        port=int(os.getenv("RDB_TEST_PORT", 28015)),
        nursery=nursery,
    )


@pytest.fixture(autouse=True, scope="function")
def cleanup_test_dbs():
    """Clean up any test databases that might be left over."""
    yield

    test_dbs = [
        "test_trio_between_db",
        "test_trio_cursor_cm_db",
        "test_trio_cursor_db",
        "test_trio_helpers_db",
        "test_trio_db",
        "test_trio_feed_drop_db",
        "test_trio_queries_db",
    ]

    try:
        conn_instance = net.make_connection(
            net.DefaultConnection,
            host=os.getenv("RDB_TEST_HOST", "localhost"),
            port=int(os.getenv("RDB_TEST_PORT", 28015)),
        )

        for db_name in test_dbs:
            try:
                query.db_drop(db_name).run(conn_instance)
            except Exception:
                pass

        conn_instance.close()
    except Exception:
        pass


@pytest.mark.unit
def test_trio_imports():
    """Test that trio module can be imported correctly."""
    assert net_trio is not None
    assert hasattr(net_trio, "Connection")
    assert hasattr(net_trio.Connection, "__init__")


@pytest.mark.trio
@pytest.mark.integration
async def test_trio_connection(conn):
    connection = await conn

    assert connection.is_open(), "Connection should be open after reconnect"

    result = await connection.start(query.db_list())
    assert isinstance(result, list), "db_list() should return a list"

    db_name = "test_trio_db"

    create_result = await connection.start(query.db_create(db_name))
    assert create_result["dbs_created"] == 1, (
        f"Expected 1 database created, got {create_result['dbs_created']}"
    )

    db_list = await connection.start(query.db_list())
    assert db_name in db_list, f"Database {db_name} should be in the list"

    drop_result = await query.db_drop(db_name).run(connection)
    await connection.noreply_wait()
    assert drop_result["dbs_dropped"] == 1, (
        f"Expected 1 database dropped, got {drop_result['dbs_dropped']}"
    )

    db_list_after = await connection.start(query.db_list())
    assert db_name not in db_list_after, (
        f"Database {db_name} should not be in the list after dropping"
    )

    return True


@pytest.mark.trio
@pytest.mark.integration
async def test_trio_basic_queries(conn):
    """Test basic query operations with trio."""
    connection = await conn
    assert connection.is_open(), "Connection should be open"

    db_name = "test_trio_queries_db"
    table_name = "test_table"

    await connection.start(query.db_create(db_name))

    create_table_result = await connection.start(
        query.db(db_name).table_create(table_name)
    )
    assert create_table_result["tables_created"] == 1

    tables = await connection.start(query.db(db_name).table_list())
    assert table_name in tables

    insert_result = await connection.start(
        query.db(db_name).table(table_name).insert({"id": 1, "name": "test"})
    )
    assert insert_result["inserted"] == 1

    result = await connection.start(query.db(db_name).table(table_name).get(1))
    assert result["id"] == 1
    assert result["name"] == "test"

    try:
        await connection.start(query.db_drop(db_name))
    except Exception:
        pass


@pytest.mark.trio
@pytest.mark.integration
async def test_trio_cursor_operations(conn):
    """Test cursor operations with trio."""
    connection = await conn
    assert connection.is_open(), "Connection should be open"

    db_name = "test_trio_cursor_db"
    table_name = "test_cursor_table"

    try:
        await connection.start(query.db_drop(db_name))
    except Exception:
        pass

    await connection.start(query.db_create(db_name))
    await connection.start(query.db(db_name).table_create(table_name))

    docs = [{"id": i, "value": f"item_{i}"} for i in range(5)]
    insert_result = await connection.start(
        query.db(db_name).table(table_name).insert(docs)
    )
    assert insert_result["inserted"] == 5

    cursor = await connection.start(query.db(db_name).table(table_name).order_by("id"))

    results = []
    if hasattr(cursor, "__aiter__"):
        async for item in cursor:
            results.append(item)
    elif hasattr(cursor, "__iter__"):
        results = list(cursor)
    else:
        try:
            while await cursor.fetch_next():
                item = await cursor.next()
                results.append(item)
        except Exception:
            pass

    assert len(results) == 5
    assert results[0]["id"] == 0
    assert results[4]["id"] == 4

    try:
        await connection.start(query.db_drop(db_name))
    except Exception:
        pass


@pytest.mark.trio
@pytest.mark.integration
async def test_trio_between_secondary_index(conn):
    """A secondary-index between query returns every row and the connection closes."""
    connection = await conn
    assert connection.is_open(), "Connection should be open"

    db_name = "test_trio_between_db"
    table_name = "station"

    try:
        await connection.start(query.db_drop(db_name))
    except Exception:
        pass

    await connection.start(query.db_create(db_name))
    await connection.start(query.db(db_name).table_create(table_name))
    table = query.db(db_name).table(table_name)
    docs = [{"id": i, "n": i} for i in range(5)]
    inserted = await connection.start(table.insert(docs))
    assert inserted["inserted"] == 5
    await connection.start(table.index_create("n"))
    await connection.start(table.index_wait("n"))

    cursor = await connection.start(
        table.between(0, 4, index="n", right_bound="closed")
    )
    results = [item async for item in cursor]
    assert sorted(row["id"] for row in results) == [0, 1, 2, 3, 4]

    await connection.start(query.db_drop(db_name))
    await connection.close()
    assert connection.is_open() is False


@pytest.mark.trio
@pytest.mark.integration
async def test_trio_error_handling(conn):
    """Test error handling with trio."""
    connection = await conn
    assert connection.is_open(), "Connection should be open"

    with pytest.raises(Exception):
        await connection.start(query.table("nonexistent_table"))

    result = await connection.start(query.db_list())
    assert isinstance(result, list)


@pytest.mark.trio
@pytest.mark.integration
async def test_trio_reconnect(conn):
    """Test reconnection functionality with trio."""
    connection = await conn
    assert connection.is_open(), "Connection should be open"

    await connection.reconnect()
    assert connection.is_open(), "Connection should be open after reconnect"

    result = await connection.start(query.db_list())
    assert isinstance(result, list)


@pytest.mark.trio
async def assert_test_table_async(query_func, conn, scenarios):
    """Async version of assert_test_table for trio connections."""
    for scenario in scenarios:
        result = await conn.start(query_func(*scenario.args))

        if hasattr(result, "__getitem__"):
            actual = result[scenario.expected_field]
        else:
            actual = result

        assert actual == scenario.expected, (
            f"Expected {scenario.expected}, got {actual}"
        )

        if scenario.callback:
            await scenario.callback()

    return True


@pytest.mark.trio
async def drop_db_async(db_name, conn):
    """Async version of drop_db for trio connections."""
    from rethinkdb import r

    result = await conn.start(r.db_drop(db_name))
    return result


@pytest.mark.trio
@pytest.mark.integration
async def test_trio_with_test_helpers(conn):
    """Test using the test helper functions with trio."""
    connection = await conn
    assert connection.is_open(), "Connection should be open"

    db_name = "test_trio_helpers_db"
    table_name = "test_helpers_table"

    try:
        await connection.start(query.db_drop(db_name))
    except Exception:
        pass

    await connection.start(query.db_create(db_name))
    await connection.start(query.db(db_name).table_create(table_name))

    scenarios = [
        Scenario(
            name="insert data",
            args=[query.db(db_name).table(table_name).insert({"test": "value"})],
            expected=1,
            expected_field="inserted",
        ),
        Scenario(
            name="count rows",
            args=[query.db(db_name).table(table_name).count()],
            expected=1,
            expected_field=None,
        ),
    ]

    await assert_test_table_async(lambda *args: args[0], connection, scenarios)
    await drop_db_async(db_name, connection)


@pytest.mark.trio
@pytest.mark.integration
async def test_trio_connection_context_manager(conn):
    """async with closes the Trio connection."""
    connection = await conn
    async with connection:
        assert connection.is_open()
        result = await connection.start(query.db_list())
        assert isinstance(result, list)

    assert connection.is_open() is False


@pytest.mark.trio
@pytest.mark.integration
async def test_trio_cursor_context_manager(conn):
    """async with closes the Trio cursor, and a second close is safe."""
    connection = await conn
    db_name = "test_trio_cursor_cm_db"
    table_name = "test_cursor_cm_table"
    try:
        try:
            await connection.start(query.db_drop(db_name))
        except Exception:
            pass

        await connection.start(query.db_create(db_name))
        await connection.start(query.db(db_name).table_create(table_name))
        inserted = await connection.start(
            query.db(db_name).table(table_name).insert([{"id": i} for i in range(5)])
        )
        assert inserted["inserted"] == 5

        cursor = await connection.start(query.db(db_name).table(table_name))
        async with cursor:
            async for _row in cursor:
                break
        assert cursor.error is not None
        await cursor.close()
    finally:
        try:
            await connection.start(query.db_drop(db_name))
        except Exception:
            pass


class _FinProxy:
    """Forward one TCP connection, then half-close the client side."""

    def __init__(self, upstream_host: str, upstream_port: int) -> None:
        self._upstream_host = upstream_host
        self._upstream_port = upstream_port
        self._ready = threading.Event()
        self._stop_pump = threading.Event()
        self._server: socket.socket | None = None
        self._client: socket.socket | None = None
        self._upstream: socket.socket | None = None
        self.port = 0
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> int:
        """Listen, and return the local port once it is accepting."""
        self._thread.start()
        assert self._ready.wait(5)
        return self.port

    def fin(self) -> None:
        """Stop forwarding and send a FIN to the driver, leaving the socket open."""
        self._stop_pump.set()
        if self._client is None:
            return
        try:
            self._client.shutdown(socket.SHUT_WR)
        except OSError:
            pass

    def close(self) -> None:
        """Close the proxy sockets."""
        self._stop_pump.set()
        for sock in (self._client, self._upstream, self._server):
            if sock is None:
                continue
            try:
                sock.close()
            except OSError:
                pass

    def _run(self) -> None:
        server = socket.socket()
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        self._server = server
        self.port = server.getsockname()[1]
        self._ready.set()
        client, _addr = server.accept()
        self._client = client
        upstream = socket.create_connection((self._upstream_host, self._upstream_port))
        self._upstream = upstream

        def pump(src: socket.socket, dst: socket.socket) -> None:
            try:
                while not self._stop_pump.is_set():
                    src.settimeout(0.2)
                    try:
                        data = src.recv(65536)
                    except TimeoutError:
                        continue
                    if not data:
                        break
                    dst.sendall(data)
            except OSError:
                return

        threading.Thread(target=pump, args=(client, upstream), daemon=True).start()
        threading.Thread(target=pump, args=(upstream, client), daemon=True).start()
        self._stop_pump.wait()


def _sync_run(host: str, port: int, term: object) -> None:
    connection = net.make_connection(net.DefaultConnection, host=host, port=port)
    try:
        term.run(connection)  # type: ignore[attr-defined]
    finally:
        connection.close()


@pytest.mark.trio
@pytest.mark.integration
async def test_trio_changefeed_peer_fin_raises(nursery):
    """A clean TCP close while waiting for changes raises and the context exits."""
    host = os.getenv("RDB_TEST_HOST", "localhost")
    port = int(os.getenv("RDB_TEST_PORT", 28015))
    db_name = "test_trio_feed_drop_db"
    table_name = "feed"
    try:
        _sync_run(host, port, query.db_drop(db_name))
    except Exception:
        pass
    _sync_run(host, port, query.db_create(db_name))
    proxy = None
    try:
        _sync_run(host, port, query.db(db_name).table_create(table_name))
        proxy = _FinProxy(host, port)
        proxy_port = proxy.start()

        def insert_row() -> None:
            _sync_run(
                host,
                port,
                query.db(db_name).table(table_name).insert({"n": 1}),
            )

        with trio.fail_after(8):
            connection = await net.make_connection(
                net_trio.Connection,
                host="127.0.0.1",
                port=proxy_port,
                db=db_name,
                nursery=nursery,
            )

            async def watch_feed() -> None:
                async with connection:
                    cursor = (
                        await query.db(db_name)
                        .table(table_name)
                        .changes()
                        .run(connection)
                    )
                    threading.Thread(target=insert_row, daemon=True).start()
                    saw_change = False
                    async for _change in cursor:
                        assert saw_change is False
                        saw_change = True
                        proxy.fin()

            with pytest.raises(ReqlRuntimeError, match="Connection is closed"):
                await watch_feed()
            assert connection.is_open() is False
    finally:
        if proxy is not None:
            proxy.close()
        try:
            _sync_run(host, port, query.db_drop(db_name))
        except Exception:
            pass
