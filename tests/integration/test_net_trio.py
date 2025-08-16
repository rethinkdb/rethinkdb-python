import os

import pytest
import trio

from rethinkdb import net, net_trio, query
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
        "test_trio_cursor_db",
        "test_trio_helpers_db",
        "test_trio_db",
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
    assert (
        create_result["dbs_created"] == 1
    ), f"Expected 1 database created, got {create_result['dbs_created']}"

    db_list = await connection.start(query.db_list())
    assert db_name in db_list, f"Database {db_name} should be in the list"

    drop_result = await query.db_drop(db_name).run(connection)
    await connection.noreply_wait()
    assert (
        drop_result["dbs_dropped"] == 1
    ), f"Expected 1 database dropped, got {drop_result['dbs_dropped']}"

    db_list_after = await connection.start(query.db_list())
    assert (
        db_name not in db_list_after
    ), f"Database {db_name} should not be in the list after dropping"

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

        assert (
            actual == scenario.expected
        ), f"Expected {scenario.expected}, got {actual}"

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
