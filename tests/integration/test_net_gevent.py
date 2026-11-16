import os

import pytest

from rethinkdb import net, net_gevent, query
from tests.helpers import Scenario


@pytest.fixture
def conn():
    """
    Return a new connection instance.
    """
    return net.make_connection(
        net_gevent.Connection,
        host=os.getenv("RDB_TEST_HOST", "localhost"),
        port=int(os.getenv("RDB_TEST_PORT", 28015)),
    )


@pytest.fixture(autouse=True, scope="function")
def cleanup_test_dbs():
    """Clean up any test databases that might be left over."""
    yield

    test_dbs = [
        "test_gevent_between_db",
        "test_gevent_cursor_cm_db",
        "test_gevent_cursor_db",
        "test_gevent_helpers_db",
        "test_gevent_db",
        "test_gevent_queries_db",
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


@pytest.mark.gevent
@pytest.mark.unit
def test_gevent_imports():
    """Test that gevent module can be imported correctly."""
    assert net_gevent is not None
    assert hasattr(net_gevent, "Connection")
    assert hasattr(net_gevent.Connection, "__init__")


@pytest.mark.gevent
@pytest.mark.integration
def test_gevent_connection(conn):
    """Test basic connection functionality with Gevent."""
    connection = conn

    assert connection.is_open(), "Connection should be open"

    result = connection.start(query.db_list())
    assert isinstance(result, list), "db_list() should return a list"

    db_name = "test_gevent_db"

    create_result = connection.start(query.db_create(db_name))
    assert create_result["dbs_created"] == 1, (
        f"Expected 1 database created, got {create_result['dbs_created']}"
    )

    db_list = connection.start(query.db_list())
    assert db_name in db_list, f"Database {db_name} should be in the list"

    drop_result = connection.start(query.db_drop(db_name))
    assert drop_result["dbs_dropped"] == 1, (
        f"Expected 1 database dropped, got {drop_result['dbs_dropped']}"
    )

    db_list_after = connection.start(query.db_list())
    assert db_name not in db_list_after, (
        f"Database {db_name} should not be in the list after dropping"
    )

    return True


@pytest.mark.gevent
@pytest.mark.integration
def test_gevent_basic_queries(conn):
    """Test basic query operations with Gevent."""
    connection = conn
    assert connection.is_open(), "Connection should be open"

    db_name = "test_gevent_queries_db"
    table_name = "test_table"

    connection.start(query.db_create(db_name))

    create_table_result = connection.start(query.db(db_name).table_create(table_name))
    assert create_table_result["tables_created"] == 1

    tables = connection.start(query.db(db_name).table_list())
    assert table_name in tables

    insert_result = connection.start(
        query.db(db_name).table(table_name).insert({"id": 1, "name": "test"})
    )
    assert insert_result["inserted"] == 1

    result = connection.start(query.db(db_name).table(table_name).get(1))
    assert result["id"] == 1
    assert result["name"] == "test"

    try:
        connection.start(query.db_drop(db_name))
    except Exception:
        pass


@pytest.mark.gevent
@pytest.mark.integration
def test_gevent_cursor_operations(conn):
    """Test cursor operations with Gevent."""
    connection = conn
    assert connection.is_open(), "Connection should be open"

    db_name = "test_gevent_cursor_db"
    table_name = "test_cursor_table"

    try:
        connection.start(query.db_drop(db_name))
    except Exception:
        pass

    connection.start(query.db_create(db_name))
    connection.start(query.db(db_name).table_create(table_name))

    docs = [{"id": i, "value": f"item_{i}"} for i in range(5)]
    insert_result = connection.start(query.db(db_name).table(table_name).insert(docs))
    assert insert_result["inserted"] == 5

    cursor = connection.start(query.db(db_name).table(table_name).order_by("id"))

    results = []
    if hasattr(cursor, "__iter__"):
        for item in cursor:
            results.append(item)
    else:
        try:
            while cursor.fetch_next():
                item = cursor.next()
                results.append(item)
        except Exception:
            pass

    assert len(results) == 5
    assert results[0]["id"] == 0
    assert results[4]["id"] == 4

    try:
        connection.start(query.db_drop(db_name))
    except Exception:
        pass


@pytest.mark.gevent
@pytest.mark.integration
def test_gevent_between_secondary_index(conn):
    """A secondary-index between query returns every row and the connection closes."""
    connection = conn
    assert connection.is_open(), "Connection should be open"

    db_name = "test_gevent_between_db"
    table_name = "station"

    try:
        connection.start(query.db_drop(db_name))
    except Exception:
        pass

    connection.start(query.db_create(db_name))
    connection.start(query.db(db_name).table_create(table_name))
    table = query.db(db_name).table(table_name)
    docs = [{"id": i, "n": i} for i in range(5)]
    inserted = connection.start(table.insert(docs))
    assert inserted["inserted"] == 5
    connection.start(table.index_create("n"))
    connection.start(table.index_wait("n"))

    cursor = connection.start(table.between(0, 4, index="n", right_bound="closed"))
    results = list(cursor)
    assert sorted(row["id"] for row in results) == [0, 1, 2, 3, 4]

    connection.start(query.db_drop(db_name))
    connection.close()
    assert connection.is_open() is False


@pytest.mark.gevent
@pytest.mark.integration
def test_gevent_error_handling(conn):
    """Test error handling with Gevent."""
    connection = conn
    assert connection.is_open(), "Connection should be open"

    with pytest.raises(Exception):
        connection.start(query.table("nonexistent_table"))

    result = connection.start(query.db_list())
    assert isinstance(result, list)


@pytest.mark.gevent
@pytest.mark.integration
def test_gevent_reconnect(conn):
    """Test reconnection functionality with Gevent."""
    connection = conn
    assert connection.is_open(), "Connection should be open"

    connection.reconnect()
    assert connection.is_open(), "Connection should be open after reconnect"

    result = connection.start(query.db_list())
    assert isinstance(result, list)


def assert_test_table_sync(query_func, conn, scenarios):
    """Sync version of assert_test_table for Gevent connections."""
    for scenario in scenarios:
        result = conn.start(query_func(*scenario.args))

        if hasattr(result, "__getitem__"):
            actual = result[scenario.expected_field]
        else:
            actual = result

        assert actual == scenario.expected, (
            f"Expected {scenario.expected}, got {actual}"
        )

        if scenario.callback:
            scenario.callback()

    return True


def drop_db_sync(db_name, conn):
    """Sync version of drop_db for Gevent connections."""
    from rethinkdb import r

    result = conn.start(r.db_drop(db_name))
    return result


@pytest.mark.gevent
@pytest.mark.integration
def test_gevent_with_test_helpers(conn):
    """Test using the test helper functions with Gevent."""
    connection = conn
    assert connection.is_open(), "Connection should be open"

    db_name = "test_gevent_helpers_db"
    table_name = "test_helpers_table"

    try:
        connection.start(query.db_drop(db_name))
    except Exception:
        pass

    connection.start(query.db_create(db_name))
    connection.start(query.db(db_name).table_create(table_name))

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

    assert_test_table_sync(lambda *args: args[0], connection, scenarios)
    drop_db_sync(db_name, connection)


@pytest.mark.gevent
@pytest.mark.integration
def test_gevent_concurrent_queries(conn):
    """Test concurrent queries with Gevent using greenlets."""
    connection = conn
    assert connection.is_open(), "Connection should be open"

    db_name = "test_gevent_concurrent_db"
    table_name = "test_concurrent_table"

    try:
        connection.start(query.db_drop(db_name))
    except Exception:
        pass

    connection.start(query.db_create(db_name))
    connection.start(query.db(db_name).table_create(table_name))

    docs = [{"id": i, "value": f"concurrent_{i}"} for i in range(10)]
    connection.start(query.db(db_name).table(table_name).insert(docs))

    def run_query(query_id):
        return connection.start(query.db(db_name).table(table_name).get(query_id))

    import gevent  # type: ignore

    greenlets = [gevent.spawn(run_query, i) for i in range(5)]
    gevent.joinall(greenlets)

    results = [g.value for g in greenlets]

    assert len(results) == 5
    for i, result in enumerate(results):
        assert result["id"] == i
        assert result["value"] == f"concurrent_{i}"

    try:
        connection.start(query.db_drop(db_name))
    except Exception:
        pass


@pytest.mark.gevent
@pytest.mark.integration
def test_gevent_database_and_table_operations(conn):
    """Test basic database and table operations with Gevent."""
    connection = conn
    assert connection.is_open(), "Connection should be open"

    db_name = "test_gevent_ops_db"
    table_name = "test_ops_table"

    try:
        connection.start(query.db_drop(db_name))
    except Exception:
        pass

    create_result = connection.start(query.db_create(db_name))
    assert create_result["dbs_created"] == 1

    db_list = connection.start(query.db_list())
    assert db_name in db_list

    table_create_result = connection.start(query.db(db_name).table_create(table_name))
    assert table_create_result["tables_created"] == 1

    table_list = connection.start(query.db(db_name).table_list())
    assert table_name in table_list

    insert_result = connection.start(
        query.db(db_name)
        .table(table_name)
        .insert({"id": 1, "name": "Alice", "age": 30})
    )
    assert insert_result["inserted"] == 1

    alice = connection.start(query.db(db_name).table(table_name).get(1))
    assert alice["name"] == "Alice"
    assert alice["age"] == 30

    count = connection.start(query.db(db_name).table(table_name).count())
    assert count == 1

    try:
        connection.start(query.db_drop(db_name))
    except Exception:
        pass


@pytest.mark.gevent
@pytest.mark.integration
def test_gevent_connection_context_manager(conn):
    """with closes the gevent connection."""
    connection = conn
    assert connection.is_open()
    with connection:
        result = connection.start(query.db_list())
        assert isinstance(result, list)

    assert connection.is_open() is False


@pytest.mark.gevent
@pytest.mark.integration
def test_gevent_cursor_context_manager(conn):
    """with closes the gevent cursor, and a second close is safe."""
    connection = conn
    db_name = "test_gevent_cursor_cm_db"
    table_name = "test_cursor_cm_table"
    try:
        try:
            connection.start(query.db_drop(db_name))
        except Exception:
            pass

        connection.start(query.db_create(db_name))
        connection.start(query.db(db_name).table_create(table_name))
        inserted = connection.start(
            query.db(db_name).table(table_name).insert([{"id": i} for i in range(5)])
        )
        assert inserted["inserted"] == 5

        cursor = connection.start(query.db(db_name).table(table_name))
        with cursor:
            next(cursor)
        assert cursor.error is not None
        cursor.close()
    finally:
        try:
            connection.start(query.db_drop(db_name))
        except Exception:
            pass
