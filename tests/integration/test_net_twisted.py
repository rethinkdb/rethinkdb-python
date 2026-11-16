import os

import pytest
from twisted.internet import defer

from rethinkdb import net, net_twisted, query


@pytest.fixture
def conn():
    """
    Return a new connection instance.
    """
    return net.make_connection(
        net_twisted.Connection,
        host=os.getenv("RDB_TEST_HOST", "localhost"),
        port=int(os.getenv("RDB_TEST_PORT", 28015)),
    )


@pytest.fixture(autouse=True, scope="function")
def cleanup_test_dbs():
    """Clean up any test databases that might be left over."""
    yield

    test_dbs = [
        "test_twisted_between_db",
        "test_twisted_cursor_cm_db",
        "test_twisted_cursor_db",
        "test_twisted_helpers_db",
        "test_twisted_db",
        "test_twisted_queries_db",
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


@pytest.mark.twisted
@pytest.mark.unit
def test_twisted_imports():
    """Test that twisted module can be imported correctly."""
    assert net_twisted is not None
    assert hasattr(net_twisted, "Connection")
    assert hasattr(net_twisted.Connection, "__init__")


@pytest.mark.twisted
@pytest.mark.integration
def test_twisted_connection(conn):
    """Test basic connection functionality with Twisted."""

    @defer.inlineCallbacks
    def run_test():
        connection = yield conn

        assert connection.is_open(), "Connection should be open"

        result = yield connection.start(query.db_list())
        assert isinstance(result, list), "db_list() should return a list"

        db_name = "test_twisted_db"

        create_result = yield connection.start(query.db_create(db_name))
        assert create_result["dbs_created"] == 1, (
            f"Expected 1 database created, got {create_result['dbs_created']}"
        )

        db_list = yield connection.start(query.db_list())
        assert db_name in db_list, f"Database {db_name} should be in the list"

        drop_result = yield connection.start(query.db_drop(db_name))
        assert drop_result["dbs_dropped"] == 1, (
            f"Expected 1 database dropped, got {drop_result['dbs_dropped']}"
        )

        db_list_after = yield connection.start(query.db_list())
        assert db_name not in db_list_after, (
            f"Database {db_name} should not be in the list after dropping"
        )

        return True

    return run_test()


@pytest.mark.twisted
@pytest.mark.integration
def test_twisted_basic_queries(conn):
    """Test basic query operations with Twisted."""

    @defer.inlineCallbacks
    def run_test():
        connection = yield conn
        assert connection.is_open(), "Connection should be open"

        db_name = "test_twisted_queries_db"
        table_name = "test_table"

        yield connection.start(query.db_create(db_name))

        create_table_result = yield connection.start(
            query.db(db_name).table_create(table_name)
        )
        assert create_table_result["tables_created"] == 1

        tables = yield connection.start(query.db(db_name).table_list())
        assert table_name in tables

        insert_result = yield connection.start(
            query.db(db_name).table(table_name).insert({"id": 1, "name": "test"})
        )
        assert insert_result["inserted"] == 1

        result = yield connection.start(query.db(db_name).table(table_name).get(1))
        assert result["id"] == 1
        assert result["name"] == "test"

        try:
            yield connection.start(query.db_drop(db_name))
        except Exception:
            pass

    return run_test()


@pytest.mark.twisted
@pytest.mark.integration
def test_twisted_cursor_operations(conn):
    """Test cursor operations with Twisted."""

    @defer.inlineCallbacks
    def run_test():
        connection = yield conn
        assert connection.is_open(), "Connection should be open"

        db_name = "test_twisted_cursor_db"
        table_name = "test_cursor_table"

        try:
            yield connection.start(query.db_drop(db_name))
        except Exception:
            pass

        yield connection.start(query.db_create(db_name))
        yield connection.start(query.db(db_name).table_create(table_name))

        docs = [{"id": i, "value": f"item_{i}"} for i in range(5)]
        insert_result = yield connection.start(
            query.db(db_name).table(table_name).insert(docs)
        )
        assert insert_result["inserted"] == 5

        cursor = yield connection.start(
            query.db(db_name).table(table_name).order_by("id")
        )

        results = []
        if hasattr(cursor, "__iter__"):
            for item in cursor:
                results.append(item)
        else:
            try:
                while True:
                    has_next = yield cursor.fetch_next()
                    if not has_next:
                        break
                    item = yield cursor.next()
                    results.append(item)
            except Exception:
                pass

        assert len(results) == 5
        assert results[0]["id"] == 0
        assert results[4]["id"] == 4

        try:
            yield connection.start(query.db_drop(db_name))
        except Exception:
            pass

    return run_test()


@pytest.mark.twisted
@pytest.mark.integration
def test_twisted_between_secondary_index(conn):
    """A secondary-index between query returns every row and the connection closes."""

    @defer.inlineCallbacks
    def run_test():
        connection = yield conn
        assert connection.is_open(), "Connection should be open"

        db_name = "test_twisted_between_db"
        table_name = "station"

        try:
            yield connection.start(query.db_drop(db_name))
        except Exception:
            pass

        yield connection.start(query.db_create(db_name))
        yield connection.start(query.db(db_name).table_create(table_name))
        table = query.db(db_name).table(table_name)
        docs = [{"id": i, "n": i} for i in range(5)]
        inserted = yield connection.start(table.insert(docs))
        assert inserted["inserted"] == 5
        yield connection.start(table.index_create("n"))
        yield connection.start(table.index_wait("n"))

        cursor = yield connection.start(
            table.between(0, 4, index="n", right_bound="closed")
        )
        results = []
        error = None
        try:
            while True:
                has_next = yield cursor.fetch_next()
                if not has_next:
                    break
                item = yield cursor.next()
                results.append(item)
        except Exception as exc:
            error = exc

        assert sorted(row["id"] for row in results) == [0, 1, 2, 3, 4], error
        yield connection.start(query.db_drop(db_name))
        yield connection.close()
        assert connection.is_open() is False

    return run_test()


@pytest.mark.twisted
@pytest.mark.integration
def test_twisted_error_handling(conn):
    """Test error handling with Twisted."""

    @defer.inlineCallbacks
    def run_test():
        connection = yield conn
        assert connection.is_open(), "Connection should be open"

        with pytest.raises(Exception):
            yield connection.start(query.table("nonexistent_table"))

        result = yield connection.start(query.db_list())
        assert isinstance(result, list)

    return run_test()


@pytest.mark.twisted
@pytest.mark.integration
def test_twisted_reconnect(conn):
    """Test reconnection functionality with Twisted."""

    @defer.inlineCallbacks
    def run_test():
        connection = yield conn
        assert connection.is_open(), "Connection should be open"

        yield connection.reconnect()
        assert connection.is_open(), "Connection should be open after reconnect"

        result = yield connection.start(query.db_list())
        assert isinstance(result, list)

    return run_test()


@pytest.mark.twisted
@pytest.mark.integration
def test_twisted_database_and_table_operations(conn):
    """Test database and table operations with Twisted."""

    @defer.inlineCallbacks
    def run_test():
        connection = yield conn
        assert connection.is_open(), "Connection should be open"

        db_name = "test_twisted_helpers_db"
        table_name = "test_helpers_table"

        try:
            yield connection.start(query.db_drop(db_name))
        except Exception:
            pass

        yield connection.start(query.db_create(db_name))
        yield connection.start(query.db(db_name).table_create(table_name))

        insert_result = yield connection.start(
            query.db(db_name).table(table_name).insert({"test": "value"})
        )
        assert insert_result["inserted"] == 1

        count_result = yield connection.start(
            query.db(db_name).table(table_name).count()
        )
        assert count_result == 1

        yield connection.start(query.db_drop(db_name))

    return run_test()


@pytest.mark.twisted
@pytest.mark.integration
def test_twisted_connection_context_manager(conn):
    """async with closes the Twisted connection."""

    async def run_test():
        connection = await conn
        async with connection:
            assert connection.is_open()
            result = await connection.start(query.db_list())
            assert isinstance(result, list)
        assert connection.is_open() is False

    return defer.ensureDeferred(run_test())


@pytest.mark.twisted
@pytest.mark.integration
def test_twisted_cursor_context_manager(conn):
    """async with closes the Twisted cursor, and a second close is safe."""

    async def run_test():
        connection = await conn
        db_name = "test_twisted_cursor_cm_db"
        table_name = "test_cursor_cm_table"
        try:
            try:
                await connection.start(query.db_drop(db_name))
            except Exception:
                pass

            await connection.start(query.db_create(db_name))
            await connection.start(query.db(db_name).table_create(table_name))
            inserted = await connection.start(
                query.db(db_name)
                .table(table_name)
                .insert([{"id": i} for i in range(5)])
            )
            assert inserted["inserted"] == 5

            cursor = await connection.start(query.db(db_name).table(table_name))
            async with cursor:
                await cursor.next()
            assert cursor.error is not None
            await cursor.close()
        finally:
            try:
                await connection.start(query.db_drop(db_name))
            except Exception:
                pass

    return defer.ensureDeferred(run_test())
