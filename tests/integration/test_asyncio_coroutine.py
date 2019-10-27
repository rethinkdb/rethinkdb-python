import os
import sys
from asyncio import coroutine
import pytest
from rethinkdb import r
from rethinkdb.errors import ReqlRuntimeError


INTEGRATION_TEST_DB = 'integration_test'


@pytest.mark.integration
@pytest.mark.skipif(sys.version_info == (3, 4) or sys.version_info == (3, 5),
                    reason="requires python3.4 or python3.5")
@coroutine
def test_flow_couroutine_paradigm():

    r.set_loop_type("asyncio")

    connection = yield from r.connect(os.getenv("RETHINKDB_HOST"))

    try:
        yield from r.db_create(INTEGRATION_TEST_DB).run(connection)
    except ReqlRuntimeError:
        pass

    connection.use(INTEGRATION_TEST_DB)

    yield from r.table_create("marvel").run(connection)

    marvel_heroes = r.table('marvel')
    yield from marvel_heroes.insert({
        'id': 1,
        'name': 'Iron Man',
        'first_appearance': 'Tales of Suspense #39'
    }).run(connection)

    cursor = yield from marvel_heroes.run(connection)

    while (yield from cursor.fetch_next()):
        hero = yield from cursor.__anext__()
        assert hero['name'] == 'Iron Man'

    yield from connection.close()
