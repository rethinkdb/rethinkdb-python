import os
import sys
from collections import namedtuple
from asyncio import coroutine
import pytest
from rethinkdb import RethinkDB
from rethinkdb.errors import ReqlRuntimeError

Helper = namedtuple("Helper", "r connection")

INTEGRATION_TEST_DB = 'integration_test'


@pytest.fixture
async def rethinkdb_helper():

    r = RethinkDB()
    r.set_loop_type("asyncio")

    connection = await r.connect(os.getenv("REBIRTHDB_HOST"))

    try:
        await r.db_create(INTEGRATION_TEST_DB).run(connection)
    except ReqlRuntimeError:
        pass

    connection.use(INTEGRATION_TEST_DB)

    yield Helper(r=r, connection=connection)

    await connection.close()


@pytest.mark.integration
@pytest.mark.skipif(sys.version_info < (3, 5),
                    reason="requires python3.5 or higher")
async def test_flow(rethinkdb_helper):

    r = rethinkdb_helper.r
    connection = rethinkdb_helper.connection

    await r.table_create("marvel").run(connection)

    marvel_heroes = r.table('marvel')
    await marvel_heroes.insert({
        'id': 1,
        'name': 'Iron Man',
        'first_appearance': 'Tales of Suspense #39'
    }).run(connection)

    cursor = await marvel_heroes.run(connection)
    async for hero in cursor:
        assert hero['name'] == 'Iron Man'


@pytest.mark.integration
@pytest.mark.skipif(sys.version_info < (3, 4),
                    reason="requires python3.4")
@coroutine
def test_flow_couroutine_paradigm(rethinkdb_helper):

    r = rethinkdb_helper.r
    connection = rethinkdb_helper.connection

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
