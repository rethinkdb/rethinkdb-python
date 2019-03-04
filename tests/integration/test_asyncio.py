import pytest
import os
from rethinkdb import RethinkDB
from rethinkdb.errors import ReqlRuntimeError
from collections import namedtuple

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
async def test_flow(rethinkdb_helper):

    r: RethinkDB = rethinkdb_helper.r
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
