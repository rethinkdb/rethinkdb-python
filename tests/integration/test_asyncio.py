import os
import sys
from collections import namedtuple
import pytest
from rethinkdb import r
from rethinkdb.errors import ReqlRuntimeError

Helper = namedtuple("Helper", "r connection")

INTEGRATION_TEST_DB = 'integration_test'


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.skipif(sys.version_info < (3, 6),
                    reason="requires python3.6 or higher")
async def test_flow():
    """
    Test the flow for 3.6 and up, async generators are
    not supported in 3.5.
    """

    r.set_loop_type("asyncio")

    connection = await r.connect(os.getenv("REBIRTHDB_HOST"))

    try:
        await r.db_create(INTEGRATION_TEST_DB).run(connection)
    except ReqlRuntimeError:
        pass

    connection.use(INTEGRATION_TEST_DB)

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

    await connection.close()