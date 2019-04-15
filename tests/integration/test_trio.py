from collections import namedtuple
import os
import sys

from async_generator import async_generator, yield_
import pytest
from rethinkdb import RethinkDB
from rethinkdb.errors import ReqlRuntimeError
import trio


INTEGRATION_TEST_DB = 'integration_test'
r = RethinkDB()
r.set_loop_type('trio')


@pytest.fixture
@async_generator
async def integration_db(nursery):
    async with r.open(db='test', nursery=nursery) as conn:
        try:
            await r.db_create(INTEGRATION_TEST_DB).run(conn)
        except ReqlRuntimeError:
            pass
    await yield_(r.db(INTEGRATION_TEST_DB))


@pytest.fixture
@async_generator
async def marvel_table(integration_db, nursery):
    async with r.open(db='test', nursery=nursery) as conn:
        await r.table_create('marvel').run(conn)
        await yield_(r.table('marvel'))
        await r.table_drop('marvel').run(conn)


@pytest.mark.trio
@pytest.mark.integration
async def test_trio(marvel_table, nursery):
    """
    Test the flow for 3.6 and up, async generators are
    not supported in 3.5.
    """
    async with r.open(db='test', nursery=nursery) as conn:
        await marvel_table.insert({
            'id': 1,
            'name': 'Iron Man',
            'first_appearance': 'Tales of Suspense #39'
        }).run(conn)

        cursor = await marvel_table.run(conn)
        async for hero in cursor:
            hero['name'] == 'Iron Man'
