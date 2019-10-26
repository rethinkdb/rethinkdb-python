import os
import sys
from collections import namedtuple
import pytest
from rethinkdb import r
from rethinkdb.errors import ReqlRuntimeError

Helper = namedtuple("Helper", "r connection")

INTEGRATION_TEST_DB = 'integration_test'


@pytest.mark.integration
@pytest.mark.skipif(sys.version_info < (3, 6), reason="requires python3.6 or higher")
async def test_tornado_connect(io_loop):
    """
    Test the flow for 3.6 and up, async generators are
    not supported in 3.5.
    """

    r.set_loop_type("tornado")

    connection = await r.connect(os.getenv("REBIRTHDB_HOST"))
    dbs = await r.db_list().run(connection)
    assert isinstance(dbs, list)
    await connection.close()
