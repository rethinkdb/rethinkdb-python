import sys
import pytest

from asyncio import coroutine
from tests.helpers import INTEGRATION_TEST_DB, IntegrationTestCaseBase


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.skipif(
    sys.version_info == (3, 4) or sys.version_info == (3, 5),
    reason="requires python3.4 or python3.5"
)
class TestAsyncio(IntegrationTestCaseBase):
    def setup_method(self):
        super(TestAsyncio, self).setup_method()
        self.table_name = 'test_asyncio'
        self.r.set_loop_type('asyncio')

    def teardown_method(self):
        super(TestAsyncio, self).teardown_method()
        self.r.set_loop_type(None)

    @coroutine
    def test_flow_coroutine_paradigm(self):
        connection = yield from self.conn

        yield from self.r.table_create(self.table_name).run(connection)

        table = self.r.table(self.table_name)
        yield from table.insert({
            'id': 1,
            'name': 'Iron Man',
            'first_appearance': 'Tales of Suspense #39'
        }).run(connection)

        cursor = yield from table.run(connection)

        while (yield from cursor.fetch_next()):
            hero = yield from cursor.__anext__()
            assert hero['name'] == 'Iron Man'

        yield from connection.close()
