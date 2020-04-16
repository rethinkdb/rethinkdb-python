import sys
import pytest

from tests.helpers import INTEGRATION_TEST_DB, IntegrationTestCaseBase


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.skipif(
    sys.version_info == (3, 4) or sys.version_info == (3, 5),
    reason="requires python3.4 or python3.5",
)
class TestAsyncio(IntegrationTestCaseBase):
    def setup_method(self):
        super(TestAsyncio, self).setup_method()
        self.table_name = "test_asyncio"
        self.r.set_loop_type("asyncio")

    def teardown_method(self):
        super(TestAsyncio, self).teardown_method()
        self.r.set_loop_type(None)

    async def test_flow_coroutine_paradigm(self):
        connection = yield self.conn

        yield self.r.table_create(self.table_name).run(connection)

        table = self.r.table(self.table_name)
        yield table.insert(
            {"id": 1, "name": "Iron Man", "first_appearance": "Tales of Suspense #39"}
        ).run(connection)

        cursor = yield table.run(connection)

        while (yield cursor.fetch_next()):
            hero = yield cursor.__anext__()
            assert hero["name"] == "Iron Man"

        yield connection.close()
