import pytest

from tests.helpers import INTEGRATION_TEST_DB, IntegrationTestCaseBase


@pytest.mark.trio
@pytest.mark.integration
class TestTrio(IntegrationTestCaseBase):
    def setup_method(self):
        super(TestTrio, self).setup_method()
        self.table_name = "test_trio"
        self.r.set_loop_type("trio")
        self.r.table_create(self.table_name).run(self.conn)

    def teardown_method(self):
        super(TestTrio, self).teardown_method()
        self.r.set_loop_type(None)

    async def test_trio(self, nursery):
        """
        Test the flow for 3.6 and up, async generators are
        not supported in 3.5.
        """

        async with self.r.open(db=INTEGRATION_TEST_DB, nursery=nursery) as conn:
            await self.r.table(self.table_name).insert(
                {
                    "id": 1,
                    "name": "Iron Man",
                    "first_appearance": "Tales of Suspense #39",
                }
            ).run(conn)

            cursor = await self.r.table(self.table_name).run(conn)
            async for hero in cursor:
                hero["name"] == "Iron Man"
