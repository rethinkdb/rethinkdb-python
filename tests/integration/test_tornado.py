import sys

import pytest

from tests.helpers import IntegrationTestCaseBase


@pytest.mark.tornado
@pytest.mark.integration
class TestTornado(IntegrationTestCaseBase):
    def setup_method(self):
        super(TestTornado, self).setup_method()
        self.table_name = "test_tornado"
        self.r.set_loop_type("tornado")
        self.r.table_create(self.table_name).run(self.conn)

    def teardown_method(self):
        super(TestTornado, self).teardown_method()
        self.r.set_loop_type(None)

    async def test_tornado_list_tables(self):
        tables = self.r.table_list().run(self.conn)
        assert isinstance(tables, list)
