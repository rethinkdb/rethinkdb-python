import sys

import pytest

from tests.helpers import IntegrationTestCaseBase


@pytest.mark.tornado
@pytest.mark.integration
@pytest.mark.skipif(sys.version_info < (3, 6), reason="requires python3.6 or higher")
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
        """
        Test the flow for 3.6 and up, async generators are
        not supported in 3.5.
        """

        tables = self.r.table_list().run(self.conn)
        assert isinstance(tables, list)
