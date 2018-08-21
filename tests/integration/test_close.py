import pytest

from rebirthdb.errors import ReqlCursorEmpty
from tests.helpers import IntegrationTestCaseBase, INTEGRATION_TEST_DB


@pytest.mark.integration
class TestCursorClose(IntegrationTestCaseBase):
    def test_close_cursor(self):
        cursor = self.r.db('rethinkdb').table('users').run(self.conn)
        cursor.close()

        assert cursor.conn.is_open() is True
        assert isinstance(cursor.error, ReqlCursorEmpty)
