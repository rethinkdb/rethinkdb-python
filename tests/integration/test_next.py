import pytest

from rebirthdb.errors import RqlCursorEmpty
from tests.helpers import IntegrationTestCaseBase, INTEGRATION_TEST_DB


@pytest.mark.integration
class TestCursorNext(IntegrationTestCaseBase):
    def setup_method(self):
        super(TestCursorNext, self).setup_method()
        self.table_name = 'cursor_next'
        self.r.table_create(self.table_name).run(self.conn)
        self.documents = [
            {'id': 1, 'name': 'Testing Cursor/Next 1'},
            {'id': 2, 'name': 'Testing Cursor/Next 2'},
        ]

    def teardown_method(self):
        self.r.table_drop(self.table_name).run(self.conn)
        super(TestCursorNext, self).teardown_method()

    def test_get_next_document(self):
        self.r.table(self.table_name).insert(self.documents).run(self.conn)

        cursor = self.r.table(self.table_name).run(self.conn)

        for document in reversed(self.documents):
            assert document == cursor.next()

    def test_cursor_empty_no_document(self):
        cursor = self.r.table(self.table_name).run(self.conn)

        with pytest.raises(RqlCursorEmpty):
            cursor.next()

    def test_cursor_empty_iteration(self):
        self.r.table(self.table_name).insert(self.documents).run(self.conn)

        cursor = self.r.table(self.table_name).run(self.conn)

        for i in range(0, len(self.documents)):
            cursor.next()

        with pytest.raises(RqlCursorEmpty):
            cursor.next()
