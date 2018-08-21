import pytest

from rebirthdb.errors import ReqlCursorEmpty
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
            {'id': 3, 'name': 'Testing Cursor/Next 3'},
            {'id': 4, 'name': 'Testing Cursor/Next 4'},
            {'id': 5, 'name': 'Testing Cursor/Next 5'},
        ]

    def teardown_method(self):
        self.r.table_drop(self.table_name).run(self.conn)
        super(TestCursorNext, self).teardown_method()

    def test_get_next_document(self):
        self.r.table(self.table_name).insert(self.documents).run(self.conn)
        documents = list()

        cursor = self.r.table(self.table_name).run(self.conn)

        for document in reversed(self.documents):
            documents.append(cursor.next())

        assert sorted(documents) == sorted(self.documents)

    def test_cursor_empty_no_document(self):
        cursor = self.r.table(self.table_name).run(self.conn)

        with pytest.raises(ReqlCursorEmpty):
            cursor.next()

    def test_cursor_empty_iteration(self):
        self.r.table(self.table_name).insert(self.documents).run(self.conn)

        cursor = self.r.table(self.table_name).run(self.conn)

        for i in range(0, len(self.documents)):
            cursor.next()

        with pytest.raises(ReqlCursorEmpty):
            cursor.next()

    def test_stop_iteration(self):
        self.r.table(self.table_name).insert(self.documents).run(self.conn)

        cursor = self.r.table(self.table_name).run(self.conn)

        with pytest.raises(StopIteration):
            for i in range(0, len(self.documents) + 1):
                cursor.next()
