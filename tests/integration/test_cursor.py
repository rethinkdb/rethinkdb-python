import pytest

from rethinkdb.errors import ReqlCursorEmpty
from tests.helpers import IntegrationTestCaseBase


@pytest.mark.integration
class TestCursor(IntegrationTestCaseBase):
    def setup_method(self):
        super(TestCursor, self).setup_method()
        self.table_name = "test_cursor"
        self.r.table_create(self.table_name).run(self.conn)
        self.documents = [
            {"id": 1, "name": "Testing Cursor/Next 1"},
            {"id": 2, "name": "Testing Cursor/Next 2"},
            {"id": 3, "name": "Testing Cursor/Next 3"},
            {"id": 4, "name": "Testing Cursor/Next 4"},
            {"id": 5, "name": "Testing Cursor/Next 5"},
        ]

    def test_get_next_document(self):
        self.r.table(self.table_name).insert(self.documents).run(self.conn)
        documents = list()

        cursor = self.r.table(self.table_name).run(self.conn)

        for document in reversed(self.documents):
            documents.append(cursor.next())

        assert sorted(documents, key=lambda doc: doc.get("id")) == self.documents

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

    def test_for_loop(self):
        self.r.table(self.table_name).insert(self.documents).run(self.conn)

        documents = list()

        for document in self.r.table(self.table_name).run(self.conn):
            documents.append(document)

        assert sorted(documents, key=lambda doc: doc.get("id")) == self.documents

    def test_next(self):
        self.r.table(self.table_name).insert(self.documents).run(self.conn)

        cursor = self.r.table(self.table_name).run(self.conn)

        assert hasattr(cursor, "__next__")

    def test_iter(self):
        self.r.table(self.table_name).insert(self.documents).run(self.conn)

        cursor = self.r.table(self.table_name).run(self.conn)

        assert hasattr(cursor, "__iter__")

    def test_close_cursor(self):
        cursor = self.r.table(self.table_name).run(self.conn)
        cursor.close()

        assert cursor.conn.is_open()
        assert isinstance(cursor.error, ReqlCursorEmpty)
