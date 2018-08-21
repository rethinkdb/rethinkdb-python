import pytest

from tests.helpers import IntegrationTestCaseBase, INTEGRATION_TEST_DB


@pytest.mark.integration
class TestCursorFor(IntegrationTestCaseBase):
    def setup_method(self):
        super(TestCursorFor, self).setup_method()
        self.table_name = 'cursor_for'
        self.r.table_create(self.table_name).run(self.conn)
        self.documents = [
            {'id': 1, 'name': 'Testing Cursor/Next 1'},
            {'id': 2, 'name': 'Testing Cursor/Next 2'},
        ]

    def teardown_method(self):
        self.r.table_drop(self.table_name).run(self.conn)
        super(TestCursorFor, self).teardown_method()

    def test_loop(self):
        self.r.table(self.table_name).insert(self.documents).run(self.conn)

        documents = list()

        for document in self.r.table(self.table_name).run(self.conn):
            documents.append(document)

        assert sorted(documents) == sorted(self.documents)
