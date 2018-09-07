import pytest

from rethinkdb.errors import ReqlRuntimeError
from tests.helpers import IntegrationTestCaseBase, INTEGRATION_TEST_DB


@pytest.mark.integration
class TestDatabase(IntegrationTestCaseBase):
    def setup_method(self):
        super(TestDatabase, self).setup_method()
        self.test_db_name = 'test_database'

    def test_db_create(self):
        result = self.r.db_create(self.test_db_name).run(self.conn)
        self.r.db_drop(self.test_db_name).run(self.conn)

        assert result['dbs_created'] == 1
        assert result['config_changes'][0]['old_val'] is None
        assert result['config_changes'][0]['new_val']['name'] == self.test_db_name

    def test_db_create_twice(self):
        self.r.db_create(self.test_db_name).run(self.conn)

        with pytest.raises(ReqlRuntimeError):
            self.r.db_create(self.test_db_name).run(self.conn)

        self.r.db_drop(self.test_db_name).run(self.conn)

    def test_db_create_not_alphanumeric(self):
        test_db_name = '!!!'

        with pytest.raises(ReqlRuntimeError):
            self.r.db_create(test_db_name).run(self.conn)

    def test_db_drop(self):
        self.r.db_create(self.test_db_name).run(self.conn)
        result = self.r.db_drop(self.test_db_name).run(self.conn)

        assert result['dbs_dropped'] == 1
        assert result['tables_dropped'] == 0
        assert result['config_changes'][0]['new_val'] is None
        assert result['config_changes'][0]['old_val']['name'] == self.test_db_name

    def test_db_drop_twice(self):
        self.r.db_create(self.test_db_name).run(self.conn)
        self.r.db_drop(self.test_db_name).run(self.conn)

        with pytest.raises(ReqlRuntimeError):
            self.r.db_drop(self.test_db_name).run(self.conn)

    def test_db_list(self):
        expected_result = [
            INTEGRATION_TEST_DB,
            'rethinkdb',
            'test'
        ]

        result = self.r.db_list().run(self.conn)

        assert sorted(result) == sorted(expected_result)
