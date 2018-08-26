import pytest

from rebirthdb.errors import ReqlRuntimeError, ReqlOpFailedError
from tests.helpers import IntegrationTestCaseBase, INTEGRATION_TEST_DB


@pytest.mark.integration
class TestTable(IntegrationTestCaseBase):
    def setup_method(self):
        super(TestTable, self).setup_method()
        self.table_name = 'test_index'
        self.r.table_create(self.table_name).run(self.conn)

    def test_create_index(self):
        index_field = 'name'

        result = self.r.table(self.table_name).index_create(index_field).run(self.conn)

        assert result['created'] == 1

    def test_create_nested_field_index(self):
        index_field = 'author_name'

        result = self.r.table(self.table_name).index_create(index_field, [
            self.r.row['author']['name']
        ]).run(self.conn)

        assert result['created'] == 1

    def test_create_index_geo(self):
        index_field = 'location'

        result = self.r.table(self.table_name).index_create(index_field, geo=True).run(self.conn)

        assert result['created'] == 1

    def test_create_compound_index(self):
        index_field = 'name_and_age'

        result = self.r.table(self.table_name).index_create(index_field, [
            self.r.row['name'],
            self.r.row['age']
        ]).run(self.conn)

        assert result['created'] == 1

    def test_create_multi_index(self):
        index_field = 'name'

        result = self.r.table(self.table_name).index_create(index_field, multi=True).run(self.conn)

        assert result['created'] == 1

    def test_create_index_twice(self):
        index_field = 'name'

        self.r.table(self.table_name).index_create(index_field).run(self.conn)

        with pytest.raises(ReqlRuntimeError):
            self.r.table(self.table_name).index_create(index_field).run(self.conn)

    def test_drop_index(self):
        index_field = 'name'
        self.r.table(self.table_name).index_create(index_field).run(self.conn)

        result = self.r.table(self.table_name).index_drop(index_field).run(self.conn)

        assert result['dropped'] == 1

    def test_drop_index_twice(self):
        index_field = 'name'
        self.r.table(self.table_name).index_create(index_field).run(self.conn)
        self.r.table(self.table_name).index_drop(index_field).run(self.conn)

        with pytest.raises(ReqlRuntimeError):
            self.r.table(self.table_name).index_drop(index_field).run(self.conn)

    def test_list_index(self):
        index_field = 'name'
        expected_index_list = [
            index_field
        ]

        self.r.table(self.table_name).index_create(index_field).run(self.conn)
        result = self.r.table(self.table_name).index_list().run(self.conn)

        assert len(result) == 1
        assert result == expected_index_list

    def test_rename_index(self):
        index_field = 'name'
        renamed_field = 'username'

        self.r.table(self.table_name).index_create(index_field).run(self.conn)
        result = self.r.table(self.table_name).index_rename(index_field, renamed_field).run(self.conn)

        assert len(result) == 1
        assert result['renamed'] == 1

    def test_rename_index_same_key(self):
        index_field = 'name'

        self.r.table(self.table_name).index_create(index_field).run(self.conn)
        result = self.r.table(self.table_name).index_rename(index_field, index_field).run(self.conn)

        assert len(result) == 1
        assert result['renamed'] == 0

    def test_rename_index_overwrite(self):
        index_field = 'name'
        renamed_field = 'username'

        self.r.table(self.table_name).index_create(index_field).run(self.conn)
        self.r.table(self.table_name).index_create(renamed_field).run(self.conn)
        result = self.r.table(self.table_name).index_rename(index_field, renamed_field, overwrite=True).run(self.conn)

        assert len(result) == 1
        assert result['renamed'] == 1

    def test_rename_index_without_overwrite(self):
        index_field = 'name'
        renamed_field = 'username'

        self.r.table(self.table_name).index_create(index_field).run(self.conn)
        self.r.table(self.table_name).index_create(renamed_field).run(self.conn)

        with pytest.raises(ReqlOpFailedError):
            result = self.r.table(self.table_name).index_rename(index_field, renamed_field).run(self.conn)

    def test_table_index_status(self):
        index_field = 'name'

        self.r.table(self.table_name).index_create(index_field).run(self.conn)
        result = self.r.table(self.table_name).index_status().run(self.conn)

        assert len(result) == 1
        assert result[0]['index'] == index_field
        assert result[0]['multi'] == False
        assert result[0]['outdated'] == False

    def test_index_status_empty(self):
        result = self.r.table(self.table_name).index_status().run(self.conn)

        assert len(result) == 0

    def test_index_status_non_existing(self):
        index_field = 'name'

        with pytest.raises(ReqlOpFailedError):
            self.r.table(self.table_name).index_status(index_field).run(self.conn)
