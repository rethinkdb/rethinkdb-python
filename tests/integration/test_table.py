import pytest

from rethinkdb.errors import ReqlOpFailedError, ReqlRuntimeError
from tests.helpers import INTEGRATION_TEST_DB, IntegrationTestCaseBase


@pytest.mark.integration
class TestTable(IntegrationTestCaseBase):
    def setup_method(self):
        super(TestTable, self).setup_method()
        self.test_table_name = "test_table"

    def test_table_create(self):
        result = self.r.table_create(self.test_table_name).run(self.conn)

        assert result["tables_created"] == 1
        assert len(result["config_changes"]) == 1
        assert result["config_changes"][0]["old_val"] is None
        assert result["config_changes"][0]["new_val"]["name"] == self.test_table_name
        assert result["config_changes"][0]["new_val"]["db"] == INTEGRATION_TEST_DB
        assert result["config_changes"][0]["new_val"]["durability"] == "hard"
        assert result["config_changes"][0]["new_val"]["primary_key"] == "id"
        assert result["config_changes"][0]["new_val"]["write_acks"] == "majority"
        assert len(result["config_changes"][0]["new_val"]["shards"]) == 1

    def test_table_different_primary_key(self):
        expected_primary_key = "bazinga"

        result = self.r.table_create(
            self.test_table_name, primary_key=expected_primary_key
        ).run(self.conn)

        assert result["tables_created"] == 1
        assert len(result["config_changes"]) == 1
        assert (
            result["config_changes"][0]["new_val"]["primary_key"]
            == expected_primary_key
        )

    def test_table_multiple_shards(self):
        expected_shards = 2

        result = self.r.table_create(self.test_table_name, shards=expected_shards).run(
            self.conn
        )

        assert result["tables_created"] == 1
        assert len(result["config_changes"]) == 1
        assert len(result["config_changes"][0]["new_val"]["shards"]) == expected_shards

    def test_table_create_with_replicas(self):
        expected_replicas = 1

        result = self.r.table_create(
            self.test_table_name, replicas=expected_replicas
        ).run(self.conn)

        assert result["tables_created"] == 1
        assert len(result["config_changes"]) == 1
        assert (
            len(result["config_changes"][0]["new_val"]["shards"][0]["replicas"])
            == expected_replicas
        )

    def test_table_multiple_replicas(self):
        expected_replicas = 2

        # Can't put 2 replicas, it's impossible to have more replicas than the number of servers
        with pytest.raises(ReqlOpFailedError):
            self.r.table_create(self.test_table_name, replicas=expected_replicas).run(
                self.conn
            )

    def test_table_create_twice(self):
        self.r.table_create(self.test_table_name).run(self.conn)

        with pytest.raises(ReqlRuntimeError):
            self.r.table_create(self.test_table_name).run(self.conn)

    def test_table_drop(self):
        self.r.table_create(self.test_table_name).run(self.conn)

        result = self.r.table_drop(self.test_table_name).run(self.conn)

        assert result["tables_dropped"] == 1
        assert len(result["config_changes"]) == 1
        assert result["config_changes"][0]["new_val"] is None
        assert result["config_changes"][0]["old_val"]["name"] == self.test_table_name
        assert result["config_changes"][0]["old_val"]["db"] == INTEGRATION_TEST_DB
        assert result["config_changes"][0]["old_val"]["durability"] == "hard"
        assert result["config_changes"][0]["old_val"]["primary_key"] == "id"
        assert result["config_changes"][0]["old_val"]["write_acks"] == "majority"
        assert len(result["config_changes"][0]["old_val"]["shards"]) == 1

    def test_table_drop_twice(self):
        self.r.table_create(self.test_table_name).run(self.conn)
        self.r.table_drop(self.test_table_name).run(self.conn)

        with pytest.raises(ReqlOpFailedError):
            self.r.table_drop(self.test_table_name).run(self.conn)

    def test_table_list(self):
        self.r.table_create(self.test_table_name).run(self.conn)

        expected_result = [self.test_table_name]

        result = self.r.table_list().run(self.conn)

        assert result == expected_result
