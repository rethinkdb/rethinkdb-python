from copy import deepcopy

import pytest

from tests.helpers import IntegrationTestCaseBase


@pytest.mark.integration
class TestDateAndTime(IntegrationTestCaseBase):
    def setup_method(self):
        super(TestDateAndTime, self).setup_method()
        self.table_name = "test_now"
        self.r.table_create(self.table_name).run(self.conn)

        self.expected_insert_response = {
            "deleted": 0,
            "errors": 0,
            "inserted": 1,
            "replaced": 0,
            "skipped": 0,
            "unchanged": 0,
        }

    @staticmethod
    def compare_seconds(a, b):
        """
        During the tests, the milliseconds are a little different, so we need to look at the results in seconds.
        """

        def second_precision(dt):
            return str(dt).split(".")[0]

        assert second_precision(a) == second_precision(b)

    def test_insert_with_now(self):
        now = self.r.now()
        insert_data = {
            "id": 1,
            "name": "Captain America",
            "real_name": "Steven Rogers",
            "universe": "Earth-616",
            "created_at": now,
        }

        response = self.r.table(self.table_name).insert(insert_data).run(self.conn)
        document = self.r.table(self.table_name).get(1).run(self.conn)

        assert response == self.expected_insert_response
        self.compare_seconds(document["created_at"], self.r.now().run(self.conn))
