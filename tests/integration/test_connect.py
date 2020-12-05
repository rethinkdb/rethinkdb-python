import os

import pytest

from rethinkdb import r
from tests.helpers import INTEGRATION_TEST_DB, IntegrationTestCaseBase


@pytest.mark.integration
class TestConnect(IntegrationTestCaseBase):
    def setup_method(self):
        super(TestConnect, self).setup_method()

    def test_connect(self):
        db_url = "rethinkdb://{host}".format(host=self.rethinkdb_host)

        assert self.r.connect(url=db_url) is not None

    def test_connect_with_username(self):
        db_url = "rethinkdb://admin@{host}".format(host=self.rethinkdb_host)

        assert self.r.connect(url=db_url) is not None

    def test_connect_to_db(self):
        db_url = "rethinkdb://{host}/{database}".format(
            host=self.rethinkdb_host, database=INTEGRATION_TEST_DB
        )

        assert self.r.connect(url=db_url) is not None
