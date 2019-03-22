from subprocess import run

import pytest

from tests.helpers import IntegrationTestCaseBase


@pytest.mark.integration
class TestRestore(IntegrationTestCaseBase):
    def test_restore(self):
        run(["rethinkdb-restore", "-c", self.rethinkdb_host])
