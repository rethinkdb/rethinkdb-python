from subprocess import run

import pytest

from tests.helpers import IntegrationTestCaseBase


@pytest.mark.integration
class TestDump(IntegrationTestCaseBase):
    def test_dump(self):
        status = run(["rethinkdb-dump", "-c", self.rethinkdb_host])
        assert status.returncode == 0, status.stderr
