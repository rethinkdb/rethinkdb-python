import pytest

from tests.helpers import IntegrationTestCaseBase, INTEGRATION_TEST_DB


@pytest.mark.integration
class TestREPL(IntegrationTestCaseBase):

    def setup_method(self):
        super(TestREPL, self).setup_method()
        self.conn = self.r.connect().repl()

    def test_repl_does_not_require_conn(self):
        databases = self.r.db_list().run()
        assert INTEGRATION_TEST_DB in databases
