import os
from rethinkdb import r


INTEGRATION_TEST_DB = 'integration_test'


class IntegrationTestCaseBase(object):
    def _create_database(self, conn):
        if INTEGRATION_TEST_DB not in self.r.db_list().run(conn):
            self.r.db_create(INTEGRATION_TEST_DB).run(conn)

        conn.use(INTEGRATION_TEST_DB)

    def setup_method(self):
        self.r = r
        self.rethinkdb_host = os.getenv('RETHINKDB_HOST', '127.0.0.1')

        self.conn = self.r.connect(
            host=self.rethinkdb_host
        )

        self._create_database(self.conn)

    def teardown_method(self):
        self.r.db_drop(INTEGRATION_TEST_DB).run(self.conn)
        self.conn.close()
