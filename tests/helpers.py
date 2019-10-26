import os
from rethinkdb import r


INTEGRATION_TEST_DB = 'integration_test'


class IntegrationTestCaseBase(object):
    conn = None

    def connect(self):
        self.conn = r.connect(
            host=rethinkdb_host
        )

    def setup_method(self):
        rethinkdb_host=os.getenv('RETHINKDB_HOST')

        self.connect()

        if INTEGRATION_TEST_DB not in r.db_list().run(self.conn):
            r.db_create(INTEGRATION_TEST_DB).run(self.conn)

        self.conn.use(INTEGRATION_TEST_DB)

    def teardown_method(self):
        r.db_drop(INTEGRATION_TEST_DB).run(self.conn)
        self.conn.close()
