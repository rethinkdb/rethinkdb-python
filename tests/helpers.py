import os
from rethinkdb import RethinkDB


INTEGRATION_TEST_DB = 'integration_test'


class IntegrationTestCaseBase(object):
    r = RethinkDB()
    conn = None

    def connect(self):
        self.conn = self.r.connect(
            host=self.rebirthdb_host
        )

    def setup_method(self):
        self.rebirthdb_host=os.getenv('REBIRTHDB_HOST')

        self.connect()

        if INTEGRATION_TEST_DB not in self.r.db_list().run(self.conn):
            self.r.db_create(INTEGRATION_TEST_DB).run(self.conn)

        self.conn.use(INTEGRATION_TEST_DB)

    def teardown_method(self):
        self.r.db_drop(INTEGRATION_TEST_DB).run(self.conn)
        self.conn.close()
