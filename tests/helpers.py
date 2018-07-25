from rebirthdb import RebirthDB


INTEGRATION_TEST_DB = 'integration_test'


class IntegrationTestCaseBase(object):
    r = RebirthDB()
    conn = None

    def setup_method(self):
        self.conn = self.r.connect()

        if INTEGRATION_TEST_DB not in self.r.db_list().run(self.conn):
            self.r.db_create(INTEGRATION_TEST_DB).run(self.conn)

        self.conn.use(INTEGRATION_TEST_DB)

    def teardown_method(self):
        self.r.db_drop(INTEGRATION_TEST_DB).run(self.conn)
        self.conn.close()
