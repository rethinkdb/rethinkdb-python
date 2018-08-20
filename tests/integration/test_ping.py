import os
import pytest

from tests.helpers import IntegrationTestCaseBase


@pytest.mark.integration
class TestPing(IntegrationTestCaseBase):
    def setup_method(self):
        super(TestPing, self).setup_method()
        self.rebirthdb_host=os.getenv('REBIRTHDB_HOST')

    def teardown_method(self):
        with self.r.connect(host=self.rebirthdb_host) as conn:
            curr = self.r.db("rethinkdb").table("users").filter(
                self.r.row["id"].ne("admin")
            ).delete().run(conn)
            assert list(curr)
        super(TestPing, self).teardown_method()

    def test_bad_password(self):
        with pytest.raises(self.r.ReqlAuthError):
            self.r.connect(password="0xDEADBEEF", host=self.rebirthdb_host)

    def test_password_connect(self):
        with self.r.connect(user="admin", password="", host=self.rebirthdb_host) as conn:
            curr = self.r.db("rethinkdb").table("users").insert(
                {"id": "user", "password": "0xDEADBEEF"}
            ).run(conn)
            assert curr == {
                'deleted': 0,
                'errors': 0,
                'inserted': 1,
                'replaced': 0,
                'skipped': 0,
                'unchanged': 0}
            curr = self.r.db("rethinkdb").grant("user", {"read": True}).run(conn)
            assert curr == {
                'granted': 1,
                'permissions_changes': [
                    {
                        'new_val': {'read': True},
                        'old_val': None}]}
        with self.r.connect(user="user", password="0xDEADBEEF", host=self.rebirthdb_host) as conn:
            curr = self.r.db("rethinkdb").table("users").get("admin").run(conn)
            assert curr == {'id': 'admin', 'password': False}
            with pytest.raises(self.r.ReqlPermissionError):
                curr = self.r.db("rethinkdb").table("users").insert(
                    {"id": "bob", "password": ""}
                ).run(conn)
                assert curr is False

    def test_context_manager(self):
        with self.r.connect(host=self.rebirthdb_host) as conn:
            assert conn.is_open() is True
        assert conn.is_open() is False
