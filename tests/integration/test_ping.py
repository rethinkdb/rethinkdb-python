import os

import pytest

from tests.helpers import IntegrationTestCaseBase

BAD_PASSWORD = "0xDEADBEEF"


@pytest.mark.integration
class TestPing(IntegrationTestCaseBase):
    def teardown_method(self):
        with self.r.connect(host=self.rethinkdb_host) as conn:
            self.r.db("rethinkdb").table("users").filter(
                self.r.row["id"].ne("admin")
            ).delete().run(conn)
        super(TestPing, self).teardown_method()

    def test_bad_password(self):
        with pytest.raises(self.r.ReqlAuthError):
            self.r.connect(password=BAD_PASSWORD, host=self.rethinkdb_host)

    def test_password_connect(self):
        new_user = "user"
        with self.r.connect(
            user="admin", password="", host=self.rethinkdb_host
        ) as conn:
            curr = (
                self.r.db("rethinkdb")
                .table("users")
                .insert({"id": new_user, "password": BAD_PASSWORD})
                .run(conn)
            )
            assert curr == {
                "deleted": 0,
                "errors": 0,
                "inserted": 1,
                "replaced": 0,
                "skipped": 0,
                "unchanged": 0,
            }
            curr = self.r.grant(new_user, {"read": True}).run(conn)
            assert curr == {
                "granted": 1,
                "permissions_changes": [{"new_val": {"read": True}, "old_val": None}],
            }
        with self.r.connect(
            user=new_user, password=BAD_PASSWORD, host=self.rethinkdb_host
        ) as conn:
            with pytest.raises(self.r.ReqlPermissionError):
                # Only administrators may access system tables
                curr = self.r.db("rethinkdb").table("users").get("admin").run(conn)

            with pytest.raises(self.r.ReqlPermissionError):
                # No permission for write. Only for read.
                self.r.db("rethinkdb").table("users").insert(
                    {"id": "bob", "password": ""}
                ).run(conn)

    def test_context_manager(self):
        with self.r.connect(host=self.rethinkdb_host) as conn:
            assert conn.is_open() is True
        assert conn.is_open() is False
