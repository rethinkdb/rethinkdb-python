import pytest

from tests.helpers import IntegrationTestCaseBase


@pytest.mark.integration
class TestWriteHooks(IntegrationTestCaseBase):
    def setup_method(self):
        super(TestWriteHooks, self).setup_method()

        self.table_name = "test_write_hooks"
        self.documents = [
            {"id": 1, "name": "Testing write hooks 1"},
        ]

        self.r.table_create(self.table_name).run(self.conn)
        self.r.table(self.table_name).insert(self.documents).run(self.conn)

    def test_set_write_hook(self):
        response = (
            self.r.table(self.table_name)
            .set_write_hook(
                lambda context, old_val, new_val: new_val.merge(
                    {"modified_at": context["timestamp"]}
                )
            )
            .run(self.conn)
        )

        assert response == {"created": 1}

    def test_write_hook_add_extra_data(self):
        self.r.table(self.table_name).set_write_hook(
            lambda context, old_val, new_val: new_val.merge(
                {"modified_at": context["timestamp"]}
            )
        ).run(self.conn)

        self.r.table(self.table_name).insert(
            {"id": 2, "name": "Testing write hooks 1"}
        ).run(self.conn)

        document = self.r.table(self.table_name).get(2).run(self.conn)

        assert document.get("modified_at") != None

    def test_get_write_hook(self):
        self.r.table(self.table_name).set_write_hook(
            lambda context, old_val, new_val: new_val.merge(
                {"modified_at": context["timestamp"]}
            )
        ).run(self.conn)

        hook = self.r.table(self.table_name).get_write_hook().run(self.conn)

        assert list(sorted(hook.keys())) == ["function", "query"]
