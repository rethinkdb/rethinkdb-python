# Copyright 2022 RethinkDB
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from typing import Any, Callable, Iterable, List, Optional, Tuple

from pydantic import BaseModel
import pytest

from rethinkdb import ast, net, r


class Scenario(BaseModel):
    name: str
    args: Any = []
    kwargs: dict = {}
    expr_args: Any = []
    expected: Any
    expected_field: Optional[str] = None
    callback: Optional[Callable[[], None]] = None
    result_as_string: bool = False
    result_as_list: bool = False

    def __str__(self) -> str:
        return self.name


@pytest.fixture
def conn() -> net.Connection:
    """
    Return a new connection instance.
    """

    return net.make_connection(
        net.DefaultConnection,
        host=os.getenv("RDB_TEST_HOST", "localhost"),
        port=int(os.getenv("RDB_TEST_PORT", 28015)),
    )


def format_failure(scenario: Scenario, result: Any, query: Any) -> str:
    """
    Format failure message to have a better error output.
    """

    return (
        f'Failed scenario: "{scenario.name}"\n'
        f"\t  executed: {query}\n"
        f"\t  wanted: {scenario.expected}\n"
        f"\t  got: {result}"
    )


def assert_test_table(
    command, conn: net.Connection, scenarios: Iterable[Scenario]
) -> None:
    """
    Run table test assertions.
    """

    failed: List[Tuple] = []

    for scenario in scenarios:
        executor = ast.expr(*scenario.expr_args) if scenario.expr_args else r
        query = getattr(executor, command.__name__)(*scenario.args, **scenario.kwargs)

        try:
            result = query.run(conn)

            if scenario.expected_field is not None:
                if result[scenario.expected_field] != scenario.expected:
                    failed.append((scenario, result, query))
            else:
                if scenario.result_as_string:
                    result = str(result)
                elif scenario.result_as_list:
                    result = list(result)

                if result != scenario.expected:
                    failed.append((scenario, result, query))
        except Exception as exc:
            failed.append((scenario, str(exc), query))
        finally:
            if scenario.callback is not None:
                # Cleanup after the test
                scenario.callback()

    error_message = "\n\t".join(map(lambda x: format_failure(*x), failed))
    assert len(failed) == 0, f"The following scenario(s) failed:\n\t{error_message}"


def get_user(username: str, conn: net.Connection) -> Optional[dict]:
    """
    Return the user if exists.
    """

    return r.db("rethinkdb").table("users").get(username).run(conn)


def create_user(username: str, conn: net.Connection) -> dict:
    """
    Create a new user.
    """

    return (
        r.db("rethinkdb")
        .table("users")
        .insert({"id": username, "password": "secret"})
        .run(conn)
    )


def delete_user(username: str, conn: net.Connection) -> dict:
    """
    Delete a user.
    """

    return r.db("rethinkdb").table("users").get(username).delete().run(conn)


def create_db(db_name: str, conn: net.Connection) -> dict:
    """
    Create a new database.
    """

    return r.db_create(db_name).run(conn)


def drop_db(db_name: str, conn: net.Connection) -> dict:
    """
    Drop a database.
    """

    return r.db_drop(db_name).run(conn)


def create_table(db_name, table_name: str, conn: net.Connection) -> dict:
    """
    Create a new table.
    """

    return r.db(db_name).table_create(table_name).run(conn)


def drop_table(db_name, table_name: str, conn: net.Connection) -> dict:
    """
    Drop a table.
    """

    return r.db(db_name).table_drop(table_name).run(conn)
