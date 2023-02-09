# Copyright 2022 - present RethinkDB
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

import threading

import pytest

from rethinkdb.repl import REPL_CONNECTION_ATTRIBUTE, Repl


class TestConnection:  # pylint: disable=too-few-public-methods
    """
    Test connection object
    """


def setup_module(_):
    """
    Setup tests for this module.
    """

    local_thread = threading.local()

    if hasattr(local_thread, REPL_CONNECTION_ATTRIBUTE):
        delattr(local_thread, REPL_CONNECTION_ATTRIBUTE)


@pytest.mark.integration
def test_get_connection():
    """
    Test getting connection from the local thread.
    """

    repl = Repl()

    assert repl.get_connection() is None
    assert repl.is_repl_active is False


@pytest.mark.integration
def test_get_existing_connection():
    """
    Test getting existing connection from the local thread.
    """

    repl = Repl()
    connection = TestConnection()

    repl.set_connection(connection)

    assert repl.get_connection() == connection
    assert repl.is_repl_active is True


@pytest.mark.integration
def test_set_connection():
    """
    Test setting connection on the local thread.
    """

    repl = Repl()
    connection = TestConnection()

    repl.set_connection(connection)

    assert repl.get_connection() == connection
    assert repl.is_repl_active is True


@pytest.mark.integration
def test_override_connection():
    """
    Test overriding connection on the local thread.
    """

    repl = Repl()
    original_connection = TestConnection()
    new_connection = TestConnection()

    repl.set_connection(original_connection)

    assert repl.get_connection() == original_connection
    assert repl.is_repl_active is True

    repl.set_connection(new_connection)

    assert repl.get_connection() == new_connection
    assert repl.is_repl_active is True


@pytest.mark.integration
def test_clear_connection():
    """
    Test clearing connection from the local thread.
    """

    repl = Repl()
    connection = TestConnection()

    repl.set_connection(connection)
    repl.clear_connection()

    assert repl.get_connection() is None
    assert repl.is_repl_active is False


@pytest.mark.integration
def test_clear_not_existing_connection():
    """
    Test clearing connection from the local thread which was never set.
    """

    repl = Repl()
    repl.clear_connection()

    assert repl.get_connection() is None
    assert repl.is_repl_active is False
