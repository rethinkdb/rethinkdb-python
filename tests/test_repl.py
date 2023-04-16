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

from unittest.mock import Mock, patch

from rethinkdb.repl import REPL_CONNECTION_ATTRIBUTE, Repl


@patch("rethinkdb.repl.threading")
def test_init(mock_threading):
    """
    Test initialization of REPL object.
    """

    repl = Repl()

    assert repl.is_repl_active is False
    assert repl.thread_data == mock_threading.local.return_value


@patch("rethinkdb.repl.threading")
def test_get_thread_data(mock_threading):
    """
    Test getting thread data from the Repl object.
    """

    local_thread_data = Mock()
    delattr(local_thread_data, REPL_CONNECTION_ATTRIBUTE)

    mock_threading.local.return_value = local_thread_data

    repl = Repl()
    connection = repl.get_connection()

    assert connection is None
    assert repl.is_repl_active is False


@patch("rethinkdb.repl.threading")
def test_get_existing_thread_data(mock_threading):
    """
    Test getting existing thread data from the Repl object.
    """

    expected_connection = Mock()

    local_thread_data = Mock()
    setattr(local_thread_data, REPL_CONNECTION_ATTRIBUTE, expected_connection)

    mock_threading.local.return_value = local_thread_data

    repl = Repl()
    connection = repl.get_connection()

    assert connection == expected_connection


@patch("rethinkdb.repl.threading")
def test_set_connection_on_thread(mock_threading):
    """
    Test setting connection on thread when no previous connection was set.
    """

    expected_connection = Mock()

    local_thread_data = Mock()
    delattr(local_thread_data, REPL_CONNECTION_ATTRIBUTE)

    mock_threading.local.return_value = local_thread_data

    repl = Repl()
    repl.set_connection(expected_connection)

    assert repl.get_connection() == expected_connection
    assert repl.is_repl_active is True


@patch("rethinkdb.repl.threading")
def test_override_connection_on_thread(mock_threading):
    """
    Test setting connection on thread when a previous connection was already set.
    """

    original_connection = Mock()
    expected_connection = Mock()

    local_thread_data = Mock()
    setattr(local_thread_data, REPL_CONNECTION_ATTRIBUTE, original_connection)

    mock_threading.local.return_value = local_thread_data

    repl = Repl()
    repl.set_connection(expected_connection)

    assert repl.get_connection() == expected_connection
    assert repl.is_repl_active is True


@patch("rethinkdb.repl.threading")
def test_clear_thread_data(mock_threading):
    """
    Test clearing the thread data.
    """

    original_connection = Mock()

    local_thread_data = Mock()
    setattr(local_thread_data, REPL_CONNECTION_ATTRIBUTE, original_connection)

    mock_threading.local.return_value = local_thread_data

    repl = Repl()
    repl.clear_connection()
    connection = repl.get_connection()

    assert connection is None
    assert repl.is_repl_active is False


@patch("rethinkdb.repl.threading")
def test_clear_not_existing_thread_data(mock_threading):
    """
    Test clearing the thread data.
    """

    local_thread_data = Mock()
    delattr(local_thread_data, REPL_CONNECTION_ATTRIBUTE)

    mock_threading.local.return_value = local_thread_data

    repl = Repl()
    repl.clear_connection()
    connection = repl.get_connection()

    assert connection is None
    assert repl.is_repl_active is False
