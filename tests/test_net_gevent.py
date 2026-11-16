# Copyright 2016-2026 RethinkDB
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

import pytest

from rethinkdb.encoder import ReqlEncoder
from rethinkdb.net import Query
from rethinkdb.net_gevent import ConnectionInstance


class _QueryParent:
    """Parent connection that encodes queries with the stock encoder."""

    def get_json_encoder(self, query: object) -> ReqlEncoder:
        del query
        return ReqlEncoder()


class _Socket:
    """Socket that reports whether the response waiter exists during ``sendall``."""

    def __init__(
        self, instance: ConnectionInstance, token: int, *, fail: bool = False
    ) -> None:
        self.instance = instance
        self.token = token
        self.fail = fail
        self.saw_token = False

    def sendall(self, data: bytes) -> None:
        del data
        # pylint: disable=protected-access
        self.saw_token = self.token in self.instance._user_queries
        if self.fail:
            raise OSError("reset")
        if self.saw_token:
            _, result = self.instance._user_queries[self.token]
            result.set("ok")


def _instance(token: int, *, fail: bool = False) -> tuple[ConnectionInstance, _Socket]:
    instance = ConnectionInstance(_QueryParent())
    socket = _Socket(instance, token, fail=fail)
    instance._socket = socket  # type: ignore[assignment]  # pylint: disable=protected-access
    return instance, socket


@pytest.mark.gevent
@pytest.mark.unit
def test_run_query_registers_before_send() -> None:
    """The response waiter exists before ``sendall`` can yield to the reader."""
    instance, socket = _instance(3)

    assert instance.run_query(Query(1, 3, None), noreply=False) == "ok"
    assert socket.saw_token is True


@pytest.mark.gevent
@pytest.mark.unit
def test_noreply_query_sends_without_a_waiter() -> None:
    """A noreply write does not occupy the response table."""
    instance, socket = _instance(3)

    assert instance.run_query(Query(1, 3, None), noreply=True) is None
    assert socket.saw_token is False


@pytest.mark.gevent
@pytest.mark.unit
def test_send_failure_drops_pending_waiter() -> None:
    """A failed send removes the waiter the caller will not wait on."""
    instance, socket = _instance(3, fail=True)

    with pytest.raises(OSError, match="reset"):
        instance.run_query(Query(1, 3, None), noreply=False)

    assert socket.saw_token is True
    assert 3 not in instance._user_queries  # pylint: disable=protected-access
