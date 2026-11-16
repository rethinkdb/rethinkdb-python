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

import asyncio

import pytest

from rethinkdb.encoder import ReqlEncoder
from rethinkdb.errors import ReqlDriverError
from rethinkdb.net import Query
from rethinkdb.net_asyncio import ConnectionInstance


class _Socket:
    """Socket double that accepts the connect-time options."""

    def setsockopt(self, level: int, option: int, value: int) -> None:
        del level, option, value


class _Handshake:
    """Handshake that emits one request and then finishes."""

    def __init__(self) -> None:
        self._sent = False

    def reset(self) -> None:
        self._sent = False

    def next_message(self, raw_response: bytes | None) -> bytes | None:
        del raw_response
        if self._sent:
            return None
        self._sent = True
        return b"protocol"


class _ConnectParent:
    """Parent connection with the attributes ``connect`` reads."""

    def __init__(self) -> None:
        self.host = "localhost"
        self.port = 28015
        self.ssl: dict[str, str] = {}
        self.handshake = _Handshake()


class _QueryParent:
    """Parent connection that encodes queries with the stock encoder."""

    def get_json_encoder(self, query: object) -> ReqlEncoder:
        del query
        return ReqlEncoder()


class _EofReader:
    """Reader whose first ``readuntil`` reports EOF."""

    def __init__(self) -> None:
        self.readuntil_calls = 0

    def at_eof(self) -> bool:
        return True

    async def readuntil(self, separator: bytes) -> bytes:
        del separator
        self.readuntil_calls += 1
        raise asyncio.IncompleteReadError(partial=b"", expected=1)


class _ConnectWriter:
    """Writer that records handshake writes and drains."""

    def __init__(self) -> None:
        self.writes: list[bytes] = []
        self.drain_calls = 0

    def write(self, data: bytes) -> None:
        self.writes.append(data)

    async def drain(self) -> None:
        self.drain_calls += 1

    def get_extra_info(self, name: str) -> _Socket:
        assert name == "socket"
        return _Socket()

    def close(self) -> None:
        return None

    async def wait_closed(self) -> None:
        return None


class _QueryWriter:
    """Writer that observes query registration from inside ``drain``."""

    def __init__(
        self,
        instance: ConnectionInstance,
        token: int,
        *,
        fail: bool = False,
        complete_before_failure: bool = False,
    ) -> None:
        self.instance = instance
        self.token = token
        self.fail = fail
        self.complete_before_failure = complete_before_failure
        self.saw_token = False
        self.drain_calls = 0

    def write(self, data: bytes) -> None:
        del data

    async def drain(self) -> None:
        self.drain_calls += 1
        # pylint: disable=protected-access
        self.saw_token = self.token in self.instance._user_queries
        if self.saw_token and (not self.fail or self.complete_before_failure):
            _, future = self.instance._user_queries[self.token]
            future.set_result("ok")
        if self.fail:
            raise ConnectionResetError("reset")


def _query(token: int) -> Query:
    return Query(1, token, None)


def _query_instance(writer: _QueryWriter) -> ConnectionInstance:
    instance = writer.instance
    instance._streamwriter = writer  # pylint: disable=protected-access
    return instance


@pytest.mark.asyncio
@pytest.mark.unit
async def test_handshake_eof_is_a_driver_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A handshake that ends before the delimiter fails as a driver error."""
    reader = _EofReader()
    writer = _ConnectWriter()

    async def open_connection(
        *args: object, **kwargs: object
    ) -> tuple[_EofReader, _ConnectWriter]:
        del args, kwargs
        return reader, writer

    monkeypatch.setattr(
        "rethinkdb.net_asyncio.asyncio.open_connection", open_connection
    )
    instance = ConnectionInstance(_ConnectParent())

    with pytest.raises(ReqlDriverError, match="Could not connect"):
        await instance.connect(1)

    assert writer.writes == [b"protocol"]
    assert writer.drain_calls == 1
    assert reader.readuntil_calls == 1


@pytest.mark.asyncio
@pytest.mark.unit
async def test_run_query_registers_before_drain() -> None:
    """The response waiter exists before ``drain`` yields to the reader."""
    instance = ConnectionInstance(_QueryParent())
    writer = _QueryWriter(instance, token=7)
    _query_instance(writer)

    assert await instance.run_query(_query(7), noreply=False) == "ok"
    assert writer.drain_calls == 1
    assert writer.saw_token is True


@pytest.mark.asyncio
@pytest.mark.unit
async def test_noreply_query_drains_without_a_waiter() -> None:
    """A noreply write is drained and does not occupy the response table."""
    instance = ConnectionInstance(_QueryParent())
    writer = _QueryWriter(instance, token=7)
    _query_instance(writer)

    assert await instance.run_query(_query(7), noreply=True) is None
    assert writer.drain_calls == 1
    assert writer.saw_token is False


@pytest.mark.asyncio
@pytest.mark.unit
async def test_drain_failure_drops_pending_waiter() -> None:
    """A failed drain removes the waiter the caller will not await."""
    instance = ConnectionInstance(_QueryParent())
    writer = _QueryWriter(instance, token=7, fail=True)
    _query_instance(writer)

    with pytest.raises(ConnectionResetError, match="reset"):
        await instance.run_query(_query(7), noreply=False)

    assert writer.saw_token is True
    assert 7 not in instance._user_queries  # pylint: disable=protected-access


@pytest.mark.asyncio
@pytest.mark.unit
async def test_drain_failure_keeps_a_completed_waiter() -> None:
    """A response delivered during ``drain`` is left in place when the write fails."""
    instance = ConnectionInstance(_QueryParent())
    writer = _QueryWriter(instance, token=7, fail=True, complete_before_failure=True)
    _query_instance(writer)

    with pytest.raises(ConnectionResetError, match="reset"):
        await instance.run_query(_query(7), noreply=False)

    assert 7 in instance._user_queries  # pylint: disable=protected-access
