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
from rethinkdb.net import Query
from rethinkdb.net_tornado import ConnectionInstance


class _QueryParent:
    """Parent connection that encodes queries with the stock encoder."""

    def __init__(self) -> None:
        self.handshake = _Handshake()

    def get_json_encoder(self, query: object) -> ReqlEncoder:
        del query
        return ReqlEncoder()


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


class _HandshakeStream:
    """Stream that finishes its write before the following read can proceed."""

    def __init__(self) -> None:
        self.write_finished = False
        self.read_after_write = False

    async def write(self, data: bytes) -> None:
        del data
        await asyncio.sleep(0)
        self.write_finished = True

    async def read_until(self, separator: bytes) -> bytes:
        del separator
        self.read_after_write = self.write_finished
        return b"ok\0"


class _QueryStream:
    """Stream that observes query registration from inside ``write``."""

    def __init__(
        self, instance: ConnectionInstance, token: int, *, fail: bool = False
    ) -> None:
        self.instance = instance
        self.token = token
        self.fail = fail
        self.saw_token = False

    async def write(self, data: bytes) -> None:
        del data
        # pylint: disable=protected-access
        self.saw_token = self.token in self.instance._user_queries
        if self.fail:
            raise ConnectionResetError("reset")
        if self.saw_token:
            _, future = self.instance._user_queries[self.token]
            future.set_result("ok")


def _instance(stream: object) -> ConnectionInstance:
    instance = ConnectionInstance(_QueryParent())
    instance._socket.close()  # pylint: disable=protected-access
    instance._stream = stream  # type: ignore[assignment]  # pylint: disable=protected-access
    return instance


@pytest.mark.asyncio
@pytest.mark.tornado
@pytest.mark.unit
async def test_handshake_awaits_write_before_read() -> None:
    """The handshake does not read until the preceding write has finished."""
    stream = _HandshakeStream()
    instance = _instance(stream)

    await instance._perform_handshake(None)  # pylint: disable=protected-access

    assert stream.write_finished is True
    assert stream.read_after_write is True


@pytest.mark.asyncio
@pytest.mark.tornado
@pytest.mark.unit
async def test_run_query_registers_before_write() -> None:
    """The response waiter exists before ``write`` yields to the reader."""
    instance = ConnectionInstance(_QueryParent())
    stream = _QueryStream(instance, token=4)
    instance._socket.close()  # pylint: disable=protected-access
    instance._stream = stream  # type: ignore[assignment]  # pylint: disable=protected-access

    assert await instance.run_query(Query(1, 4, None), noreply=False) == "ok"
    assert stream.saw_token is True


@pytest.mark.asyncio
@pytest.mark.tornado
@pytest.mark.unit
async def test_noreply_query_writes_without_a_waiter() -> None:
    """A noreply write does not occupy the response table."""
    instance = ConnectionInstance(_QueryParent())
    stream = _QueryStream(instance, token=4)
    instance._socket.close()  # pylint: disable=protected-access
    instance._stream = stream  # type: ignore[assignment]  # pylint: disable=protected-access

    assert await instance.run_query(Query(1, 4, None), noreply=True) is None
    assert stream.saw_token is False


@pytest.mark.asyncio
@pytest.mark.tornado
@pytest.mark.unit
async def test_write_failure_drops_pending_waiter() -> None:
    """A failed write removes the waiter the caller will not await."""
    instance = ConnectionInstance(_QueryParent())
    stream = _QueryStream(instance, token=4, fail=True)
    instance._socket.close()  # pylint: disable=protected-access
    instance._stream = stream  # type: ignore[assignment]  # pylint: disable=protected-access

    with pytest.raises(ConnectionResetError, match="reset"):
        await instance.run_query(Query(1, 4, None), noreply=False)

    assert stream.saw_token is True
    assert 4 not in instance._user_queries  # pylint: disable=protected-access
