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
import trio

from rethinkdb.encoder import ReqlEncoder
from rethinkdb.errors import ReqlDriverError
from rethinkdb.net import Query
from rethinkdb.net_trio import ConnectionInstance, TrioCursor


class _ChunkStream:
    """Stream that returns scripted chunks from ``receive_some``."""

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = list(chunks)

    async def receive_some(self, max_bytes: int) -> bytes:
        if not self._chunks:
            return b""
        chunk = self._chunks[0]
        if not chunk:
            self._chunks.pop(0)
            return b""
        taken, rest = chunk[:max_bytes], chunk[max_bytes:]
        if rest:
            self._chunks[0] = rest
        else:
            self._chunks.pop(0)
        return taken


class _BrokenStream:
    """Stream whose reads and writes fail with ``error``."""

    def __init__(self, error: Exception) -> None:
        self._error = error

    async def receive_some(self, max_bytes: int) -> bytes:
        del max_bytes
        raise self._error

    async def send_all(self, data: bytes) -> None:
        del data
        raise self._error


class _ClosedParent:
    """Parent connection whose ``resume`` reports a closed connection."""

    async def resume(self, cursor: object) -> None:
        del cursor
        raise ReqlDriverError("Connection is closed.")


class _CursorConn:
    """Stand-in for the connection object a cursor points at."""

    def __init__(self) -> None:
        self.parent = _ClosedParent()


def _instance(stream: object) -> ConnectionInstance:
    instance = ConnectionInstance(parent=None)
    instance._stream = stream  # pylint: disable=protected-access
    return instance


@pytest.mark.trio
@pytest.mark.unit
async def test_read_exactly_returns_a_full_chunk() -> None:
    """A complete read is returned unchanged, including bytes split across chunks."""
    instance = _instance(_ChunkStream([b"ab", b"cd"]))

    assert await instance._read_exactly(4) == b"abcd"  # pylint: disable=protected-access


@pytest.mark.trio
@pytest.mark.unit
async def test_read_exactly_empty_chunk_closes_connection() -> None:
    """EOF is a closed connection, not another read."""
    instance = _instance(_ChunkStream([b""]))

    with pytest.raises(ReqlDriverError, match="Connection is closed"):
        await instance._read_exactly(4)  # pylint: disable=protected-access

    assert instance._closed is True  # pylint: disable=protected-access


@pytest.mark.trio
@pytest.mark.unit
@pytest.mark.parametrize(
    "error",
    [trio.BrokenResourceError("reset"), trio.ClosedResourceError("closed")],
)
async def test_read_exactly_broken_stream_closes_connection(error: Exception) -> None:
    """A broken or closed Trio stream raises a driver error."""
    instance = _instance(_BrokenStream(error))

    with pytest.raises(ReqlDriverError, match="Connection is closed"):
        await instance._read_exactly(4)  # pylint: disable=protected-access

    assert instance._closed is True  # pylint: disable=protected-access


@pytest.mark.trio
@pytest.mark.unit
async def test_read_until_empty_chunk_closes_connection() -> None:
    """A handshake read stops with a driver error when the peer sends EOF."""
    instance = _instance(_ChunkStream([b"ok", b""]))

    with pytest.raises(ReqlDriverError, match="Connection is closed"):
        await instance._read_until(b"\0")  # pylint: disable=protected-access

    assert instance._closed is True  # pylint: disable=protected-access


@pytest.mark.trio
@pytest.mark.unit
async def test_send_broken_stream_closes_connection() -> None:
    """A failed write is reported as a closed connection."""
    instance = _instance(_BrokenStream(trio.BrokenResourceError("reset")))

    with pytest.raises(ReqlDriverError, match="Connection is closed"):
        await instance._send(b"query")  # pylint: disable=protected-access

    assert instance._closed is True  # pylint: disable=protected-access


@pytest.mark.trio
@pytest.mark.unit
async def test_parent_continue_swallows_closed_connection() -> None:
    """CONTINUE after the socket dies does not escape into the caller nursery."""
    cursor = TrioCursor.__new__(TrioCursor)
    cursor.conn = _CursorConn()  # type: ignore[assignment]

    assert await cursor._parent_continue() is None  # pylint: disable=protected-access


class _QueryParent:
    """Parent connection that encodes queries with the stock encoder."""

    def get_json_encoder(self, query: object) -> ReqlEncoder:
        del query
        return ReqlEncoder()


class _RecordingStream:
    """Stream that reports whether the response waiter exists during ``send_all``."""

    def __init__(self, instance: ConnectionInstance, token: int, *, fail: bool) -> None:
        self.instance = instance
        self.token = token
        self.fail = fail
        self.saw_token = False

    async def send_all(self, data: bytes) -> None:
        del data
        # pylint: disable=protected-access
        self.saw_token = self.token in self.instance._user_queries
        if self.fail:
            raise trio.BrokenResourceError("reset")
        if self.saw_token:
            _, future = self.instance._user_queries[self.token]
            future.set_result("ok")


def _query_instance(
    token: int, *, fail: bool = False
) -> tuple[ConnectionInstance, _RecordingStream]:
    instance = ConnectionInstance(_QueryParent())
    stream = _RecordingStream(instance, token, fail=fail)
    instance._stream = stream  # pylint: disable=protected-access
    return instance, stream


@pytest.mark.trio
@pytest.mark.unit
async def test_run_query_registers_before_send() -> None:
    """The response waiter exists before ``send_all`` yields to the reader."""
    instance, stream = _query_instance(9)

    assert await instance.run_query(Query(1, 9, None), noreply=False) == "ok"
    assert stream.saw_token is True


@pytest.mark.trio
@pytest.mark.unit
async def test_noreply_query_sends_without_a_waiter() -> None:
    """A noreply write does not occupy the response table."""
    instance, stream = _query_instance(9)

    assert await instance.run_query(Query(1, 9, None), noreply=True) is None
    assert stream.saw_token is False


@pytest.mark.trio
@pytest.mark.unit
async def test_send_failure_drops_pending_waiter() -> None:
    """A failed send removes the waiter the caller will not await."""
    instance, stream = _query_instance(9, fail=True)

    with pytest.raises(ReqlDriverError, match="Connection is closed"):
        await instance.run_query(Query(1, 9, None), noreply=False)

    assert stream.saw_token is True
    assert 9 not in instance._user_queries  # pylint: disable=protected-access
