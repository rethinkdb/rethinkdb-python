# Copyright 2016-2025 RethinkDB
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
#
# This file incorporates work covered by the following copyright:
# Copyright 2010-2016 RethinkDB, all rights reserved.

import asyncio
import socket
import struct
from typing import Any, Dict, Optional, cast

from tornado import gen, iostream
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from tornado.tcpclient import TCPClient

from rethinkdb.errors import (
    ReqlAuthError,
    ReqlCursorEmpty,
    ReqlDriverError,
    ReqlTimeoutError,
)
from rethinkdb.net import Connection as ConnectionBase
from rethinkdb.net import Cursor, Query, Response, maybe_profile
from rethinkdb.ql2_pb2 import Query as PbQuery
from rethinkdb.ql2_pb2 import Response as PbResponse

__all__ = ["Connection"]


async def with_absolute_timeout(deadline, generator, **kwargs):
    """
    Run a generator with an absolute timeout.
    """
    if deadline is None:
        res = await generator
    else:
        try:
            res = await gen.with_timeout(deadline, generator, **kwargs)
        except gen.TimeoutError as exc:
            raise ReqlTimeoutError() from exc
    return res


# The Tornado implementation of the Cursor object:
# The `new_response` Future notifies any waiting coroutines that the can attempt
# to grab the next result.  In addition, the waiting coroutine will schedule a
# timeout at the given deadline (if provided), at which point the future will be
# errored.
class TornadoCursor(Cursor):
    """
    The Tornado implementation of the Cursor object.
    """

    def __init__(self, *args, **kwargs):
        Cursor.__init__(self, *args, **kwargs)
        self.new_response = Future()

    def __aiter__(self):
        """Make the cursor async iterable."""
        return self

    async def __anext__(self):
        """Get the next item from the cursor."""
        if await self.fetch_next():
            return await self.next()

        raise StopAsyncIteration

    def extend(self, res_buf):
        Cursor.extend(self, res_buf)
        self.new_response.set_result(True)
        self.new_response = Future()

    async def fetch_next(self, wait=True):
        """
        Convenience function so users know when they've hit the end of the cursor
        without having to catch an exception
        """
        timeout = Cursor._wait_to_timeout(wait)
        # pylint: disable=protected-access
        deadline = None if timeout is None else self.conn._io_loop.time() + timeout
        while len(self.items) == 0 and self.error is None:
            self._maybe_fetch_batch()
            await with_absolute_timeout(deadline, self.new_response)
        # If there is a (non-empty) error to be received, we return True, so the
        # user will receive it on the next `next` call.
        return len(self.items) != 0 or not isinstance(self.error, ReqlCursorEmpty)

    @staticmethod
    def _empty_error():
        # We do not have ReqlCursorEmpty inherit from StopIteration as that interferes
        # with Tornado's gen.coroutine and is the equivalent of gen.Return(None).
        return ReqlCursorEmpty

    def _get_next(self, timeout=None):
        """
        Get the next item from the cursor.
        """
        # pylint: disable=protected-access
        deadline = None if timeout is None else self.conn._io_loop.time() + timeout
        while len(self.items) == 0:
            self._maybe_fetch_batch()
            if self.error is not None:
                raise self.error
            # Use asyncio.create_task for non-async method
            asyncio.create_task(with_absolute_timeout(deadline, self.new_response))
        return self.items.popleft()


class ConnectionInstance:
    """
    The Tornado implementation of the Connection object.
    """

    def __init__(self, parent: "Connection", io_loop: Optional[IOLoop] = None):
        self._parent = parent
        self._closing = False
        self._user_queries: Dict[int, Any] = {}
        self._cursor_cache: Dict[int, TornadoCursor] = {}
        self._ready: "Future[Any]" = Future()
        self._io_loop: IOLoop = io_loop or IOLoop.current()
        self._stream: Optional[iostream.IOStream] = None
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def client_port(self) -> Optional[int]:
        """
        Get the client port of the connection.
        """
        if self.is_open():
            return self._socket.getsockname()[1]
        return None

    def client_address(self) -> Optional[str]:
        """
        Get the client address of the connection.
        """
        if self.is_open():
            return self._socket.getsockname()[0]
        return None

    async def connect(self, timeout: Optional[int] = None) -> "Connection":
        """
        Connect to the RethinkDB server.
        """
        deadline = None if timeout is None else self._io_loop.time() + timeout

        try:
            stream_future = self._create_connection()
            self._stream = await with_absolute_timeout(
                deadline, stream_future, quiet_exceptions=(iostream.StreamClosedError)
            )
        except Exception as err:
            raise ReqlDriverError(
                f"Could not connect to {self._parent.host}:{self._parent.port}. "
                f"Error: {err}"
            ) from err

        self._stream.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._stream.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        try:
            await self._perform_handshake(deadline)
        except (ReqlAuthError, ReqlTimeoutError):
            self._safe_close_stream()
            raise
        except Exception as err:
            self._safe_close_stream()
            raise ReqlDriverError(
                f"Connection interrupted during handshake with "
                f"{self._parent.host}:{self._parent.port}. Error: {err}"
            ) from err

        # Start a parallel function to perform reads
        self._io_loop.add_callback(self._reader)
        return self._parent

    def _create_connection(self):
        """Create the connection future."""
        if len(self._parent.ssl) > 0:
            ssl_options = {}
            if self._parent.ssl["ca_certs"]:
                ssl_options["ca_certs"] = self._parent.ssl["ca_certs"]
                ssl_options["cert_reqs"] = 2  # ssl.CERT_REQUIRED
            return TCPClient().connect(
                self._parent.host, self._parent.port, ssl_options=ssl_options
            )
        return TCPClient().connect(self._parent.host, self._parent.port)

    async def _perform_handshake(self, deadline):
        """Perform the handshake with the server."""
        self._parent.handshake.reset()
        response = None
        while True:
            request = self._parent.handshake.next_message(response)
            if request is None:
                break
            # This may happen in the `V1_0` protocol where we send two requests as
            # an optimization, then need to read each separately
            if request != "":
                self._stream.write(request)

            response = await with_absolute_timeout(
                deadline,
                self._stream.read_until(b"\0"),
                quiet_exceptions=(iostream.StreamClosedError),
            )
            response = response[:-1]

    def _safe_close_stream(self):
        """Safely close the stream."""
        try:
            if self._stream is not None:
                self._stream.close()
        except iostream.StreamClosedError:
            pass

    def is_open(self) -> bool:
        """
        Check if the connection is open.
        """
        return self._stream is not None and not self._stream.closed()

    async def close(
        self,
        noreply_wait: bool = False,
        token: Optional[int] = None,
        exception: Optional[Exception] = None,
    ):
        """
        Close the connection.
        """
        self._closing = True
        if exception is not None:
            err_message = f"Connection is closed ({exception})."
        else:
            err_message = "Connection is closed."

        # Cursors may remove themselves when errored, so copy a list of them
        for cursor in list(self._cursor_cache.values()):
            cursor.raise_error(err_message)

        for _, future in iter(self._user_queries.values()):
            future.set_exception(ReqlDriverError(err_message))

        self._user_queries = {}
        self._cursor_cache = {}

        if noreply_wait and token is not None:
            noreply = Query(PbQuery.QueryType.NOREPLY_WAIT, token, None, None)
            await self.run_query(noreply, False)

        if self._stream is not None:
            try:
                self._stream.close()
            except iostream.StreamClosedError:
                pass
        return None

    async def run_query(self, query: Query, noreply: bool) -> Optional[Any]:
        """
        Run a query on the connection.
        """
        if self._stream is None:
            raise ReqlDriverError("Connection is closed.")

        await self._stream.write(query.serialize(self._parent.get_json_encoder(query)))
        if noreply:
            return None

        response_future: Future = Future()
        self._user_queries[query.token] = (query, response_future)
        res = await response_future
        return res

    # The _reader coroutine runs in its own context at the top level of the
    # Tornado.IOLoop it was created with.  It runs in parallel, reading responses
    # off of the socket and forwarding them to the appropriate Future or Cursor.
    # This is shut down as a consequence of closing the stream, or an error in the
    # socket/protocol from the server.  Unexpected errors in this coroutine will
    # close the ConnectionInstance and be passed to any open Futures or Cursors.
    async def _reader(self) -> None:
        try:
            while self._stream is not None:
                buf = await self._stream.read_bytes(12)
                (
                    token,
                    length,
                ) = struct.unpack("<qL", buf)
                buf = await self._stream.read_bytes(length)

                await self._handle_response(token, buf)
        except (
            ReqlDriverError,
            ReqlAuthError,
            ReqlTimeoutError,
            ReqlCursorEmpty,
        ) as exc:
            if not self._closing:
                await self.close(exception=exc)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            if not self._closing:
                await self.close(exception=exc)

    async def _handle_response(self, token: int, buf: bytes) -> None:
        """Handle a response from the server."""
        cursor = self._cursor_cache.get(token)
        if cursor is not None:
            cursor.extend(buf)
            return

        if token not in self._user_queries:
            if not self._closing:
                raise ReqlDriverError("Unexpected response received.")
            return

        # Handle user query response
        query, future = self._user_queries[token]
        res = Response(token, buf, self._parent.get_json_decoder(query))

        if res.response_type == PbResponse.ResponseType.SUCCESS_ATOM:
            future.set_result(maybe_profile(res.data[0], res))
        elif res.response_type in (
            PbResponse.ResponseType.SUCCESS_SEQUENCE,
            PbResponse.ResponseType.SUCCESS_PARTIAL,
        ):
            cursor = TornadoCursor(self, query, res)
            future.set_result(maybe_profile(cast(Any, cursor), res))
        elif res.response_type == PbResponse.ResponseType.WAIT_COMPLETE:
            future.set_result(None)
        elif res.response_type == PbResponse.ResponseType.SERVER_INFO:
            future.set_result(res.data[0])
        else:
            future.set_exception(res.make_error(query))

        del self._user_queries[token]


# Wrap functions from the base connection class that may throw - these will
# put any exception inside a Future and return it.
class Connection(ConnectionBase):
    """
    A RethinkDB connection using the Tornado event loop.
    """

    def __init__(self, *args, **kwargs):
        ConnectionBase.__init__(self, ConnectionInstance, *args, **kwargs)

    async def reconnect(  # pylint: disable=invalid-overridden-method
        self, noreply_wait=True, timeout=None
    ):
        # We close before reconnect so reconnect doesn't try to close us
        # and then fail to return the Future (this is a little awkward).
        await self.close(noreply_wait)
        res = await ConnectionBase.reconnect(self, noreply_wait, timeout)
        return res

    async def close(
        self, *_args, **kwargs
    ):  # pylint: disable=invalid-overridden-method
        if self.is_open() is False:
            return

        self.check_open()

        if self._instance is not None:
            token = self._new_token() if kwargs.get("noreply_wait", True) else None
            await self._instance.close(**kwargs, token=token)
            self._instance = None

    async def noreply_wait(self):  # pylint: disable=invalid-overridden-method
        res = await ConnectionBase.noreply_wait(self)
        return res

    async def server(self):  # pylint: disable=invalid-overridden-method
        res = await ConnectionBase.server(self)
        return res

    async def start(  # pylint: disable=invalid-overridden-method
        self, term, **global_optargs
    ):
        res = await ConnectionBase.start(self, term, **global_optargs)
        return res

    async def resume(self, cursor):  # pylint: disable=invalid-overridden-method
        res = await ConnectionBase.resume(self, cursor)
        return res

    async def stop(self, cursor):  # pylint: disable=invalid-overridden-method
        res = await ConnectionBase.stop(self, cursor)
        return res
