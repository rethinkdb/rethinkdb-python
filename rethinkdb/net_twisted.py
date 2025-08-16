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

import struct
import time
from typing import Any, Dict, Generator, List, Optional, Type, Union, cast

from twisted.internet import defer, reactor
from twisted.internet.defer import (
    CancelledError,
    Deferred,
    DeferredQueue,
    inlineCallbacks,
)
from twisted.internet.endpoints import clientFromString
from twisted.internet.error import TimeoutError as TwistedTimeoutError
from twisted.internet.protocol import ClientFactory, Protocol

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


class DatabaseProtocol(Protocol):
    """Twisted protocol handling handshake and frame parsing for RethinkDB."""

    WAITING_FOR_HANDSHAKE = 0
    READY = 1

    def __init__(self, factory: "DatabaseProtoFactory") -> None:
        self.factory = factory
        self.state = DatabaseProtocol.WAITING_FOR_HANDSHAKE
        self._handlers = {
            DatabaseProtocol.WAITING_FOR_HANDSHAKE: self._handle_handshake,
            DatabaseProtocol.READY: self._handle_response,
        }

        self.buf = bytes()
        self.buf_expected_length = 0
        self.buf_token: Optional[int] = None

        self.wait_for_handshake = Deferred()

        self._open = True
        self._timeout_defer: Optional[Any] = None

    def connectionMade(self) -> None:
        # Send immediately the handshake.
        self.factory.handshake.reset()
        self.transport.write(self.factory.handshake.next_message(None))

        # Defer a timer which will callback when timed out and errback the
        # wait_for_handshake. Otherwise, it will be cancelled in
        # handleHandshake.
        # pylint: disable=no-member
        self._timeout_defer = reactor.callLater(
            self.factory.timeout, self._handle_handshake_timeout
        )

    def connectionLost(self, reason: Any) -> None:  # pylint: disable=signature-differs
        self._open = False

    def _handle_handshake_timeout(self) -> None:
        # If we are here, we failed to do the handshake before the timeout.
        # We close the connection and raise an ReqlTimeoutError in the
        # wait_for_handshake deferred.
        self._open = False
        self.transport.loseConnection()
        self.wait_for_handshake.errback(ReqlTimeoutError())

    def _handle_handshake(self, data: bytes) -> None:
        try:
            self.buf += data
            while True:
                end_index = self.buf.find(b"\0")
                if end_index != -1:
                    response = self.buf[:end_index]
                    self.buf = self.buf[end_index + 1 :]
                    request = self.factory.handshake.next_message(response)

                    if request is None:
                        # We're now ready to work with real data.
                        self.state = DatabaseProtocol.READY
                        # We cancel the scheduled timeout.
                        if self._timeout_defer:
                            self._timeout_defer.cancel()
                        # We callback our wait_for_handshake.
                        self.wait_for_handshake.callback(None)
                    elif request != "":
                        self.transport.write(request)
                else:
                    break
        except Exception as exc:  # pylint: disable=broad-exception-caught
            self.wait_for_handshake.errback(exc)

    def _handle_response(self, data: bytes) -> None:
        # If we have more than one response, we should handle all of them.
        self.buf += data
        while True:
            # 1. Read the header, until we read the length of the awaited payload.
            if self.buf_expected_length == 0:
                if len(self.buf) >= 12:
                    token, length = struct.unpack("<qL", self.buf[:12])
                    self.buf_token = token
                    self.buf_expected_length = length
                    self.buf = self.buf[12:]
                else:
                    # We quit the function, it is impossible to have read the
                    # entire payload at this point.
                    return

            # 2. Buffer the data, until the size of the data match the expected
            # length provided by the header.
            if len(self.buf) < self.buf_expected_length:
                return

            self.factory.response_handler(
                self.buf_token, self.buf[: self.buf_expected_length]
            )
            self.buf = self.buf[self.buf_expected_length :]
            self.buf_token = None
            self.buf_expected_length = 0

    def dataReceived(self, data: bytes) -> None:
        try:
            if self._open:
                self._handlers[self.state](data)
        except Exception as e:
            self.transport.loseConnection()
            raise ReqlDriverError(
                f"Driver failed to handle received data. Error: {e}. Dropping the connection."
            ) from e


class DatabaseProtoFactory(ClientFactory):
    """Protocol factory for DatabaseProtocol."""

    protocol = DatabaseProtocol

    def __init__(self, timeout: int, response_handler: Any, handshake: Any) -> None:
        self.timeout = timeout
        self.handshake = handshake
        self.response_handler = response_handler

    def startedConnecting(self, connector: Any) -> None:
        """Called when a connection is started."""

    def buildProtocol(self, addr: Any) -> DatabaseProtocol:
        """Builds the protocol."""
        p = DatabaseProtocol(self)
        return p

    def clientConnectionLost(self, connector: Any, reason: Any) -> None:
        """Called when the connection is lost."""

    def clientConnectionFailed(self, connector: Any, reason: Any) -> None:
        """Called when the connection fails."""


class CursorItems(DeferredQueue):
    """Queue-like container used by TwistedCursor to buffer items."""

    def __init__(self) -> None:
        super().__init__()

    def cancel_getters(self, err: Exception) -> None:
        """
        Cancel all waiters.
        """
        for waiter in self.waiting[:]:
            if not waiter.called:
                waiter.errback(err)
            self.waiting.remove(waiter)

    def extend(self, data: List[Any]) -> None:
        """Extend the queue with the given data."""
        for k in data:
            self.put(k)

    def __len__(self) -> int:
        """Return the number of items in the queue."""
        return len(self.pending)

    def __getitem__(self, index: int) -> Any:
        """Return the item at the given index."""
        return self.pending[index]

    def __iter__(self):
        return iter(self.pending)


class TwistedCursor(Cursor):
    """Cursor implementation backed by Twisted Deferreds."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("items_type", CursorItems)
        super().__init__(*args, **kwargs)
        self.waiting: List["Deferred[Any]"] = []

    def extend(self, res_buf: bytes) -> None:
        super().extend(res_buf)

        if self.error is not None:
            self.items.cancel_getters(self.error)

        for d in self.waiting[:]:
            d.callback(None)
            self.waiting.remove(d)

    @staticmethod
    def _empty_error() -> Type[ReqlCursorEmpty]:
        return ReqlCursorEmpty

    def fetch_next(self, wait: Union[bool, float] = True) -> "Deferred[bool]":
        """Fetch the next item, waiting if necessary."""
        timeout = Cursor._wait_to_timeout(wait)
        deadline = None if timeout is None else time.time() + timeout

        def wait_canceller(d: "Deferred[Any]") -> None:
            d.errback(ReqlTimeoutError())

        wait_defer: "Deferred[Any]" = Deferred(canceller=wait_canceller)

        def check_completion() -> None:
            if len(self.items) > 0 or self.error is not None:
                if not wait_defer.called:
                    wait_defer.callback(not self._is_empty() or self._has_error())
            elif deadline is not None and time.time() > deadline:
                if not wait_defer.called:
                    wait_defer.errback(ReqlTimeoutError())
            else:
                self._maybe_fetch_batch()
                # pylint: disable=no-member
                reactor.callLater(0.01, check_completion)

        check_completion()
        return wait_defer

    def _has_error(self) -> bool:
        return self.error is not None and not isinstance(self.error, ReqlCursorEmpty)

    def _is_empty(self) -> bool:
        return isinstance(self.error, ReqlCursorEmpty) and len(self.items) == 0

    @inlineCallbacks
    def _get_next(self, timeout: Optional[float] = None) -> "Deferred[Any]":
        if len(self.items) == 0 and self.error is not None:
            return defer.fail(self.error)

        def raise_timeout(errback: Any) -> None:
            if isinstance(errback.value, CancelledError):
                raise ReqlTimeoutError()
            raise errback.value

        item_defer = self.items.get()

        if timeout is not None:
            item_defer.addErrback(raise_timeout)
            # pylint: disable=no-member
            reactor.callLater(timeout, item_defer.cancel)

        self._maybe_fetch_batch()
        return item_defer


class ConnectionInstance:
    """Manage a Twisted transport and map responses to cursors/deferreds."""

    def __init__(self, parent: "Connection", start_reactor: bool = False) -> None:
        self._parent = parent
        self._closing = False
        self._connection: Optional[DatabaseProtocol] = None
        self._user_queries: Dict[int, tuple] = {}
        self.__cursor_cache: Dict[int, TwistedCursor] = {}

        if start_reactor:
            # pylint: disable=no-member
            reactor.run()

    def client_port(self) -> Optional[int]:
        """Return the client port for this connection."""
        if self.is_open() and self._connection:
            return self._connection.transport.getHost().port
        return None

    def client_address(self) -> Optional[str]:
        """Return the client address for this connection."""
        if self.is_open() and self._connection:
            return self._connection.transport.getHost().host
        return None

    def _handle_response(self, token: int, data: bytes) -> None:
        """Handle a response from the server."""
        try:
            cursor = self.cursor_cache.get(token)
            if cursor is not None:
                cursor.extend(data)
            elif token in self._user_queries:
                query, deferred = self._user_queries[token]
                res = Response(token, data, self._parent.get_json_decoder(query))
                if res.response_type == PbResponse.ResponseType.SUCCESS_ATOM:
                    deferred.callback(maybe_profile(res.data[0], res))
                elif res.response_type in (
                    PbResponse.ResponseType.SUCCESS_SEQUENCE,
                    PbResponse.ResponseType.SUCCESS_PARTIAL,
                ):
                    cursor = TwistedCursor(self, query, res)
                    deferred.callback(maybe_profile(cast(Any, cursor), res))
                elif res.response_type == PbResponse.ResponseType.WAIT_COMPLETE:
                    deferred.callback(None)
                elif res.response_type == PbResponse.ResponseType.SERVER_INFO:
                    deferred.callback(res.data[0])
                else:
                    deferred.errback(res.make_error(query))
                del self._user_queries[token]
            elif not self._closing:
                raise ReqlDriverError("Unexpected response received.")
        except Exception as e:  # pylint: disable=broad-exception-caught
            if not self._closing:
                self.close(exception=e)

    @inlineCallbacks
    def _connect_timeout(
        self, factory: DatabaseProtoFactory, timeout: int
    ) -> Generator:
        """Connect with a timeout."""
        try:
            # TODO: use ssl options
            # TODO: this doesn't work for literal IPv6 addresses like '::1'
            args = f"tcp:{self._parent.host}:{self._parent.port}"

            if timeout is not None:
                args = args + f":timeout={timeout}"

            endpoint = clientFromString(reactor, args)
            proto = yield endpoint.connect(factory)
            return proto
        except TwistedTimeoutError as exc:
            raise ReqlTimeoutError() from exc

    @inlineCallbacks
    def connect(self, timeout: int) -> Generator:
        """Establish a connection to the server."""
        factory = DatabaseProtoFactory(
            timeout, self._handle_response, self._parent.handshake
        )

        # We connect to the server, and send the handshake payload.
        connection = None
        try:
            connection = yield self._connect_timeout(factory, timeout)
        except Exception as e:  # pylint: disable=broad-exception-caught
            raise ReqlDriverError(
                f"Could not connect to {self._parent.host}:{self._parent.port}. Error: {e}"
            ) from e

        # Now, we need to wait for the handshake.
        try:
            if connection:
                yield connection.wait_for_handshake
        except ReqlAuthError:
            raise
        except ReqlTimeoutError as exc:
            raise ReqlTimeoutError(self._parent.host, self._parent.port) from exc
        except Exception as e:  # pylint: disable=broad-exception-caught
            error_msg = (
                f"Handshake interrupted with {self._parent.host}:{self._parent.port}."
                f" Error: {e}"
            )
            raise ReqlDriverError(error_msg) from e

        self._connection = connection

        return self._parent

    @property
    def cursor_cache(self) -> Dict[int, "TwistedCursor"]:
        """
        Return the cursor's cache.
        """
        return self.__cursor_cache

    def reset_cursor_cache(self) -> None:
        """
        Reset the cursor cache to drop cached items.
        """
        self.__cursor_cache = {}

    def is_open(self) -> bool:
        """Check if the connection is open."""
        return (
            self._connection is not None
            and self._connection._open  # pylint: disable=protected-access
        )

    def close(
        self,
        noreply_wait: bool = False,
        token: Optional[int] = None,
        exception: Optional[Exception] = None,
    ) -> "Deferred[Any]":
        """Close the connection."""
        d: "Deferred[Any]" = defer.succeed(None)
        self._closing = True
        error_message = "Connection is closed"
        if exception is not None:
            error_message = f"Connection is closed (reason: {exception})"

        for cursor in list(self.cursor_cache.values()):
            cursor.raise_error(error_message)

        for _, deferred in list(self._user_queries.values()):
            if not deferred.called:
                deferred.errback(ReqlDriverError(error_message))

        self._user_queries = {}
        self.reset_cursor_cache()

        if noreply_wait and token is not None:
            noreply = Query(PbQuery.QueryType.NOREPLY_WAIT, token, None, None)
            d = self.run_query(noreply, False)

        def close_connection(res: Any) -> Any:
            if self._connection:
                self._connection.transport.loseConnection()
            return res

        return d.addBoth(close_connection)

    @inlineCallbacks
    def run_query(self, query: Query, noreply: bool) -> Generator:
        """Run a query on the connection."""
        response_defer = Deferred()
        if not noreply:
            self._user_queries[query.token] = (query, response_defer)
        # Send the query
        if self._connection:
            self._connection.transport.write(
                query.serialize(self._parent.get_json_encoder(query))
            )

        if noreply:
            return None
        res = yield response_defer
        return res


class Connection(ConnectionBase):
    """A RethinkDB connection using the Twisted event loop."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(ConnectionInstance, *args, **kwargs)

    @inlineCallbacks
    def reconnect(
        self, noreply_wait: bool = True, timeout: Optional[int] = None
    ) -> Generator:
        yield self.close(noreply_wait)
        res = yield super().reconnect(noreply_wait, timeout)
        return res

    @inlineCallbacks
    def close(self, *args: Any, **kwargs: Any) -> Generator:
        res = yield super().close(*args, **kwargs) or None
        return res

    @inlineCallbacks
    def noreply_wait(self) -> Generator:
        res = yield super().noreply_wait()
        return res

    @inlineCallbacks
    def server(self) -> Generator:
        res = yield super().server()
        return res

    @inlineCallbacks
    def _start(self, term: Any, **global_optargs: Any) -> Generator:
        res = yield super().start(term, **global_optargs)
        return res

    @inlineCallbacks
    def _continue(self, cursor: "TwistedCursor") -> Generator:
        res = yield super().resume(cursor)
        return res

    @inlineCallbacks
    def _stop(self, cursor: "TwistedCursor") -> Generator:
        res = yield super().stop(cursor)
        return res
