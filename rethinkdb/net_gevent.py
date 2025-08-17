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

import errno
import logging
import ssl
import struct

import gevent  # type: ignore[import-untyped]
from gevent import socket  # type: ignore[import-untyped]
from gevent.event import AsyncResult, Event  # type: ignore[import-untyped]
from gevent.lock import Semaphore  # type: ignore[import-untyped]

from rethinkdb.errors import (
    ReqlAuthError,
    ReqlCursorEmpty,
    ReqlDriverError,
    ReqlTimeoutError,
)
from rethinkdb.net import Connection as ConnectionBase
from rethinkdb.net import Cursor, Query, Response
from rethinkdb.net import SocketWrapper as SocketWrapperBase
from rethinkdb.net import maybe_profile
from rethinkdb.ql2_pb2 import Query as PbQuery
from rethinkdb.ql2_pb2 import Response as PbResponse

__all__ = ["Connection"]


class GeventCursor(Cursor):
    """Gevent-based cursor implementation for RethinkDB queries."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.new_response = Event()

    def __iter__(self):
        return self

    def __next__(self):
        return self._get_next(None)

    @staticmethod
    def _empty_error():
        """Return the empty error exception class."""
        return ReqlCursorEmpty()

    def _extend(self, res_buf):
        """Extend the cursor with new response data."""
        # pylint: disable=no-member
        super()._extend(res_buf)
        # pylint: enable=no-member
        self.new_response.set()
        self.new_response.clear()

    def _get_next(self, timeout):  # pylint: disable=signature-differs
        with gevent.Timeout(timeout, ReqlTimeoutError()):
            self._maybe_fetch_batch()
            while len(self.items) == 0:
                if self.error is not None:
                    raise self.error
                self.new_response.wait()
            return self.items.popleft()


class SocketWrapper(SocketWrapperBase):
    """Gevent-based socket wrapper for RethinkDB connections."""

    # pylint: disable=super-init-not-called,too-many-branches,too-many-statements
    def __init__(self, parent):
        self.host = parent._parent.host
        self.port = parent._parent.port
        self._read_buffer = None
        self._socket = None
        self.ssl = parent._parent.ssl

        try:
            self._socket = socket.create_connection((self.host, self.port))
            self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

            if len(self.ssl) > 0:
                try:
                    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                    ssl_context.options |= ssl.OP_NO_SSLv2
                    ssl_context.options |= ssl.OP_NO_SSLv3
                    ssl_context.verify_mode = ssl.CERT_REQUIRED
                    ssl_context.check_hostname = True
                    ssl_context.load_verify_locations(self.ssl["ca_certs"])
                    self._socket = ssl_context.wrap_socket(
                        self._socket, server_hostname=self.host
                    )
                except IOError as exc:
                    self._socket.close()
                    raise ReqlDriverError(
                        f"SSL handshake failed (see server log for more information): {exc}"
                    ) from exc

            parent._parent.handshake.reset()
            response = None
            while True:
                request = parent._parent.handshake.next_message(response)
                if request is None:
                    break

                # This may happen in the `V1_0` protocol where we send two requests as
                # an optimization, then need to read each separately
                if request != "":
                    self.sendall(request)

                # The response from the server is a null-terminated string
                response = b""
                while True:
                    char = self.recvall(1)
                    if char == b"\0":
                        break
                    response += char
        except (ReqlAuthError, ReqlTimeoutError):
            self.close()
            raise
        except ReqlDriverError as exc:
            self.close()
            error = (
                str(exc)
                .replace("receiving from", "during handshake with")
                .replace("sending to", "during handshake with")
            )
            raise ReqlDriverError(error) from exc
        except Exception as exc:
            self.close()
            raise ReqlDriverError(
                f"Could not connect to {self.host}:{self.port}. Error: {exc}"
            ) from exc

    def is_open(self):
        """
        Return if the connection is open.
        """
        return self._socket is not None

    def close(self):
        if self._socket is not None:
            try:
                self._socket.shutdown(socket.SHUT_RDWR)
                self._socket.close()
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logging.error("Error closing socket: %s", str(exc))
            finally:
                self._socket = None

    def recvall(self, length, deadline=None):
        """Receive all data from the socket."""
        res = b"" if self._read_buffer is None else self._read_buffer
        while len(res) < length:
            while True:
                try:
                    chunk = self._socket.recv(length - len(res))
                    break
                except ReqlTimeoutError:
                    raise
                except IOError as exc:
                    if exc.errno == errno.ECONNRESET:
                        self.close()
                        raise ReqlDriverError("Connection is closed.") from exc
                    if exc.errno != errno.EINTR:
                        self.close()
                        raise ReqlDriverError(
                            f"Connection interrupted receiving from {self.host}:{self.port} - {exc}"
                        ) from exc
                except Exception as exc:
                    self.close()
                    raise ReqlDriverError(
                        f"Error receiving from {self.host}:{self.port} - {exc}"
                    ) from exc
            if len(chunk) == 0:
                self.close()
                raise ReqlDriverError("Connection is closed.")
            res += chunk
        return res

    def sendall(self, data):
        """Send all data to the socket."""
        offset = 0
        while offset < len(data):
            try:
                offset += self._socket.send(data[offset:])
            except IOError as exc:
                if exc.errno == errno.ECONNRESET:
                    self.close()
                    raise ReqlDriverError("Connection is closed.") from exc
                if exc.errno != errno.EINTR:
                    self.close()
                    raise ReqlDriverError(
                        f"Connection interrupted sending to {self.host}:{self.port} - {exc}"
                    ) from exc
            except Exception as exc:
                self.close()
                raise ReqlDriverError(
                    f"Error sending to {self.host}:{self.port} - {exc}"
                ) from exc


class ConnectionInstance:
    """Gevent-based connection instance for RethinkDB."""

    def __init__(self, parent, io_loop=None):
        # pylint: disable=unused-argument
        self._parent = parent
        self._closing = False
        self._user_queries = {}
        self._cursor_cache = {}

        self._write_mutex = Semaphore()
        self._socket = None

    def connect(self, timeout):
        """Connect to the RethinkDB server."""
        with gevent.Timeout(
            timeout, ReqlTimeoutError(self._parent.host, self._parent.port)
        ):
            self._socket = SocketWrapper(self)

        # Start a parallel coroutine to perform reads
        gevent.spawn(self._reader)
        return self._parent

    def is_open(self):
        """Check if the connection is open."""
        return self._socket is not None and self._socket.is_open()

    def close(self, noreply_wait=False, token=None, exception=None):
        """Close the connection."""
        self._closing = True
        if exception is not None:
            err_message = f"Connection is closed ({exception})."
        else:
            err_message = "Connection is closed."

        # Cursors may remove themselves when errored, so copy a list of them
        for cursor in list(self._cursor_cache.values()):
            # pylint: disable=protected-access
            cursor._error(err_message)
            # pylint: enable=protected-access

        for _query, async_res in iter(self._user_queries.values()):
            async_res.set_exception(ReqlDriverError(err_message))

        self._user_queries = {}
        self._cursor_cache = {}

        if noreply_wait:
            noreply = Query(PbQuery.QueryType.NOREPLY_WAIT, token, None, None)
            self.run_query(noreply, False)

        try:
            self._socket.close()
        except OSError:
            pass

    def run_query(self, query, noreply):
        """Run a query on the connection."""
        self._write_mutex.acquire()

        try:
            self._socket.sendall(query.serialize(self._parent.get_json_encoder(query)))
        finally:
            self._write_mutex.release()

        if noreply:
            return None

        async_res = AsyncResult()
        self._user_queries[query.token] = (query, async_res)
        return async_res.get()

    def _reader(self):
        """Read responses from the socket and dispatch them to cursors or queries."""
        try:
            while True:
                buf = self._socket.recvall(12)
                (
                    token,
                    length,
                ) = struct.unpack("<qL", buf)
                buf = self._socket.recvall(length)

                cursor = self._cursor_cache.get(token)
                if cursor is not None:
                    # pylint: disable=protected-access
                    cursor._extend(buf)
                    # pylint: enable=protected-access
                elif token in self._user_queries:
                    # Do not pop the query from the dict until later, so
                    # we don't lose track of it in case of an exception
                    query, async_res = self._user_queries[token]
                    res = Response(token, buf, self._parent.get_json_decoder(query))
                    if res.response_type == PbResponse.ResponseType.SUCCESS_ATOM:
                        async_res.set(maybe_profile(res.data[0], res))
                    elif res.response_type in (
                        PbResponse.ResponseType.SUCCESS_SEQUENCE,
                        PbResponse.ResponseType.SUCCESS_PARTIAL,
                    ):
                        cursor = GeventCursor(self, query, res)
                        async_res.set(maybe_profile(cursor, res))
                    elif res.response_type == PbResponse.ResponseType.WAIT_COMPLETE:
                        async_res.set(None)
                    else:
                        async_res.set_exception(res.make_error(query))
                    del self._user_queries[token]
                elif not self._closing:
                    raise ReqlDriverError("Unexpected response received.")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            if not self._closing:
                self.close(exception=exc)


class Connection(ConnectionBase):
    """Gevent-based RethinkDB connection."""

    def __init__(self, *args, **kwargs):
        super().__init__(ConnectionInstance, *args, **kwargs)
