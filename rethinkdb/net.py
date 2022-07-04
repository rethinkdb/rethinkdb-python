# Copyright 2021 RethinkDB
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

# TODO: Refactor the net module and split into multiple files
# pylint: disable=too-many-lines

import collections
import errno
import logging
import numbers
import pprint
import socket
import ssl
import struct
import time
from typing import Any, Dict, List, Optional, Type, Union
from urllib.parse import parse_qs, urlparse

from rethinkdb.ast import DB, ReqlQuery, expr
from rethinkdb.encoder import ReqlDecoder, ReqlEncoder
from rethinkdb.errors import (
    ReqlAuthError,
    ReqlCursorEmpty,
    ReqlDriverError,
    ReqlError,
    ReqlInternalError,
    ReqlNonExistenceError,
    ReqlOpFailedError,
    ReqlOpIndeterminateError,
    ReqlPermissionError,
    ReqlQueryLogicError,
    ReqlResourceLimitError,
    ReqlRuntimeError,
    ReqlServerCompileError,
    ReqlTimeoutError,
    ReqlUserError,
)
from rethinkdb.handshake import BaseHandshake, HandshakeV1_0
from rethinkdb.ql2_pb2 import Query as PbQuery
from rethinkdb.ql2_pb2 import Response as PbResponse
from rethinkdb.repl import Repl

__all__ = [
    "Connection",
    "Cursor",
    "DEFAULT_PORT",
    "DefaultConnection",
    "make_connection",
]

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 28015
DEFAULT_USER = "admin"
DEFAULT_TIMEOUT = 20

logger = logging.getLogger(__name__)


def maybe_profile(
    value: Union["DefaultCursor", str], res: "Response"
) -> Union["DefaultCursor", str]:
    """
    If the profile is set for the response, return a dict composed of the
    original value and profile.
    """

    if res.profile is not None:  # type: ignore
        return {"value": value, "profile": res.profile}  # type: ignore

    return value


class Query:  # pylint: disable=too-few-public-methods
    """
    Query sent to the database.
    """

    __slot__ = (
        "query_type",
        "token",
        "term",
        "kwargs",
        "_json_decoder",
        "_json_encoder",
    )

    def __init__(
        self,
        query_type: int,
        token: int,
        term_type: Optional[ReqlQuery],
        kwargs: Optional[Dict[str, Any]] = None,
    ):
        self.query_type = query_type
        self.token = token
        self.term_type = term_type
        self.kwargs = kwargs or {}

        self.json_decoder = self.kwargs.pop("json_decoder", None)
        self.json_encoder = self.kwargs.pop("json_encoder", None)

    def serialize(self, reql_encoder: ReqlEncoder = ReqlEncoder()) -> bytes:
        """
        Serialize Query using the Reql encoder.
        """

        message: List[Union[PbQuery.QueryType, ReqlQuery, int]] = [self.query_type]

        if self.term_type is not None:
            message.append(self.term_type)

        if self.kwargs is not None:
            message.append(expr(self.kwargs))

        query_str = reql_encoder.encode(message).encode("utf-8")
        query_header = struct.pack("<QL", self.token, len(query_str))

        return query_header + query_str


class Response:  # pylint: disable=too-few-public-methods
    """
    Response received from the DB.
    """

    def __init__(
        self,
        token: int,
        json_response: bytes,
        reql_decoder: ReqlDecoder = ReqlDecoder(),
    ):
        response = reql_decoder.decode(json_response.decode())

        self.token: int = token
        self.response_type: int = response["t"]
        self.data: list = response["r"]
        self.backtrace: Optional[List[int]] = response.get("b", None)
        self.profile = response.get("p", None)
        self.error_type: Optional[int] = response.get("e", None)

    def make_error(self, query: Query) -> ReqlError:
        """
        Compose an error response from the query and the response
        received, from the database. In case the response returned by
        the server is unknown to the client, a `ReqlDriverError` will
        return.
        """

        if self.error_type is None:
            raise ReqlRuntimeError("Invalid error type received")

        error: ReqlError = ReqlDriverError(
            f"Unknown Response type {self.response_type} encountered in a response."
        )

        if self.response_type == PbResponse.ResponseType.CLIENT_ERROR:
            error = ReqlDriverError(self.data[0], query.term_type, self.backtrace)
        elif self.response_type == PbResponse.ResponseType.COMPILE_ERROR:
            error = ReqlServerCompileError(
                self.data[0], query.term_type, self.backtrace
            )
        elif self.response_type == PbResponse.ResponseType.RUNTIME_ERROR:
            runtime_error_type_mapping = {
                PbResponse.ErrorType.INTERNAL: ReqlInternalError,
                PbResponse.ErrorType.RESOURCE_LIMIT: ReqlResourceLimitError,
                PbResponse.ErrorType.QUERY_LOGIC: ReqlQueryLogicError,
                PbResponse.ErrorType.NON_EXISTENCE: ReqlNonExistenceError,
                PbResponse.ErrorType.OP_FAILED: ReqlOpFailedError,
                PbResponse.ErrorType.OP_INDETERMINATE: ReqlOpIndeterminateError,
                PbResponse.ErrorType.USER: ReqlUserError,
                PbResponse.ErrorType.PERMISSION_ERROR: ReqlPermissionError,
            }

            runtime_error_type = runtime_error_type_mapping.get(
                self.error_type, ReqlRuntimeError
            )

            error = runtime_error_type(self.data[0], query.term_type, self.backtrace)

        return error


class Cursor:
    """
    This class encapsulates all shared behavior between cursor implementations.
    It provides iteration over the cursor using `iter`, as well as incremental
    iteration using `next`.
        query - the original query that resulted in the cursor, used for:
        query.term_type - the term to be used for pretty-printing backtraces
        query.token - the token to use for subsequent CONTINUE and STOP requests
        query.kwargs - dictate how to format results
    items - The current list of items obtained from the server, this is
        added to in `_extend`, which is called by the ConnectionInstance when a
        new response arrives for this cursor.
    outstanding_requests - The number of requests that are currently awaiting
        a response from the server.  This will typically be 0 or 1 unless the
        cursor is exhausted, but this can be higher if `close` is called.
    threshold - a CONTINUE request will be sent when the length of `items` goes
        below this number.
    error - indicates the current state of the cursor:
        None - there is more data available from the server and no errors have
            occurred yet
         Exception - an error has occurred in the cursor and should be raised
             to the user once all results in `items` have been returned.  This
             will be a ReqlCursorEmpty exception if the cursor completed successfully.
             TODO @gabor-boros: We should not set the `errors` to ReqlCursorEmpty, due
             to it is not an error but a success state.

     A class that derives from this should implement the following functions:
         def _get_next(self, timeout):
             where `timeout` is the maximum amount of time (in seconds) to wait for the
             next result in the cursor before raising a ReqlTimeoutError.
         def _empty_error(self):
             which returns the appropriate error to be raised when the cursor is empty
    """

    def __init__(
        self, conn_instance, query, first_response, items_type=collections.deque
    ):
        self.conn: "ConnectionInstance" = conn_instance
        self.query = query
        self.items = items_type()
        self.outstanding_requests = 0
        self.threshold = 1
        self.error = None
        self._json_decoder = self.conn.parent.get_json_decoder(self.query)

        self.conn.cursor_cache[self.query.token] = self

        self._maybe_fetch_batch()
        self._extend_internal(first_response)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @staticmethod
    def _empty_error() -> Type[ReqlCursorEmpty]:
        """
        Return the empty cursor exception's class.
        """
        return ReqlCursorEmpty

    def _get_next(self, timeout: Optional[float] = None):
        """
        Return the next item through the cursor.

        `timeout` is the maximum amount of time (in seconds) to wait for the
        next result in the cursor before raising a ReqlTimeoutError.
        """
        raise NotImplementedError("implement _get_next before using it")

    def close(self):
        """
        Close the cursor.
        """
        if self.error is None:
            self.error = self._empty_error()

        if not self.conn.is_open():
            return

        self.outstanding_requests += 1
        self.conn.parent.stop(self)

    @staticmethod
    def _wait_to_timeout(wait):
        if isinstance(wait, bool):
            return None if wait else 0

        if isinstance(wait, numbers.Real) and wait >= 0:
            return wait

        raise ReqlDriverError(f"Invalid wait timeout '{wait}'")

    def next(self, wait=True):
        """
        Get the next item using the cursor.
        """
        return self._get_next(Cursor._wait_to_timeout(wait))

    def extend(self, res_buf):
        """
        TODO
        """
        self.outstanding_requests -= 1
        self._maybe_fetch_batch()

        res = Response(self.query.token, res_buf, self._json_decoder)
        self._extend_internal(res)

    def _extend_internal(self, res):
        self.threshold = len(res.data)
        if self.error is None:
            if res.response_type == PbResponse.ResponseType.SUCCESS_PARTIAL:
                self.items.extend(res.data)
            elif res.response_type == PbResponse.ResponseType.SUCCESS_SEQUENCE:
                self.items.extend(res.data)
                self.error = self._empty_error()
            else:
                self.error = res.make_error(self.query)

        if self.outstanding_requests == 0 and self.error is not None:
            del self.conn.cursor_cache[res.token]

    def __str__(self):
        val_str = pprint.pformat(
            [self.items[x] for x in range(min(10, len(self.items)))]
            + (["..."] if len(self.items) > 10 else [])
        )
        if val_str.endswith("'...']"):
            val_str = val_str[: -len("'...']")] + "...]"
        spacer_str = "\n" if "\n" in val_str else ""
        if self.error is None:
            status_str = "streaming"
        elif isinstance(self.error, ReqlCursorEmpty):
            status_str = "done streaming"
        else:
            status_str = f"error: {self.error}"

        return (
            f"{self.__class__.__module__}.{self.__class__.__name__} ({status_str}):"
            f"{spacer_str}{val_str}"
        )

    def __repr__(self):
        val_str = pprint.pformat(
            [self.items[x] for x in range(min(10, len(self.items)))]
            + (["..."] if len(self.items) > 10 else [])
        )
        if val_str.endswith("'...']"):
            val_str = val_str[: -len("'...']")] + "...]"
        spacer_str = "\n" if "\n" in val_str else ""
        if self.error is None:
            status_str = "streaming"
        elif isinstance(self.error, ReqlCursorEmpty):
            status_str = "done streaming"
        else:
            status_str = f"error: {self.error}"

        return (
            f"<{self.__class__.__module__}.{self.__class__.__name__} object at "
            f"{hex(id(self))} ({status_str}): {spacer_str}{val_str}>"
        )

    def raise_error(self, message: str):
        """
        Set an error and extend with a dummy response to trigger any waiters
        """
        if self.error is None:
            self.error = ReqlRuntimeError(message, self.query.term_type, [])
            dummy_response = f'{"t":{PbResponse.ResponseType.SUCCESS_SEQUENCE},"r":[]}'
            self.extend(dummy_response)

    def _maybe_fetch_batch(self):
        if (
            self.error is None
            and len(self.items) < self.threshold
            and self.outstanding_requests == 0
        ):
            self.outstanding_requests += 1
            self.conn.parent.resume(self)


class DefaultCursorEmpty(ReqlCursorEmpty, StopIteration):
    """
    Default empty cursor.
    """


class DefaultCursor(Cursor):
    """
    Default cursor used to get data.
    """

    def __iter__(self):
        return self

    def __next__(self):
        return self._get_next()

    @staticmethod
    def _empty_error():
        return DefaultCursorEmpty()

    def _get_next(self, timeout: Optional[float] = None):
        deadline = None if timeout is None else time.time() + timeout

        while len(self.items) == 0:
            self._maybe_fetch_batch()

            if self.error is not None:
                raise self.error

            self.conn.read_response(self.query, deadline)

        return self.items.popleft()


class SocketWrapper:
    """
    Wrapper for socket connection handling
    """

    # TODO: resolve pylint issues disabled below
    # pylint: disable=too-many-branches,too-many-statements
    def __init__(self, parent: "ConnectionInstance", timeout: int):
        self.host: str = parent.parent.host
        self.port: int = parent.parent.port
        self.ssl: dict = parent.parent.ssl
        self._read_buffer: Optional[bytes] = None

        deadline: float = time.time() + timeout

        try:
            self.__socket = socket.create_connection((self.host, self.port), timeout)

            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

            if len(self.ssl) > 0:
                try:
                    if hasattr(
                        ssl, "SSLContext"
                    ):  # Python2.7 and 3.2+, or backports.ssl
                        ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
                        if hasattr(ssl_context, "options"):
                            ssl_context.options |= getattr(ssl, "OP_NO_SSLv2", 0)
                            ssl_context.options |= getattr(ssl, "OP_NO_SSLv3", 0)
                        ssl_context.verify_mode = ssl.CERT_REQUIRED
                        ssl_context.check_hostname = (
                            True  # redundant with ssl.match_hostname
                        )
                        ssl_context.load_verify_locations(self.ssl["ca_certs"])
                        self.socket = ssl_context.wrap_socket(
                            self.socket, server_hostname=self.host
                        )
                    else:  # this does not disable SSLv2 or SSLv3
                        # TODO: Replace the deprecated wrap_socket
                        self.socket = (
                            ssl.wrap_socket(  # pylint: disable=deprecated-method
                                self.socket,
                                cert_reqs=ssl.CERT_REQUIRED,
                                ssl_version=ssl.PROTOCOL_SSLv23,
                                ca_certs=self.ssl["ca_certs"],
                            )
                        )
                except IOError as err:
                    self.socket.close()

                    if "EOF occurred in violation of protocol" in str(
                        err
                    ) or "sslv3 alert handshake failure" in str(err):
                        # probably on an older version of OpenSSL

                        # pylint: disable=line-too-long
                        raise ReqlDriverError(
                            "SSL handshake failed, likely because Python is linked against an old version of OpenSSL "
                            "that does not support either TLSv1.2 or any of the allowed ciphers. This can be worked "
                            "around by lowering the security setting on the server with the options "
                            "`--tls-min-protocol TLSv1 --tls-ciphers "
                            "EECDH+AESGCM:EDH+AESGCM:AES256+EECDH:AES256+EDH:AES256-SHA` (see server log for more "
                            f"information): {err}"
                        ) from err

                    raise ReqlDriverError(
                        f"SSL handshake failed (see server log for more information): {err}"
                    ) from err
                try:
                    # TODO: Replace the deprecated match_hostname
                    ssl.match_hostname(  # pylint: disable=deprecated-method
                        self.socket.getpeercert(), hostname=self.host
                    )
                except ssl.CertificateError:
                    self.socket.close()
                    raise

            parent.parent.handshake.reset()
            response = None
            while True:
                request = parent.parent.handshake.next_message(response)
                if request is None:
                    break
                # This may happen in the `V1_0` protocol where we send two requests as
                # an optimization, then need to read each separately
                if request != "":
                    self.sendall(request)

                # The response from the server is a null-terminated string
                response = b""
                while True:
                    char = self.recvall(1, deadline)
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
        except socket.timeout as exc:
            self.close()
            raise ReqlTimeoutError(self.host, self.port) from exc
        except Exception as exc:
            self.close()
            raise ReqlDriverError(
                f"Could not connect to {self.host}:{self.port}. Error: {exc}"
            ) from exc

    @property
    def socket(self) -> Union[socket.socket, ssl.SSLSocket]:
        """
        Return the wrapped socket.
        """
        return self.__socket

    @socket.setter
    def socket(self, value: "socket.socket"):
        """
        Set the socket instance.
        """
        self._socket = value

    def is_open(self):
        """
        Return if the connection is open.
        """
        return self.socket is not None

    def close(self):
        """
        Close the connection.
        """
        if not self.is_open():
            return

        try:
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
        except ReqlError as exc:
            logger.error(exc.message)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(exc)
        finally:
            self.socket = None

    def recvall(self, length: int, deadline: Optional[float]):
        """
        Read data received through the socket.
        """
        res: bytes = bytes() if self._read_buffer is None else self._read_buffer
        timeout: Optional[float] = (
            None if deadline is None else max(0.0, deadline - time.time())
        )
        self.socket.settimeout(timeout)
        while len(res) < length:
            while True:
                try:
                    chunk = self.socket.recv(length - len(res))
                    self.socket.settimeout(None)
                    break
                except socket.timeout as exc:
                    self._read_buffer = res
                    self.socket.settimeout(None)
                    raise ReqlTimeoutError(self.host, self.port) from exc
                except IOError as exc:
                    if exc.errno == errno.ECONNRESET:
                        self.close()
                        raise ReqlDriverError("Connection is closed.") from exc

                    if exc.errno == errno.EWOULDBLOCK:
                        # This should only happen with a timeout of 0
                        raise ReqlTimeoutError(self.host, self.port) from exc

                    if exc.errno != errno.EINTR:
                        raise ReqlDriverError(
                            ("Connection interrupted " + "receiving from %s:%s - %s")
                            % (self.host, self.port, str(exc))
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

    def sendall(self, data: bytes):
        """
        Send all data to the server through the socket.
        """
        offset = 0
        while offset < len(data):
            try:
                offset += self.socket.send(data[offset:])
            except IOError as exc:
                if exc.errno == errno.ECONNRESET:
                    self.close()
                    raise ReqlDriverError("Connection is closed.") from exc

                if exc.errno != errno.EINTR:
                    self.close()
                    raise ReqlDriverError(
                        ("Connection interrupted " + "sending to %s:%s - %s")
                        % (self.host, self.port, str(exc))
                    ) from exc
            except Exception as exc:
                self.close()
                raise ReqlDriverError(
                    f"Error sending to {self.host}:{self.port} - {exc}"
                ) from exc
            except BaseException:
                self.close()
                raise


class ConnectionInstance:
    """
    Base implementation for connection instances.
    """

    def __init__(self, parent: "Connection"):
        self.__parent: "Connection" = parent
        self.__cursor_cache: dict = {}
        self._header_in_progress: Optional[bytes] = None
        self.__socket: Optional[SocketWrapper] = None
        self._closing: bool = False

    @property
    def parent(self) -> "Connection":
        """
        Return the parent connection.
        """
        return self.__parent

    @property
    def socket(self) -> Optional[SocketWrapper]:
        """
        Return the socket wrapper.
        """
        return self.__socket

    @socket.setter
    def socket(self, wrapper: Optional[SocketWrapper]) -> None:
        """
        Set the socket wrapper.
        """
        self.__socket = wrapper

    def client_port(self) -> Optional[int]:
        """
        Return the port on which the connection instance is connected to the server.
        """
        if self.socket is None:
            raise AssertionError("Socket unexpectedly returned none.")

        if not self.is_open():
            return None

        return self.socket.socket.getsockname()[1]

    def client_address(self) -> Optional[str]:
        """
        Return the address on which the connection instance is connected to the server.
        """
        if self.socket is None:
            raise AssertionError("Socket unexpectedly returned none.")

        if not self.is_open():
            return None

        return self.socket.socket.getsockname()[0]

    def connect(self, timeout: int) -> "Connection":
        """
        Open a new connection to the server with the given timeout.
        """
        self.socket = SocketWrapper(self, timeout)
        return self.parent

    def is_open(self) -> bool:
        """
        Return if the connection instance is set and the connection is open.
        """

        if self.socket is None:
            raise AssertionError("Socket unexpectedly returned none.")

        return self.socket.is_open()

    def close(self, noreply_wait=False, token=None) -> None:
        """
        Close the connection if connection instance is set.
        """
        self._closing = True

        # Cursors may remove themselves when errored, so copy a list of them
        for cursor in list(self.cursor_cache.values()):
            cursor.raise_error("Connection is closed.")

        self.reset_cursor_cache()

        try:
            if noreply_wait:
                query = Query(PbQuery.QueryType.NOREPLY_WAIT, token, None, None)
                self.run_query(query, False)
        finally:
            if self.socket is None:
                raise AssertionError("Socket unexpectedly returned none.")

            self.socket.close()
            self._header_in_progress = None

    def run_query(self, query: Query, noreply: bool):
        """
        Serialize and send the given query to the database.

        If noreply is set, the response won't be parsed and `run_query` returns
        immediately after sending the query.
        """
        if self.socket is None:
            raise AssertionError("Socket unexpectedly returned none.")

        self.socket.sendall(query.serialize(self.parent.get_json_encoder(query)))

        if noreply:
            return None

        response = self.read_response(query)
        if response is None:
            raise AssertionError("Response unexpectedly returned none.")

        response_type = getattr(response, "response_type")

        if response_type == PbResponse.ResponseType.SUCCESS_ATOM:
            return maybe_profile(response.data[0], response)

        if response_type in (
            PbResponse.ResponseType.SUCCESS_PARTIAL,
            PbResponse.ResponseType.SUCCESS_SEQUENCE,
        ):
            return maybe_profile(DefaultCursor(self, query, response), response)

        if response_type == PbResponse.ResponseType.SERVER_INFO:
            return response.data[0]

        if response_type == PbResponse.ResponseType.WAIT_COMPLETE:
            return None

        raise response.make_error(query)

    def read_response(self, query, deadline=None) -> Optional[Response]:
        """
        Read response sent by the server.
        """
        if self.socket is None:
            raise AssertionError("Socket unexpectedly returned none.")

        token = query.token
        # We may get an async continue result, in which case we save
        # it and read the next response
        while True:
            try:
                # The first 8 bytes give the corresponding query token
                # of this response.  The next 4 bytes give the
                # expected length of this response.
                if self._header_in_progress is None:
                    self._header_in_progress = self.socket.recvall(12, deadline)
                (
                    res_token,
                    res_len,
                ) = struct.unpack("<qL", self._header_in_progress)
                res_buf = self.socket.recvall(res_len, deadline)
                self._header_in_progress = None
            except KeyboardInterrupt as exc:
                # Cancel outstanding queries by dropping this connection,
                # then create a new connection for the user's convenience.
                self.parent.reconnect(noreply_wait=False)
                raise exc

            res = None

            cursor: Cursor = self.cursor_cache.get(res_token)
            if cursor is not None:
                # Construct response
                cursor.extend(res_buf)
                if res_token == token:
                    return res
            elif res_token == token:
                return Response(res_token, res_buf, self.parent.get_json_decoder(query))
            elif not self._closing:
                # This response is corrupted or not intended for us
                self.close()
                raise ReqlDriverError("Unexpected response received.")

    @property
    def cursor_cache(self):
        """
        Return the cursor's cache.
        """
        return self.__cursor_cache

    def reset_cursor_cache(self):
        """
        Reset the cursor cache to drop cached items.
        """
        self.__cursor_cache = {}


class Connection:  # pylint: disable=too-many-instance-attributes
    """
    Handle connection lifecycle, managing the connection instance, connect, reconnect,
    connection close and more.
    """

    _r = None
    _json_decoder = ReqlDecoder
    _json_encoder = ReqlEncoder

    # pylint: disable=too-many-arguments
    def __init__(  # nosec
        self,
        conn_type,
        host: str,
        port: Union[int, str],
        db: str,
        user: str,
        password: str = "",
        timeout: int = 0,
        ssl: dict = None,  # pylint: disable=redefined-outer-name
        _handshake_version: Type[BaseHandshake] = HandshakeV1_0,
        **kwargs,
    ):
        if ssl is None:
            ssl = {}

        self.db: str = db

        self.host: str = host
        try:
            self.port: int = int(port)
        except ValueError as exc:
            raise ReqlDriverError(
                f"Could not convert port {port} to an integer."
            ) from exc

        self.connect_timeout: int = timeout

        self.ssl: dict = ssl

        self._conn_type = conn_type
        self._child_kwargs: dict = kwargs
        self._instance: Optional[ConnectionInstance] = None
        self._next_token: int = 0
        self._repl: Repl = Repl()

        if "json_encoder" in kwargs:
            self._json_encoder = kwargs.pop("json_encoder")
        if "json_decoder" in kwargs:
            self._json_decoder = kwargs.pop("json_decoder")

        if _handshake_version != HandshakeV1_0:
            raise NotImplementedError(
                f"The {_handshake_version} handshake is not implemented."
            )

        self.handshake = HandshakeV1_0(
            self.host,
            self.port,
            user,
            password,
            json_encoder=self._json_encoder,
            json_decoder=self._json_decoder,
        )

    def client_port(self) -> Optional[int]:
        """
        Return the port on which the connection instance is connected to the server.
        """
        if self._instance is None:
            raise AssertionError("Connection instance unexpectedly returned none.")

        if not self.is_open():
            return None

        return self._instance.client_port()

    def client_address(self) -> Optional[str]:
        """
        Return the address on which the connection instance is connected to the server.
        """
        if self._instance is None:
            raise AssertionError("Connection instance unexpectedly returned none.")

        if not self.is_open():
            return None

        return self._instance.client_address()

    def reconnect(self, noreply_wait: bool = True, timeout: Optional[int] = None):
        """
        Reconnect to the server.
        """
        self.close(noreply_wait)
        self._instance = self._conn_type(self, **self._child_kwargs)  # type: ignore

        if self._instance is None:
            raise AssertionError("ConnectionInstance unexpectedly none.")

        return self._instance.connect(
            self.connect_timeout if timeout is None else timeout
        )

    # Not thread safe.
    def repl(self) -> "Connection":
        """
        Sets this connection as global state that will be used by subsequence calls to
        `query.run`. Useful for trying out RethinkDB in a Python repl environment.
        """
        self._repl.set_connection(self)
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close(noreply_wait=False)

    def use(self, db: str):
        """
        Set the encapsulated database to use.
        """
        self.db = db

    def is_open(self) -> bool:
        """
        Return if the connection instance is set and the connection is open.
        """
        return self._instance is not None and self._instance.is_open()

    def check_open(self) -> None:
        """
        Check if the connection is open, otherwise raise a connection closed error.
        """
        if self._instance is None or not self._instance.is_open():
            raise ReqlDriverError("Connection is closed.")

    def close(self, noreply_wait=True) -> None:
        """
        Close the connection if connection instance is set.
        """
        if self._instance is None:
            return

        self._instance.close(noreply_wait, self._new_token())
        self._next_token = 0
        self._instance = None

    def noreply_wait(self):
        """
        TODO
        """
        self.check_open()
        query = Query(PbQuery.QueryType.NOREPLY_WAIT, self._new_token(), None, None)
        return self._instance.run_query(query, False)

    def server(self):
        """
        Return the server we connected to.
        """

        self.check_open()
        query = Query(PbQuery.QueryType.SERVER_INFO, self._new_token(), None, None)
        return self._instance.run_query(query, False)

    def _new_token(self):
        response = self._next_token
        self._next_token += 1
        return response

    def start(self, term, **kwargs):
        """
        Send a new query to the server.
        """
        self.check_open()
        if "db" in kwargs or self.db is not None:
            kwargs["db"] = DB(kwargs.get("db", self.db))
        query = Query(PbQuery.QueryType.START, self._new_token(), term, kwargs)
        return self._instance.run_query(query, kwargs.get("noreply", False))

    def resume(self, cursor):
        """
        Send a CONTINUE query to the server if the connection is open.
        """
        self.check_open()
        query = Query(PbQuery.QueryType.CONTINUE, cursor.query.token, None, None)
        return self._instance.run_query(query, True)

    def stop(self, cursor):
        """
        Send a STOP query to the server if the connection is open.
        """
        self.check_open()
        query = Query(PbQuery.QueryType.STOP, cursor.query.token, None, None)
        return self._instance.run_query(query, True)

    def get_json_decoder(self, query):
        """
        Return the related json decoder.
        """
        return (query.json_decoder or self._json_decoder)(reql_format_opts=query.kwargs)

    def get_json_encoder(self, query):
        """
        Return the related json encoder.
        """
        return (query.json_encoder or self._json_encoder)()


class DefaultConnection(Connection):
    """
    Default connection without async handlers.
    """

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, ConnectionInstance, *args, **kwargs)


# pylint: disable=too-many-arguments
def make_connection(
    connection_type,
    host=DEFAULT_HOST,
    port=DEFAULT_PORT,
    db=None,
    user=DEFAULT_USER,
    password=None,
    timeout=DEFAULT_TIMEOUT,
    ssl=None,  # pylint: disable=redefined-outer-name
    url=None,
    handshake_version=HandshakeV1_0,
    **kwargs,
):
    """
    Open a new connection to the database and return a connection handler.
    """

    if password is None:
        password = ""  # nosec

    if ssl is None:
        ssl = {}

    if url:
        connection_string = urlparse(url)
        query_string = parse_qs(connection_string.query)

        user = connection_string.username or user
        password = connection_string.password or password
        host = connection_string.hostname or host
        port = connection_string.port or port

        db = connection_string.path.replace("/", "") or None
        timeout = query_string.get("timeout", DEFAULT_TIMEOUT)

        if timeout:
            timeout = int(timeout[0])

    conn = connection_type(
        host,
        port,
        db,
        user,
        password,
        timeout,
        ssl,
        handshake_version,
        **kwargs,
    )

    return conn.reconnect(timeout=timeout)
