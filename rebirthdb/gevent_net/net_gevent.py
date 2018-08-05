# Copyright 2018 RebirthDB
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

import gevent
from gevent.event import AsyncResult, Event
from gevent.lock import Semaphore

from rebirthdb import net, ql2_pb2
from rebirthdb.errors import ReqlCursorEmpty, RqlDriverError, RqlTimeoutError
from rebirthdb.net import SocketWrapper


__all__ = ['Connection']

PROTO_RESPONSE_TYPE = ql2_pb2.Response.ResponseType
PROTO_QUERY_TYPE = ql2_pb2.Query.QueryType


class GeventCursorEmpty(ReqlCursorEmpty, StopIteration):
    pass


class Connection(net.Connection):
    pass


# TODO: allow users to set sync/async?
class GeventCursor(net.Cursor):
    def __init__(self, *args, **kwargs):
        super(GeventCursor, self).__init__(*args, **kwargs)
        self.new_response = Event()

    def __iter__(self):
        return self

    def __next__(self):
        return self._get_next(None)

    def _empty_error(self):
        return GeventCursorEmpty()

    def _extend(self, res_buf):
        super(GeventCursor, self)._extend(res_buf)
        self.new_response.set()
        self.new_response.clear()

    def _get_next(self, timeout):
        with gevent.Timeout(timeout, RqlTimeoutError()):
            self._maybe_fetch_batch()
            while len(self.items) == 0:
                if self.error is not None:
                    raise self.error
                self.new_response.wait()
            return self.items.popleft()


class ConnectionInstance(object):
    def __init__(self, parent):
        self._parent = parent
        self._closing = False
        self._user_queries = {}
        self._cursor_cache = {}

        self._write_mutex = Semaphore()
        self._socket = None
        self.timeout = None

    def connect(self, timeout):
        if not self.timeout:
            self.timeout = timeout

        with gevent.Timeout(timeout, RqlTimeoutError(self._parent.host, self._parent.port)) as timeout:
            self._socket = SocketWrapper(self, timeout)

        # Start a parallel coroutine to perform reads
        gevent.spawn(self._reader)
        return self._parent

    def is_open(self):
        return self._socket is not None and self._socket.is_open()

    def close(self, no_reply_wait=False, token=None, exception=None):
        self._closing = True

        if exception is not None:
            err_message = "Connection is closed (%s)." % str(exception)
        else:
            err_message = "Connection is closed."

        # Cursors may remove themselves when errored, so copy a list of them
        for cursor in list(self._cursor_cache.values()):
            cursor._error(err_message)

        for query, async_res in iter(self._user_queries.values()):
            async_res.set_exception(RqlDriverError(err_message))

        self._user_queries = {}
        self._cursor_cache = {}

        if no_reply_wait:
            no_reply = net.Query(PROTO_QUERY_TYPE.NOREPLY_WAIT, token, None, None)
            self.run_query(no_reply, False)

        try:
            self._socket.close()
        except OSError:
            pass

    # TODO: make connection recoverable if interrupted by a user's gevent.Timeout?
    def run_query(self, query, noreply):
        self._write_mutex.acquire()

        try:
            self._socket.sendall(query.serialize(self._parent._get_json_encoder(query)))
        finally:
            self._write_mutex.release()

        if noreply:
            return None

        async_res = AsyncResult()
        self._user_queries[query.token] = (query, async_res)
        return async_res.get()

    # The _reader coroutine runs in its own coroutine in parallel, reading responses
    # off of the socket and forwarding them to the appropriate AsyncResult or Cursor.
    # This is shut down as a consequence of closing the stream, or an error in the
    # socket/protocol from the server.  Unexpected errors in this coroutine will
    # close the ConnectionInstance and be passed to any open AsyncResult or Cursors.
    def _reader(self):
        try:
            while True:
                buf = self._socket.recvall(12, self.timeout)
                (token, length,) = struct.unpack("<qL", buf)
                buf = self._socket.recvall(length, self.timeout)

                cursor = self._cursor_cache.get(token)
                if cursor is not None:
                    cursor._extend(buf)
                elif token in self._user_queries:
                    # Do not pop the query from the dict until later, so
                    # we don't lose track of it in case of an exception
                    query, async_res = self._user_queries[token]
                    res = net.Response(token, buf, self._parent._get_json_decoder(query))
                    if res.type == PROTO_RESPONSE_TYPE.SUCCESS_ATOM:
                        async_res.set(net.maybe_profile(res.data[0], res))
                    elif res.type in (PROTO_RESPONSE_TYPE.SUCCESS_SEQUENCE,
                                      PROTO_RESPONSE_TYPE.SUCCESS_PARTIAL):
                        cursor = GeventCursor(self, query, res)
                        async_res.set(net.maybe_profile(cursor, res))
                    elif res.type == PROTO_RESPONSE_TYPE.WAIT_COMPLETE:
                        async_res.set(None)
                    else:
                        async_res.set_exception(res.make_error(query))
                    del self._user_queries[token]
                elif not self._closing:
                    raise RqlDriverError("Unexpected response received.")
        except Exception as ex:
            if not self._closing:
                self.close(exception=ex)
