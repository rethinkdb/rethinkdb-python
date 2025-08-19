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


class RethinkDB:
    """
    RethinkDB is a wrapper around RethinkDB connection handling.

    It constructs the connection handlers and event loops, re-exports internal modules for easier
    use, and sets the event loop.
    """

    def __init__(self):
        super().__init__()

        # pylint: disable=import-outside-toplevel
        from rethinkdb import ast, errors, net, query

        self.ast = ast
        self.errors = errors
        self.net = net
        self.query = query

        net.Connection._r = self
        self.connection_type = None

        # Dynamically assign every re-exported internal module's function to self
        for module in (self.net, self.query, self.ast, self.errors):
            for function_name in module.__all__:
                setattr(self, function_name, getattr(module, function_name))

        self.make_connection = net.make_connection
        self.set_loop_type(None)

    def set_loop_type(self, library=None) -> None:
        """
        Set event loop type for the requested library.
        """

        if library == "asyncio":
            # pylint: disable=import-outside-toplevel
            from rethinkdb.net_asyncio import Connection as AsyncioConnection

            self.connection_type = AsyncioConnection

        if library == "gevent":
            # pylint: disable=import-outside-toplevel
            from rethinkdb.net_gevent import Connection as GeventConnection

            self.connection_type = GeventConnection

        if library == "tornado":
            # pylint: disable=import-outside-toplevel
            from rethinkdb.net_tornado import Connection as TornadoConnection

            self.connection_type = TornadoConnection

        if library == "trio":
            # pylint: disable=import-outside-toplevel
            from rethinkdb.net_trio import Connection as TrioConnection

            self.connection_type = TrioConnection

        if library == "twisted":
            # pylint: disable=import-outside-toplevel
            from rethinkdb.net_twisted import Connection as TwistedConnection

            self.connection_type = TwistedConnection

        if library is None or self.connection_type is None:
            # pylint: disable=import-outside-toplevel
            from rethinkdb.net import DefaultConnection

            self.connection_type = DefaultConnection

    def connect(self, *connect_args, **kwargs):
        """
        Make a connection to the database.
        """

        return self.make_connection(self.connection_type, *connect_args, **kwargs)


r = RethinkDB()
