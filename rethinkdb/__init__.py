# Copyright 2022 RethinkDB
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

import warnings

from rethinkdb import errors  # , version

__all__ = ["r", "RethinkDB"]
__version__ = "2.5.0"


class RethinkDB:
    """
    RethinkDB serves as an entrypoint for queries.

    It constructs the connection handlers and event loops, re-exports internal modules for easier
    use, and sets the event loop.
    """

    def __init__(self):
        super().__init__()

        # pylint: disable=import-outside-toplevel
        from rethinkdb import ast, net, query

        # Re-export internal modules for backward compatibility
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

        # Ensure the `make_connection` function is not overridden accidentally
        self.make_connection = self.net.make_connection
        self.set_loop_type(None)

    def set_loop_type(self, library=None) -> None:
        """
        Set event loop type for the requested library.
        """

        if library == "asyncio":
            warnings.warn(f"{library} is not yet supported, using the default one")
            library = None

        if library == "gevent":
            warnings.warn(f"{library} is not yet supported, using the default one")
            library = None

        if library == "tornado":
            warnings.warn(f"{library} is not yet supported, using the default one")
            library = None

        if library == "trio":
            warnings.warn(f"{library} is not yet supported, using the default one")
            library = None

        if library == "twisted":
            warnings.warn(f"{library} is not yet supported, using the default one")
            library = None

        if library is None or self.connection_type is None:
            self.connection_type = self.net.DefaultConnection

    def connect(self, *args, **kwargs):
        """
        Make a connection to the database.
        """

        return self.make_connection(self.connection_type, *args, **kwargs)


r = RethinkDB()
