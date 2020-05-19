# Copyright 2018 RethinkDB
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
import imp
import os

import pkg_resources

from rethinkdb import errors, version

# The builtins here defends against re-importing something obscuring `object`.
try:
    import __builtin__ as builtins  # Python 2
except ImportError:
    import builtins  # Python 3


__all__ = ["RethinkDB"] + errors.__all__
__version__ = version.VERSION


class RethinkDB(builtins.object):
    def __init__(self):
        super(RethinkDB, self).__init__()

        from rethinkdb import (
            _dump,
            _export,
            _import,
            _index_rebuild,
            _restore,
            ast,
            query,
            net,
        )

        self._dump = _dump
        self._export = _export
        self._import = _import
        self._index_rebuild = _index_rebuild
        self._restore = _restore

        # Re-export internal modules for backward compatibility
        self.ast = ast
        self.errors = errors
        self.net = net
        self.query = query

        net.Connection._r = self

        for module in (self.net, self.query, self.ast, self.errors):
            for function_name in module.__all__:
                setattr(self, function_name, getattr(module, function_name))

        self.set_loop_type(None)

    def set_loop_type(self, library=None):
        if library is None:
            self.connection_type = self.net.DefaultConnection
            return

        # find module file
        manager = pkg_resources.ResourceManager()
        lib_path = "%(library)s_net/net_%(library)s.py" % {"library": library}
        if not manager.resource_exists(__name__, lib_path):
            raise ValueError("Unknown loop type: %r" % library)

        # load the module
        module_path = manager.resource_filename(__name__, lib_path)
        module_name = "net_%s" % library
        module_file, pathName, desc = imp.find_module(
            module_name, [os.path.dirname(module_path)]
        )
        module = imp.load_module("rethinkdb." + module_name, module_file, pathName, desc)

        # set the connection type
        self.connection_type = module.Connection

        # cleanup
        manager.cleanup_resources()

    def connect(self, *args, **kwargs):
        return self.make_connection(self.connection_type, *args, **kwargs)


r = RethinkDB()
