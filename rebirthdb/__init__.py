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

from rebirthdb import errors, version


try:
    import __builtin__ as builtins  # Python 2
except ImportError:
    import builtins  # Python 3


__all__ = ['RebirthDB'] + errors.__all__
__version__ = version.VERSION


class RebirthDB(builtins.object):
    def __init__(self):
        super(RebirthDB, self).__init__()

        from rebirthdb import _dump, _export, _import, _index_rebuild, _restore, ast, query, net

        self._dump = _dump
        self._export = _export
        self._import = _import
        self._index_rebuild = _index_rebuild
        self._restore = _restore

        net.Connection._r = self

        for module in (net, query, ast, errors):
            for functionName in module.__all__:
                setattr(self, functionName, getattr(module, functionName))
