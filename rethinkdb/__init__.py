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

# pylint: disable=redefined-builtin, unused-import

from types import SimpleNamespace
import warnings

from rethinkdb import net
from rethinkdb.query import (
    add,
    and_,
    april,
    args,
    asc,
    august,
    avg,
    binary,
    bit_and,
    bit_not,
    bit_or,
    bit_sal,
    bit_sar,
    bit_xor,
    branch,
    ceil,
    circle,
    contains,
    count,
    db,
    db_create,
    db_drop,
    db_list,
    december,
    desc,
    distance,
    distinct,
    div,
    do,
    epoch_time,
    eq,
    error,
    february,
    floor,
    format,
    friday,
    ge,
    geojson,
    grant,
    group,
    gt,
    http,
    info,
    intersects,
    iso8601,
    january,
    js,
    json,
    july,
    june,
    le,
    line,
    literal,
    lt,
    make_timezone,
    map,
    march,
    max,
    maxval,
    may,
    min,
    minval,
    mod,
    monday,
    mul,
    ne,
    not_,
    november,
    now,
    object,
    october,
    or_,
    point,
    polygon,
    random,
    range,
    reduce,
    round,
    row,
    saturday,
    september,
    sub,
    sum,
    sunday,
    table,
    table_create,
    table_drop,
    table_list,
    thursday,
    time,
    tuesday,
    type_of,
    union,
    uuid,
    wednesday,
)

# pylint: enable=redefined-builtin, unused-import

__version__ = "2.5.0"

# Create the r namespace object containing all query functions
r = SimpleNamespace()

query_functions = {
    "add": add,
    "and_": and_,
    "april": april,
    "args": args,
    "asc": asc,
    "august": august,
    "avg": avg,
    "binary": binary,
    "bit_and": bit_and,
    "bit_not": bit_not,
    "bit_or": bit_or,
    "bit_sal": bit_sal,
    "bit_sar": bit_sar,
    "bit_xor": bit_xor,
    "branch": branch,
    "ceil": ceil,
    "circle": circle,
    "contains": contains,
    "count": count,
    "db": db,
    "db_create": db_create,
    "db_drop": db_drop,
    "db_list": db_list,
    "december": december,
    "desc": desc,
    "distance": distance,
    "distinct": distinct,
    "div": div,
    "do": do,
    "epoch_time": epoch_time,
    "eq": eq,
    "error": error,
    "february": february,
    "floor": floor,
    "format": format,
    "friday": friday,
    "ge": ge,
    "geojson": geojson,
    "grant": grant,
    "group": group,
    "gt": gt,
    "http": http,
    "info": info,
    "intersects": intersects,
    "iso8601": iso8601,
    "january": january,
    "json": json,
    "july": july,
    "june": june,
    "le": le,
    "line": line,
    "literal": literal,
    "lt": lt,
    "make_timezone": make_timezone,
    "map": map,
    "march": march,
    "max": max,
    "maxval": maxval,
    "may": may,
    "min": min,
    "minval": minval,
    "mod": mod,
    "monday": monday,
    "mul": mul,
    "ne": ne,
    "not_": not_,
    "november": november,
    "now": now,
    "object": object,
    "october": october,
    "or_": or_,
    "point": point,
    "polygon": polygon,
    "random": random,
    "range": range,
    "reduce": reduce,
    "round": round,
    "row": row,
    "saturday": saturday,
    "september": september,
    "sub": sub,
    "sum": sum,
    "sunday": sunday,
    "table": table,
    "table_create": table_create,
    "table_drop": table_drop,
    "table_list": table_list,
    "thursday": thursday,
    "time": time,
    "tuesday": tuesday,
    "type_of": type_of,
    "union": union,
    "uuid": uuid,
    "wednesday": wednesday,
    "js": js,
}

for name, func in query_functions.items():
    setattr(r, name, func)


class Client:
    """
    Client is a wrapper around RethinkDB connection handling.

    It constructs the connection handlers and event loops, re-exports internal modules for easier
    use, and sets the event loop.
    """

    def __init__(self):
        super().__init__()

        self.net = net

        net.Connection._r = self
        self.connection_type = None

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
            # pylint: disable=import-outside-toplevel
            from rethinkdb.net_gevent import Connection as GeventConnection

            self.connection_type = GeventConnection

        if library == "tornado":
            # pylint: disable=import-outside-toplevel
            from rethinkdb.net_tornado import Connection as TornadoConnection

            self.connection_type = TornadoConnection

        if library == "trio":
            warnings.warn(f"{library} is not yet supported, using the default one")
            library = None

        if library == "twisted":
            # pylint: disable=import-outside-toplevel
            from rethinkdb.net_twisted import Connection as TwistedConnection

            self.connection_type = TwistedConnection

        if library is None or self.connection_type is None:
            self.connection_type = self.net.DefaultConnection

    def connect(self, *connect_args, **kwargs):
        """
        Make a connection to the database.
        """

        return self.make_connection(self.connection_type, *connect_args, **kwargs)
