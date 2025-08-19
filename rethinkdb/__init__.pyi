# pylint: disable-all
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
# mypy: ignore-errors
import datetime
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Type, Union

from rethinkdb import ast as ast_module
from rethinkdb import errors as errors_module
from rethinkdb import net as net_module
from rethinkdb import query as query_module
from rethinkdb.ast import RqlBinary, RqlQuery, RqlTzinfo
from rethinkdb.errors import (
    InvalidHandshakeStateError,
    QueryPrinter,
    ReqlAuthError,
    ReqlCompileError,
    ReqlCursorEmpty,
    ReqlDriverCompileError,
    ReqlDriverError,
    ReqlError,
    ReqlInternalError,
    ReqlNonExistenceError,
    ReqlOpFailedError,
    ReqlOpIndeterminateError,
    ReqlOperationError,
    ReqlPermissionError,
    ReqlQueryLogicError,
    ReqlResourceLimitError,
    ReqlRuntimeError,
    ReqlServerCompileError,
    ReqlTimeoutError,
    ReqlUserError,
)
from rethinkdb.handshake import BaseHandshake
from rethinkdb.net import Connection, Cursor, DefaultConnection

class RethinkDB:
    ast: ast_module
    errors: errors_module
    net: net_module
    query: query_module
    connection_type: Optional[Type[Connection]]

    def __init__(self) -> None: ...
    def set_loop_type(self, library: Optional[str] = None) -> None: ...
    def connect(self, *connect_args: Any, **kwargs: Any) -> Connection: ...

    # from rethinkdb.ast
    RqlBinary: Type[RqlBinary] = ...
    """
    bytes(iterable_of_ints) -> bytes
    bytes(string, encoding[, errors]) -> bytes
    bytes(bytes_or_buffer) -> immutable copy of bytes_or_buffer
    bytes(int) -> bytes object of size given by the parameter initialized with null bytes
    bytes() -> empty bytes object
    """
    RqlQuery: Type[RqlQuery] = ...
    """
    The RethinkDB Query object which determines the operations we can request
    from the server.
    """
    RqlTzinfo: Type[RqlTzinfo] = ...
    """
    RethinkDB timezone information.
    """
    expr: Callable[
        [
            Union[
                str,
                bytes,
                RqlQuery,
                RqlBinary,
                datetime.date,
                datetime.datetime,
                Mapping[Any, Any],
                Iterable[Any],
                Callable[..., Any],
            ],
            int,
        ],
        RqlQuery,
    ] = ...
    """
    Convert a Python primitive into a Reql primitive value.
    """

    # from rethinkdb.errors
    InvalidHandshakeStateError: Type[InvalidHandshakeStateError] = ...
    """
    Exception raised when the client entered a not existing state during connection handshake.
    """
    QueryPrinter: Type[QueryPrinter] = ...
    """
    Helper class to print Query failures in a formatted was using carets.
    """
    ReqlAuthError: Type[ReqlAuthError] = ...
    """
    The exception raised when the authentication was unsuccessful to the database
    server.
    """
    ReqlCompileError: Type[ReqlCompileError] = ...
    """
    Exception representing any kind of compilation error. A compilation error
    can be raised during parsing a Python primitive into a Reql primitive or even
    when the server cannot parse a Reql primitive, hence it returns an error.
    """
    ReqlCursorEmpty: Type[ReqlCursorEmpty] = ...
    """
    Base exception indicates that the cursor was empty.
    """
    ReqlDriverCompileError: Type[ReqlDriverCompileError] = ...
    """
    Exception indicates that a Python primitive cannot be converted into a
    Reql primitive.
    """
    ReqlDriverError: Type[ReqlDriverError] = ...
    """
    Exception representing the Python client related exceptions.
    """
    ReqlError: Type[ReqlError] = ...
    """
    Base RethinkDB Query Language Error.
    """
    ReqlInternalError: Type[ReqlInternalError] = ...
    """
    Exception indicates that some internal error happened on server side.
    """
    ReqlNonExistenceError: Type[ReqlNonExistenceError] = ...
    """
    Exception indicates an error related to the absence of an expected value.
    """
    ReqlOpFailedError: Type[ReqlOpFailedError] = ...
    """
    Exception indicates that REQL operation failed.
    """
    ReqlOpIndeterminateError: Type[ReqlOpIndeterminateError] = ...
    """
    Exception indicates that it is unknown whether an operation failed or not.
    """
    ReqlOperationError: Type[ReqlOperationError] = ...
    """
    Exception indicates that the error happened due to availability issues.
    """
    ReqlPermissionError: Type[ReqlPermissionError] = ...
    """
    Exception indicates that the connected user has no permission to execute the query.
    """
    ReqlQueryLogicError: Type[ReqlQueryLogicError] = ...
    """
    Exception indicates that the query is syntactically correct, but not it has some
    logical errors.
    """
    ReqlResourceLimitError: Type[ReqlResourceLimitError] = ...
    """
    Exception indicates that the server exceeded a resource limit (e.g. the array size limit).
    """
    ReqlRuntimeError: Type[ReqlRuntimeError] = ...
    """
    Exception representing a runtime issue within the Python client. The runtime error
    is within the client and not the database.
    """
    ReqlServerCompileError: Type[ReqlServerCompileError] = ...
    """
    Exception indicates that a Reql primitive cannot be parsed by the server, hence
    it returned an error.
    """
    ReqlTimeoutError: Type[ReqlTimeoutError] = ...
    """
    Exception indicates that the request towards the server is timed out.
    """
    ReqlUserError: Type[ReqlUserError] = ...
    """
    Exception indicates that en error caused by `r.error` with arguments.
    """

    # from rethinkdb.net
    Connection: Type[Connection] = ...
    """
    Handle connection lifecycle, managing the connection instance, connect, reconnect,
    connection close and more.
    """
    Cursor: Type[Cursor] = ...
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
    """
    DEFAULT_PORT: int = ...
    """
    int([x]) -> integer
    int(x, base=10) -> integer
    """
    DefaultConnection: Type[DefaultConnection] = ...
    """
    Default connection without async handlers.
    """
    make_connection: Callable[
        [
            Type[Connection],
            str,
            int,
            Optional[str],
            str,
            Optional[str],
            int,
            Optional[Dict[str, Any]],
            Optional[str],
            Type[BaseHandshake],
        ],
        Connection,
    ] = ...
    """
    Open a new connection to the database and return a connection handler.
    """

    # from rethinkdb.query
    add: Callable[..., ast_module.Add] = ...
    """
    Add function.
    """
    and_: Callable[..., ast_module.And] = ...
    """
    AND function.
    """
    april: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    args: Callable[..., ast_module.Args] = ...
    """
    r.args is a special term that's used to splice an array of arguments into
    another term. This is useful when you want to call a variadic term such as
    get_all with a set of arguments produced at runtime.
    """
    asc: Callable[..., ast_module.Asc] = ...
    """
    Sort the sequence by document values of the given key(s). To specify the
    ordering, wrap the attribute with either r.asc or r.desc (defaults to
    ascending).
    """
    august: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    avg: Callable[..., ast_module.Avg] = ...
    """
    Averages all the elements of a sequence. If called with a field name,
    averages all the values of that field in the sequence, skipping elements of
    the sequence that lack that field. If called with a function, calls that
    function on every element of the sequence and averages the results, skipping
    elements of the sequence where that function returns None or a non-existence
    error.
    """
    binary: Callable[[bytes], RqlQuery] = ...
    """
    Binary function.
    """
    bit_and: Callable[..., ast_module.BitAnd] = ...
    """
    Bitwise AND function.
    """
    bit_not: Callable[..., ast_module.BitNot] = ...
    """
    Bit negation function.
    """
    bit_or: Callable[..., ast_module.BitOr] = ...
    """
    Bitwise OR function.
    """
    bit_sal: Callable[..., ast_module.BitSal] = ...
    """
    In an arithmetic shift (also referred to as signed shift), like a logical
    shift, the bits that slide off the end disappear (except for the last,
    which goes into the carry flag). But in an arithmetic shift, the spaces are
    filled in such a way to preserve the sign of the number being slid. For
    this reason, arithmetic shifts are better suited for signed numbers in
    two's complement format.
    """
    bit_sar: Callable[..., ast_module.BitSar] = ...
    """
    In an arithmetic shift (also referred to as signed shift), like a logical
    shift, the bits that slide off the end disappear (except for the last,
    which goes into the carry flag). But in an arithmetic shift, the spaces
    are filled in such a way to preserve the sign of the number being slid.
    For this reason, arithmetic shifts are better suited for signed numbers
    in two's complement format.
    """
    bit_xor: Callable[..., ast_module.BitXor] = ...
    """
    Bitwise XOR function.
    """
    branch: Callable[..., ast_module.Branch] = ...
    """
    Perform a branching conditional equivalent to if-then-else.
    """
    ceil: Callable[..., ast_module.Ceil] = ...
    """
    Ceil function.
    """
    circle: Callable[..., ast_module.Circle] = ...
    """
    Construct a circular line or polygon. A circle in RethinkDB is a polygon or
    line approximating a circle of a given radius around a given center,
    consisting of a specified number of vertices (default 32).
    """
    contains: Callable[..., ast_module.Contains] = ...
    """
    When called with values, returns True if a sequence contains all the
    specified values. When called with predicate functions, returns True if
    for each predicate there exists at least one element of the stream where
    that predicate returns True.
    """
    count: Callable[..., ast_module.Count] = ...
    """
    Counts the number of elements in a sequence or key/value pairs in an
    object, or returns the size of a string or binary object.
    """
    db: Callable[..., ast_module.DB] = ...
    """
    Reference a database.
    """
    db_create: Callable[..., ast_module.DbCreate] = ...
    """
    Create a database. A RethinkDB database is a collection of tables, similar
    to relational databases.
    """
    db_drop: Callable[..., ast_module.DbDrop] = ...
    """
    Drop a database. The database, all its tables, and corresponding data will
    be deleted.
    """
    db_list: Callable[..., ast_module.DbList] = ...
    """
    List all database names in the system. The result is a list of strings.
    """
    december: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    desc: Callable[..., ast_module.Desc] = ...
    """
    Sort the sequence by document values of the given key(s). To specify the
    ordering, wrap the attribute with either r.asc or r.desc (defaults to
    ascending).
    """
    distance: Callable[..., ast_module.Distance] = ...
    """
    Compute the distance between a point and another geometry object. At least
    one of the geometry objects specified must be a point.
    """
    distinct: Callable[..., ast_module.Distinct] = ...
    """
    Removes duplicate elements from a sequence.
    """
    div: Callable[..., ast_module.Div] = ...
    """
    Divide function.
    """
    do: Callable[..., ast_module.FunCall] = ...
    """
    Call an anonymous function using return values from other Reql commands or
    queries as arguments.
    """
    epoch_time: Callable[..., ast_module.EpochTime] = ...
    """
    Epoch time function.
    """
    eq: Callable[..., ast_module.Eq] = ...
    """
    Equals function.
    """
    error: Callable[..., ast_module.UserError] = ...
    """
    Throw a runtime error. If called with no arguments inside the second
    argument to default, re-throw the current error.
    """
    february: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    floor: Callable[..., ast_module.Floor] = ...
    """
    Floor function.
    """
    format: Callable[..., ast_module.Format] = ...
    """
    Format command takes a string as a template and formatting parameters as an
    object. The parameters in the template string must exist as keys in the
    object, otherwise, an error raised. The template must be a string literal
    and cannot be the result of other commands.
    """
    friday: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    ge: Callable[..., ast_module.Ge] = ...
    """
    Greater or equal than function.
    """
    geojson: Callable[..., ast_module.GeoJson] = ...
    """
    Convert a GeoJSON object to a Reql geometry object.
    """
    grant: Callable[..., ast_module.GrantTL] = ...
    """
    Grant or deny access permissions for a user account, globally or on a
    per-database or per-table basis.
    """
    group: Callable[..., ast_module.Group] = ...
    """
    Takes a stream and partitions it into multiple groups based on the fields
    or functions provided.
    """
    gt: Callable[..., ast_module.Gt] = ...
    """
    Greater than function.
    """
    http: Callable[..., ast_module.Http] = ...
    """
    Retrieve data from the specified URL over HTTP. The return type depends on
    the result_format option, which checks the Content-Type of the response by
    default. Make sure that you never use this command for user provided URLs.
    """
    info: Callable[..., ast_module.Info] = ...
    """
    Information function.
    """
    intersects: Callable[..., ast_module.Intersects] = ...
    """
    Tests whether two geometry objects intersect with one another. When applied
    to a sequence of geometry objects, intersects acts as a filter, returning a
    sequence of objects from the sequence that intersect with the argument.
    """
    iso8601: Callable[..., ast_module.ISO8601] = ...
    """
    ISO8601 function.
    """
    january: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    js: Callable[..., ast_module.JavaScript] = ...
    """
    Create a javascript expression.
    """
    json: Callable[..., ast_module.Json] = ...
    """
    Transform *arguments parameters into JSON.
    """
    july: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    june: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    le: Callable[..., ast_module.Le] = ...
    """
    Less or equal than function.
    """
    line: Callable[..., ast_module.Line] = ...
    """
    Construct a geometry object of type Line.
    """
    literal: Callable[..., ast_module.Literal] = ...
    """
    Replace an object in a field instead of merging it with an existing object
    in a merge or update operation. = Using literal with no arguments in a
    merge or update operation will remove the corresponding field.
    """
    lt: Callable[..., ast_module.Lt] = ...
    """
    Less than function.
    """
    make_timezone: Callable[..., RqlTzinfo] = ...
    """
    Add timezone function.
    """
    map: Callable[..., ast_module.Map] = ...
    """
    Transform each element of one or more sequences by applying a mapping
    function to them. If map is run with two or more sequences, it will
    iterate for as many items as there are in the shortest sequence.
    """
    march: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    max: Callable[..., ast_module.Max] = ...
    """
    Finds the maximum element of a sequence.
    """
    maxval: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    may: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    min: Callable[..., ast_module.Min] = ...
    """
    Finds the minimum element of a sequence.
    """
    minval: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    mod: Callable[..., ast_module.Mod] = ...
    """
    Module function.
    """
    monday: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    mul: Callable[..., ast_module.Mul] = ...
    """
    Multiply function.
    """
    ne: Callable[..., ast_module.Ne] = ...
    """
    Not equal function.
    """
    not_: Callable[..., ast_module.Not] = ...
    """
    Not function.
    """
    november: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    now: Callable[..., ast_module.Now] = ...
    """
    Now function.
    """
    object: Callable[..., ast_module.Object] = ...
    """
    Creates an object from a list of key-value pairs, where the keys must be
    strings. r.object(A, B, C, D) is equivalent to
    r.expr([[A, B], [C, D]]).coerce_to('OBJECT').
    """
    october: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    or_: Callable[..., ast_module.Or] = ...
    """
    OR function.
    """
    point: Callable[..., ast_module.Point] = ...
    """
    Construct a geometry object of type Point. The point is specified by two
    floating point numbers, the longitude (-180 to 180) and latitude (-90 to 90)
    of the point on a perfect sphere
    """
    polygon: Callable[..., ast_module.Polygon] = ...
    """
    Construct a geometry object of type Polygon.
    """
    random: Callable[..., ast_module.Random] = ...
    """
    Generate a random number between given (or implied) bounds. random takes
    zero, one or two arguments.
    """
    range: Callable[..., ast_module.Range] = ...
    """
    Range function.
    """
    reduce: Callable[..., ast_module.Reduce] = ...
    """
    Produce a single value from a sequence through repeated application of a
    reduction function.
    """
    round: Callable[..., ast_module.Round] = ...
    """
    Round function.
    """
    row: RqlQuery = ...
    saturday: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    september: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    sub: Callable[..., ast_module.Sub] = ...
    """
    Subtract function.
    """
    sum: Callable[..., ast_module.Sum] = ...
    """
    Sums all the elements of a sequence. If called with a field name, sums all
    the values of that field in the sequence, skipping elements of the sequence
    that lack that field. If called with a function, calls that function on
    every element of the sequence and sums the results, skipping elements of
    the sequence where that function returns None or a non-existence error.
    """
    sunday: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    table: Callable[..., ast_module.Table] = ...
    """
    Return all documents in a table. Other commands may be chained after table
    to return a subset of documents (such as get and filter) or perform further
    processing.
    """
    table_create: Callable[..., ast_module.TableCreateTL] = ...
    """
    Create a table. A RethinkDB table is a collection of JSON documents.
    """
    table_drop: Callable[..., ast_module.TableDropTL] = ...
    """
    Drop a table. The table and all its data will be deleted.
    """
    table_list: Callable[..., ast_module.TableListTL] = ...
    """
    List all table names in a database. The result is a list of strings.
    """
    thursday: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    time: Callable[..., ast_module.Time] = ...
    """
    Time function.
    """
    tuesday: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    type_of: Callable[..., ast_module.TypeOf] = ...
    """
    Type function.
    """
    union: Callable[..., ast_module.Union] = ...
    """
    Merge two or more sequences.
    """
    uuid: Callable[..., ast_module.UUID] = ...
    """
    Return a UUID (universally unique identifier), a string that can be used as
    a unique ID. If a string is passed to uuid as an argument, the UUID will be
    deterministic, derived from the string's SHA-1 hash.
    """
    wednesday: RqlQuery = ...
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """

r: RethinkDB
