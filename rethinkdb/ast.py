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

"""
AST module contains the way the queries are serialized and deserialized.
"""

# It is known and expected that the ast module will be lot longer than the
# usual module length, so we disabled it.
# pylint: disable=too-many-lines

# FIXME: do a major refactoring and re-enable docstring checks
# pylint: disable=missing-function-docstring,missing-class-docstring

__all__ = ["expr", "RqlQuery", "RqlBinary", "RqlTzinfo"]

from abc import abstractmethod
import base64
import binascii
from collections import abc
import datetime
import threading
from typing import TYPE_CHECKING, Any, Callable, Iterable, List, Mapping, Optional
from typing import Union as TUnion

from rethinkdb import ql2_pb2
from rethinkdb.errors import QueryPrinter, ReqlDriverCompileError, ReqlDriverError
from rethinkdb.repl import Repl
from rethinkdb.utils import EnhancedTuple

if TYPE_CHECKING:
    from rethinkdb.net import Connection

P_TERM = ql2_pb2.Term.TermType  # pylint: disable=invalid-name


class RqlQuery:  # pylint: disable=too-many-public-methods
    """
    The RethinkDB Query object which determines the operations we can request
    from the server.
    """

    def __init__(self, *args, **kwargs: dict):
        self._args = [expr(e) for e in args]
        self.kwargs = {k: expr(v) for k, v in kwargs.items()}
        self.term_type: Optional[int] = None
        self.statement: str = ""

    @abstractmethod
    def compose(self, args, kwargs):
        """Compose the Rql Query"""

    # TODO: add return value
    def run(self, connection: Optional["Connection"] = None, **kwargs: dict):
        """
        Send the query to the server for execution and return the result of the
        evaluation.
        """

        repl = Repl()
        conn = connection or repl.get_connection()

        if conn is None:
            if repl.is_repl_active:
                raise ReqlDriverError(
                    "RqlQuery.run must be given a connection to run on. "
                    "A default connection has been set with "
                    "`repl()` on another thread, but not this one."
                )

            raise ReqlDriverError("ReqlQuery.run must be given a connection to run on.")

        return conn.start(self, **kwargs)

    def __str__(self) -> str:
        """
        Return the string representation of the query.
        """
        return QueryPrinter(self).print_query()

    def __repr__(self) -> str:
        """
        Return the representation string of the object.
        """
        return f"<RqlQuery instance: {self} >"

    def build(self) -> List[str]:
        """
        Compile the query to a json-serializable object.
        """

        # TODO: Have a more specific typing here
        res: List[Any] = [self.term_type, self._args]

        if len(self.kwargs) > 0:
            res.append(self.kwargs)

        return res

    # The following are all operators and methods that operate on
    # Reql queries to build up more complex operations

    # Comparison operators
    def __eq__(self, other):
        return Eq(self, other)

    def __ne__(self, other):
        return Ne(self, other)

    def __lt__(self, other):
        return Lt(self, other)

    def __le__(self, other):
        return Le(self, other)

    def __gt__(self, other):
        return Gt(self, other)

    def __ge__(self, other):
        return Ge(self, other)

    # Numeric operators
    def __invert__(self):
        return Not(self)

    def __add__(self, other):
        return Add(self, other)

    def __radd__(self, other):
        return Add(other, self)

    def __sub__(self, other):
        return Sub(self, other)

    def __rsub__(self, other):
        return Sub(other, self)

    def __mul__(self, other):
        return Mul(self, other)

    def __rmul__(self, other):
        return Mul(other, self)

    def __div__(self, other):
        return Div(self, other)

    def __rdiv__(self, other):
        return Div(other, self)

    def __truediv__(self, other):
        return Div(self, other)

    def __rtruediv__(self, other):
        return Div(other, self)

    def __mod__(self, other):
        return Mod(self, other)

    def __rmod__(self, other):
        return Mod(other, self)

    def __and__(self, other):
        query = And(self, other)
        query.set_infix()
        return query

    def __rand__(self, other):
        query = And(other, self)
        query.set_infix()
        return query

    def __or__(self, other):
        query = Or(self, other)
        query.set_infix()
        return query

    def __ror__(self, other):
        query = Or(other, self)
        query.set_infix()
        return query

    # Non-operator versions of the above
    def eq(self, *args):  # pylint: disable=invalid-name
        """
        Non-operator version of ``__eq__``.
        """
        return Eq(self, *args)

    def ne(self, *args):  # pylint: disable=invalid-name
        """
        Non-operator version of ``__ne__``.
        """
        return Ne(self, *args)

    def lt(self, *args):  # pylint: disable=invalid-name
        """
        Non-operator version of ``__lt__``.
        """
        return Lt(self, *args)

    def le(self, *args):  # pylint: disable=invalid-name
        """
        Non-operator version of ``__le__``.
        """
        return Le(self, *args)

    def gt(self, *args):  # pylint: disable=invalid-name
        """
        Non-operator version of ``__gt__``.
        """
        return Gt(self, *args)

    def ge(self, *args):  # pylint: disable=invalid-name
        """
        Non-operator version of ``__ge__``.
        """
        return Ge(self, *args)

    def add(self, *args):
        """
        Non-operator version of ``__add__``.
        """
        return Add(self, *args)

    def sub(self, *args):
        """
        Non-operator version of ``__sub__``.
        """
        return Sub(self, *args)

    def mul(self, *args):
        """
        Non-operator version of ``__mul__``.
        """
        return Mul(self, *args)

    def div(self, *args):
        """
        Non-operator version of ``__div__``.
        """
        return Div(self, *args)

    def mod(self, *args):
        """
        Non-operator version of ``__mod__``.
        """
        return Mod(self, *args)

    def bit_and(self, *args):
        """
        Bitwise AND operator.

        A bitwise AND is a binary operation that takes two equal-length binary
        representations and performs the logical AND operation on each pair of
        the corresponding bits, which is equivalent to multiplying them. Thus,
        if both bits in the compared position are 1, the bit in the resulting
        binary representation is 1 (1 x 1 = 1); otherwise, the result is
        0 (1 x 0 = 0 and 0 x 0 = 0).
        """
        return BitAnd(self, *args)

    def bit_or(self, *args):
        """
        Bitwise OR operator.

        A bitwise OR is a binary operation that takes two bit patterns of equal
        length and performs the logical inclusive OR operation on each pair of
        corresponding bits. The result in each position is 0 if both bits are 0,
        while otherwise the result is 1.
        """
        return BitOr(self, *args)

    def bit_xor(self, *args):
        """
        Bitwise XOR operator.

        A bitwise XOR is a binary operation that takes two bit patterns of equal
        length and performs the logical exclusive OR operation on each pair of
        corresponding bits. The result in each position is 1 if only the first
        bit is 1 or only the second bit is 1, but will be 0 if both are 0 or
        both are 1. In this we perform the comparison of two bits, being 1 if
        the two bits are different, and 0 if they are the same.
        """
        return BitXor(self, *args)

    def bit_not(self, *args):
        """
        Bitwise NOT operator.

        A bitwise NOT, or complement, is a unary operation that performs logical
        negation on each bit, forming the ones' complement of the given binary
        value. Bits that are 0 become 1, and those that are 1 become 0.
        """
        return BitNot(self, *args)

    def bit_sal(self, *args):
        """
        Bitwise SAL operator.

        In an arithmetic shift (also referred to as signed shift), like a
        logical shift, the bits that slide off the end disappear (except for the
        last, which goes into the carry flag). But in an arithmetic shift, the
        spaces are filled in such a way to preserve the sign of the number being
        slid. For this reason, arithmetic shifts are better suited for signed
        numbers in two's complement format.

        Note: SHL and SAL are the same, and differentiation only happens because
        SAR and SHR (right shifting) has differences in their implementation.
        """
        return BitSal(self, *args)

    def bit_sar(self, *args):
        """
        Bitwise SAR operator.

        In an arithmetic shift (also referred to as signed shift), like a
        logical shift, the bits that slide off the end disappear (except for the
        last, which goes into the carry flag). But in an arithmetic shift, the
        spaces are filled in such a way to preserve the sign of the number being
        slid. For this reason, arithmetic shifts are better suited for signed
        numbers in two's complement format.
        """
        return BitSar(self, *args)

    def floor(self, *args):
        """
        Rounds the given value down, returning the largest integer value less
        than or equal to the given value (the value's floor).
        """
        return Floor(self, *args)

    def ceil(self, *args):
        """
        Rounds the given value up, returning the smallest integer value greater
        than or equal to the given value (the value's ceiling).
        """
        return Ceil(self, *args)

    def round(self, *args):
        """
        Rounds the given value to the nearest whole integer.
        """
        return Round(self, *args)

    def and_(self, *args):
        """
        Non-operator version of ``__and__``.
        """
        return And(self, *args)

    def or_(self, *args):
        """
        Non-operator version of ``__or__``.
        """
        return Or(self, *args)

    def not_(self, *args):
        """
        Non-operator version of ``__not__``.
        """
        return Not(self, *args)

    # N.B. Cannot use 'in' operator because it must return a boolean
    def contains(self, *args):
        """
        When called with values, returns True if a sequence contains all the
        specified values. When called with predicate functions, returns True if
        for each predicate there exists at least one element of the stream where
        that predicate returns True.
        """
        return Contains(self, *[func_wrap(arg) for arg in args])

    def has_fields(self, *args):
        """
        Test if an object has one or more fields. An object has a field if it
        has that key and the key has a non-null value. For instance, the object
        {'a': 1,'b': 2,'c': null} has the fields a and b.

        When applied to a single object, has_fields returns true if the object
        has the fields and false if it does not. When applied to a sequence, it
        will return a new sequence (an array or stream) containing the elements
        that have the specified fields.
        """
        return HasFields(self, *args)

    def with_fields(self, *args):
        """
        Plucks one or more attributes from a sequence of objects, filtering out
        any objects in the sequence that do not have the specified fields.
        Functionally, this is identical to has_fields followed by pluck on a
        sequence.
        """
        return WithFields(self, *args)

    def keys(self, *args):
        """
        Return an array containing all of an object's keys. Note that the keys
        will be sorted as described in Reql data types (for strings,
        lexicographically).
        """
        return Keys(self, *args)

    def values(self, *args):
        """
        Return an array containing all of an object's values. values()
        guarantees the values will come out in the same order as keys.
        """
        return Values(self, *args)

    def changes(self, *args, **kwargs):
        """
        Turn a query into a changefeed, an infinite stream of objects
        representing changes to the query's results as they occur. A changefeed
        may return changes to a table or an individual document ("point"
        changefeed). Commands such as filter or map may be used before the
        changes command to transform or filter the output, and many commands
        that operate on sequences can be chained after changes.
        """
        return Changes(self, *args, **kwargs)

    # Polymorphic object/sequence operations
    def pluck(self, *args):
        """
        Plucks out one or more attributes from either an object or a sequence of
        objects (projection).
        """
        return Pluck(self, *args)

    def without(self, *args):
        """
        The opposite of pluck; takes an object or a sequence of objects, and
        returns them with the specified paths removed.
        """
        return Without(self, *args)

    def do(self, *args):  # pylint: disable=invalid-name
        return FunCall(self, *args)

    def default(self, *args):
        return Default(self, *args)

    def update(self, *args, **kwargs):
        return Update(self, *[func_wrap(arg) for arg in args], **kwargs)

    def replace(self, *args, **kwargs):
        return Replace(self, *[func_wrap(arg) for arg in args], **kwargs)

    def delete(self, *args, **kwargs):
        return Delete(self, *args, **kwargs)

    # Reql type inspection
    def coerce_to(self, *args):
        return CoerceTo(self, *args)

    def ungroup(self, *args):
        return Ungroup(self, *args)

    def type_of(self, *args):
        return TypeOf(self, *args)

    def merge(self, *args):
        return Merge(self, *[func_wrap(arg) for arg in args])

    def append(self, *args):
        return Append(self, *args)

    def prepend(self, *args):
        return Prepend(self, *args)

    def difference(self, *args):
        return Difference(self, *args)

    def set_insert(self, *args):
        return SetInsert(self, *args)

    def set_union(self, *args):
        return SetUnion(self, *args)

    def set_intersection(self, *args):
        return SetIntersection(self, *args)

    def set_difference(self, *args):
        return SetDifference(self, *args)

    # Operator used for get attr / nth / slice. Non-operator versions below
    # in cases of ambiguity
    # TODO
    # Undestand the type of index. Apparently it can be of type slice
    # but of some type accepted by Bracket,
    # which I can't understand where it's defined
    def __getitem__(self, index):
        if not isinstance(index, slice):
            return Bracket(self, index, bracket_operator=True)

        if index.stop:
            return Slice(self, index.start or 0, index.stop, bracket_operator=True)

        return Slice(
            self,
            index.start or 0,
            -1,
            right_bound="closed",
            bracket_operator=True,
        )

    def __iter__(self):
        raise ReqlDriverError(
            "__iter__ called on an RqlQuery object.\n"
            "To iterate over the results of a query, call run first.\n"
            "To iterate inside a query, use map or for_each."
        )

    def get_field(self, *args):
        return GetField(self, *args)

    def nth(self, *args):
        return Nth(self, *args)

    def to_json(self, *args):
        return ToJsonString(self, *args)

    # DEPRECATE: Remove this function in the next release
    def to_json_string(self, *args):
        """
        Function `to_json_string` is an alias for `to_json` and will be removed
        in the future.
        """

        return self.to_json(*args)

    def match(self, *args):
        return Match(self, *args)

    def format(self, *args, **kwargs):
        return Format(self, *args, **kwargs)

    def split(self, *args):
        return Split(self, *args)

    def upcase(self, *args):
        return Upcase(self, *args)

    def downcase(self, *args):
        return Downcase(self, *args)

    def is_empty(self, *args):
        return IsEmpty(self, *args)

    def offsets_of(self, *args):
        return OffsetsOf(self, *[func_wrap(arg) for arg in args])

    def slice(self, *args, **kwargs):
        return Slice(self, *args, **kwargs)

    def skip(self, *args):
        return Skip(self, *args)

    def limit(self, *args):
        return Limit(self, *args)

    def reduce(self, *args):
        return Reduce(self, *[func_wrap(arg) for arg in args])

    def sum(self, *args):
        return Sum(self, *[func_wrap(arg) for arg in args])

    def avg(self, *args):
        return Avg(self, *[func_wrap(arg) for arg in args])

    def min(self, *args, **kwargs):
        return Min(self, *[func_wrap(arg) for arg in args], **kwargs)

    def max(self, *args, **kwargs):
        return Max(self, *[func_wrap(arg) for arg in args], **kwargs)

    def map(self, *args):
        if len(args) > 0:
            # `func_wrap` only the last argument
            return Map(self, *(args[:-1] + (func_wrap(args[-1]),)))

        return Map(self)

    def fold(self, *args, **kwargs):
        if len(args) > 0:
            # `func_wrap` only the last argument before optional arguments
            return Fold(
                self,
                *(args[:-1] + (func_wrap(args[-1]),)),
                **{key: func_wrap(val) for key, val in kwargs.items()},
            )

        return Fold(self)

    def filter(self, *args, **kwargs):
        return Filter(self, *[func_wrap(arg) for arg in args], **kwargs)

    def concat_map(self, *args):
        return ConcatMap(self, *[func_wrap(arg) for arg in args])

    def order_by(self, *args, **kwargs):
        args = [arg if isinstance(arg, (Asc, Desc)) else func_wrap(arg) for arg in args]
        return OrderBy(self, *args, **kwargs)

    def between(self, *args, **kwargs):
        return Between(self, *args, **kwargs)

    def distinct(self, *args, **kwargs):
        return Distinct(self, *args, **kwargs)

    # Can't overload __len__ because Python doesn't allow us to return a non-integer
    def count(self, *args):
        return Count(self, *[func_wrap(arg) for arg in args])

    def union(self, *args, **kwargs):
        func_kwargs = {key: func_wrap(val) for key, val in kwargs.items()}
        return Union(self, *args, **func_kwargs)

    def inner_join(self, *args):
        return InnerJoin(self, *args)

    def outer_join(self, *args):
        return OuterJoin(self, *args)

    def eq_join(self, *args, **kwargs):
        return EqJoin(self, *[func_wrap(arg) for arg in args], **kwargs)

    def zip(self, *args):
        return Zip(self, *args)

    def group(self, *args, **kwargs):
        return Group(self, *[func_wrap(arg) for arg in args], **kwargs)

    def branch(self, *args):
        return Branch(self, *args)

    def for_each(self, *args):
        return ForEach(self, *[func_wrap(arg) for arg in args])

    def info(self, *args):
        return Info(self, *args)

    # Array only operations
    def insert_at(self, *args):
        return InsertAt(self, *args)

    def splice_at(self, *args):
        return SpliceAt(self, *args)

    def delete_at(self, *args):
        return DeleteAt(self, *args)

    def change_at(self, *args):
        return ChangeAt(self, *args)

    def sample(self, *args):
        return Sample(self, *args)

    # Time support
    def to_iso8601(self, *args):
        return ToISO8601(self, *args)

    def to_epoch_time(self, *args):
        return ToEpochTime(self, *args)

    def during(self, *args, **kwargs):
        return During(self, *args, **kwargs)

    def date(self, *args):
        return Date(self, *args)

    def time_of_day(self, *args):
        return TimeOfDay(self, *args)

    def timezone(self, *args):
        return Timezone(self, *args)

    def year(self, *args):
        return Year(self, *args)

    def month(self, *args):
        return Month(self, *args)

    def day(self, *args):
        return Day(self, *args)

    def day_of_week(self, *args):
        return DayOfWeek(self, *args)

    def day_of_year(self, *args):
        return DayOfYear(self, *args)

    def hours(self, *args):
        return Hours(self, *args)

    def minutes(self, *args):
        return Minutes(self, *args)

    def seconds(self, *args):
        return Seconds(self, *args)

    def in_timezone(self, *args):
        return InTimezone(self, *args)

    # Geospatial support
    def to_geojson(self, *args):
        return ToGeoJson(self, *args)

    def distance(self, *args, **kwargs):
        return Distance(self, *args, **kwargs)

    def intersects(self, *args):
        return Intersects(self, *args)

    def includes(self, *args):
        return Includes(self, *args)

    def fill(self, *args):
        return Fill(self, *args)

    def polygon_sub(self, *args):
        return PolygonSub(self, *args)


class RqlBoolOperQuery(RqlQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.statement_infix = ""
        self.infix = False

    def set_infix(self):
        self.infix = True

    def compose(self, args, kwargs):  # pylint: disable=unused-argument
        term_args = [
            (
                EnhancedTuple("r.expr(", args[i], ")")
                if needs_wrap(self._args[i])
                else args[i]
            )
            for i in range(len(args))
        ]

        if self.infix:
            return EnhancedTuple(
                "(",
                EnhancedTuple(
                    *term_args, int_separator=[" ", self.statement_infix, " "]
                ),
                ")",
            )

        return EnhancedTuple(
            "r.",
            self.statement,
            "(",
            EnhancedTuple(*term_args, int_separator=", "),
            ")",
        )


class RqlBiOperQuery(RqlQuery):
    """
    RethinkDB binary query operation.
    """

    def compose(self, args, kwargs):  # pylint: disable=unused-argument
        term_args = [
            (
                EnhancedTuple("r.expr(", args[i], ")")
                if needs_wrap(self._args[i])
                else args[i]
            )
            for i in range(len(args))
        ]

        return EnhancedTuple(
            "(",
            EnhancedTuple(*term_args, int_separator=[" ", self.statement, " "]),
            ")",
        )


class RqlBiCompareOperQuery(RqlBiOperQuery):
    """
    RethinkDB comparison operator query.
    """

    def __init__(self, *args, **kwargs):
        # Detect misuse of infix bitwise operators (&, |) directly in a
        # comparison without parentheses.  If any operand of the comparison
        # was produced via the infix form (identified by `infix` being True),
        # we match the behaviour of the official drivers by raising a compile
        # -time error.

        for arg in args:
            if getattr(arg, "infix", False):
                # Determine a human-readable operator name for the error
                operator_map = {
                    "eq": "==",
                    "ne": "!=",
                    "lt": "<",
                    "le": "<=",
                    "gt": ">",
                    "ge": ">=",
                }

                class_name = self.__class__.__name__.lower()
                operator_name = operator_map.get(class_name, class_name)

                raise ReqlDriverCompileError(
                    f"Calling '{operator_name}' on result of infix bitwise operator:"
                )

        super().__init__(*args, **kwargs)


class RqlTopLevelQuery(RqlQuery):
    def compose(self, args, kwargs):
        args.extend([EnhancedTuple(key, "=", value) for key, value in kwargs.items()])
        return EnhancedTuple(
            "r.", self.statement, "(", EnhancedTuple(*(args), int_separator=", "), ")"
        )


class RqlMethodQuery(RqlQuery):
    def compose(self, args, kwargs):
        if len(args) == 0:
            return EnhancedTuple("r.", self.statement, "()")

        if needs_wrap(self._args[0]):
            args[0] = EnhancedTuple("r.expr(", args[0], ")")

        restargs = args[1:]
        restargs.extend([EnhancedTuple(k, "=", v) for k, v in kwargs.items()])
        restargs = EnhancedTuple(*restargs, int_separator=", ")

        return EnhancedTuple(args[0], ".", self.statement, "(", restargs, ")")


class RqlBracketQuery(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        self.bracket_operator = False

        if "bracket_operator" in kwargs:
            self.bracket_operator = kwargs["bracket_operator"]
            del kwargs["bracket_operator"]

        super().__init__(*args, **kwargs)

    def compose(self, args, kwargs):
        if self.bracket_operator:
            if needs_wrap(self._args[0]):
                args[0] = EnhancedTuple("r.expr(", args[0], ")")
            return EnhancedTuple(
                args[0], "[", EnhancedTuple(*args[1:], int_separator=[","]), "]"
            )

        return super().compose(args, kwargs)


class RqlTzinfo(datetime.tzinfo):
    """
    RethinkDB timezone information.
    """

    def __init__(self, offsetstr):
        super().__init__()

        hours, minutes = map(int, offsetstr.split(":"))

        self.offsetstr = offsetstr
        self.delta = datetime.timedelta(hours=hours, minutes=minutes)

    def __getinitargs__(self):
        # Consciously return a tuple
        return (self.offsetstr,)

    def __copy__(self):
        return RqlTzinfo(self.offsetstr)

    def __deepcopy__(self, memo):
        return RqlTzinfo(self.offsetstr)

    def utcoffset(self, dt):
        return self.delta

    def tzname(self, dt):
        return self.offsetstr

    def dst(self, dt):
        return datetime.timedelta(0)


class Datum(RqlQuery):
    """
    RethinkDB datum query.

    This class handles the conversion of Reql terminal types in both directions
    Going to the server though it does not support R_ARRAY or R_OBJECT as those
    are alternately handled by the MakeArray and MakeObject nodes. Why do this?
    MakeArray and MakeObject are more flexible, allowing us to construct array
    and object expressions from nested Reql expressions. Constructing pure
    R_ARRAYs and R_OBJECTs would require verifying that at all nested levels
    our arrays and objects are composed only of basic types.
    """

    def __init__(self, val):
        super().__init__()
        self.data = val

    def build(self):
        return self.data

    def compose(self, args, kwargs):  # pylint: disable=unused-argument
        return repr(self.data)


class MakeArray(RqlQuery):
    """
    RethinkDB array composer query.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.MAKE_ARRAY

    # pylint: disable=unused-argument
    def compose(self, args, kwargs):
        return EnhancedTuple("[", EnhancedTuple(*args, int_separator=", "), "]")


class MakeObj(RqlQuery):
    def __init__(self, obj_dict: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.MAKE_OBJ

        for key, value in obj_dict.items():
            if not isinstance(key, str):
                raise ReqlDriverCompileError("Object keys must be strings.")

            self.kwargs[key] = expr(value)

    def build(self):
        return self.kwargs

    # pylint: disable=unused-argument
    def compose(self, args, kwargs):
        return EnhancedTuple(
            "r.expr({",
            EnhancedTuple(
                *[
                    EnhancedTuple(repr(key), ": ", value)
                    for key, value in kwargs.items()
                ],
                int_separator=", ",
            ),
            "})",
        )


class Var(RqlQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.VAR

    # pylint: disable=unused-argument
    def compose(self, args, kwargs):
        return "var_" + args[0]


class JavaScript(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.JAVASCRIPT
        self.statement = "js"


class Http(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.HTTP
        self.statement = "http"


class UserError(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.ERROR
        self.statement = "error"


class Random(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.RANDOM
        self.statement = "random"


class Changes(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.CHANGES
        self.statement = "changes"


class Default(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DEFAULT
        self.statement = "default"


class ImplicitVar(RqlQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.IMPLICIT_VAR

    def __call__(self, *args, **kwargs):
        raise TypeError("'r.row' is not callable, use 'r.row[...]' instead")

    # pylint: disable=unused-argument
    def compose(self, args, kwargs):
        return "r.row"


class Eq(RqlBiCompareOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.EQ
        self.statement = "=="


class Ne(RqlBiCompareOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.NE
        self.statement = "!="


class Lt(RqlBiCompareOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.LT
        self.statement = "<"


class Le(RqlBiCompareOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.LE
        self.statement = "<="


class Gt(RqlBiCompareOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.GT
        self.statement = ">"


class Ge(RqlBiCompareOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.GE
        self.statement = ">="


class Not(RqlQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.NOT

    def compose(self, args, kwargs):  # pylint: disable=unused-argument
        if isinstance(self._args[0], Datum):
            args[0] = EnhancedTuple("r.expr(", args[0], ")")

        return EnhancedTuple("(~", args[0], ")")


class Add(RqlBiOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.ADD
        self.statement = "+"


class Sub(RqlBiOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.SUB
        self.statement = "-"


class Mul(RqlBiOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.MUL
        self.statement = "*"


class Div(RqlBiOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DIV
        self.statement = "/"


class Mod(RqlBiOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.MOD
        self.statement = "%"


class BitAnd(RqlBoolOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.BIT_AND
        self.statement = "bit_and"


class BitOr(RqlBoolOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.BIT_OR
        self.statement = "bit_or"


class BitXor(RqlBoolOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.BIT_XOR
        self.statement = "bit_xor"


class BitNot(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.BIT_NOT
        self.statement = "bit_not"


class BitSal(RqlBoolOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.BIT_SAL
        self.statement = "bit_sal"


class BitSar(RqlBoolOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.BIT_SAR
        self.statement = "bit_sar"


class Floor(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.FLOOR
        self.statement = "floor"


class Ceil(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.CEIL
        self.statement = "ceil"


class Round(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.ROUND
        self.statement = "round"


class Append(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.APPEND
        self.statement = "append"


class Prepend(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.PREPEND
        self.statement = "prepend"


class Difference(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DIFFERENCE
        self.statement = "difference"


class SetInsert(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.SET_INSERT
        self.statement = "set_insert"


class SetUnion(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.SET_UNION
        self.statement = "set_union"


class SetIntersection(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.SET_INTERSECTION
        self.statement = "set_intersection"


class SetDifference(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.SET_DIFFERENCE
        self.statement = "set_difference"


class Slice(RqlBracketQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.SLICE
        self.statement = "slice"

    # Slice has a special bracket syntax, implemented here
    def compose(self, args, kwargs):
        if self.bracket_operator:
            if needs_wrap(self._args[0]):
                args[0] = EnhancedTuple("r.expr(", args[0], ")")

            return EnhancedTuple(args[0], "[", args[1], ":", args[2], "]")

        return RqlBracketQuery.compose(self, args, kwargs)


class Skip(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.SKIP
        self.statement = "skip"


class Limit(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.LIMIT
        self.statement = "limit"


class GetField(RqlBracketQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.GET_FIELD
        self.statement = "get_field"


class Bracket(RqlBracketQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.BRACKET
        self.statement = "bracket"


class Contains(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.CONTAINS
        self.statement = "contains"


class HasFields(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.HAS_FIELDS
        self.statement = "has_fields"


class WithFields(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.WITH_FIELDS
        self.statement = "with_fields"


class Keys(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.KEYS
        self.statement = "keys"


class Values(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.VALUES
        self.statement = "values"


class Object(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.OBJECT
        self.statement = "object"


class Pluck(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.PLUCK
        self.statement = "pluck"


class Without(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.WITHOUT
        self.statement = "without"


class Merge(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.MERGE
        self.statement = "merge"


class Between(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.BETWEEN
        self.statement = "between"


class DB(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DB
        self.statement = "db"

    def table_list(self, *args):
        return TableList(self, *args)

    def config(self, *args):
        return Config(self, *args)

    def wait(self, *args, **kwargs):
        return Wait(self, *args, **kwargs)

    def reconfigure(self, *args, **kwargs):
        return Reconfigure(self, *args, **kwargs)

    def rebalance(self, *args, **kwargs):
        return Rebalance(self, *args, **kwargs)

    def grant(self, *args, **kwargs):
        return Grant(self, *args, **kwargs)

    def table_create(self, *args, **kwargs):
        return TableCreate(self, *args, **kwargs)

    def table_drop(self, *args):
        return TableDrop(self, *args)

    def table(self, *args, **kwargs):
        return Table(self, *args, **kwargs)


class FunCall(RqlQuery):
    # This object should be constructed with arguments first, and the
    # function itself as the last parameter.  This makes it easier for
    # the places where this object is constructed.  The actual wire
    # format is function first, arguments last, so we flip them around
    # before passing it down to the base class constructor.
    def __init__(self, *args, **kwargs):
        if len(args) == 0:
            raise ReqlDriverCompileError("Expected 1 or more arguments but found 0.")

        args = [func_wrap(args[-1])] + list(args[:-1])
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.FUNCALL

    def compose(self, args, kwargs):  # pylint: disable=unused-argument
        if len(args) != 2:
            return EnhancedTuple(
                "r.do(",
                EnhancedTuple(
                    EnhancedTuple(*(args[1:]), int_separator=", "),
                    args[0],
                    int_separator=", ",
                ),
                ")",
            )

        if isinstance(self._args[1], Datum):
            args[1] = EnhancedTuple("r.expr(", args[1], ")")

        return EnhancedTuple(args[1], ".do(", args[0], ")")


class Table(RqlQuery):  # pylint: disable=too-many-public-methods
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.TABLE
        self.statement = "table"

    def insert(self, *args, **kwargs):
        return Insert(self, *[expr(arg) for arg in args], **kwargs)

    def get(self, *args):
        return Get(self, *args)

    def get_all(self, *args, **kwargs):
        return GetAll(self, *args, **kwargs)

    def set_write_hook(self, *args, **kwargs):
        return SetWriteHook(self, *args, **kwargs)

    def get_write_hook(self, *args, **kwargs):
        return GetWriteHook(self, *args, **kwargs)

    def index_create(self, *args, **kwargs):
        if len(args) > 1:
            args = [args[0]] + [func_wrap(arg) for arg in args[1:]]

        return IndexCreate(self, *args, **kwargs)

    def index_drop(self, *args):
        return IndexDrop(self, *args)

    def index_rename(self, *args, **kwargs):
        return IndexRename(self, *args, **kwargs)

    def index_list(self, *args):
        return IndexList(self, *args)

    def index_status(self, *args):
        return IndexStatus(self, *args)

    def index_wait(self, *args):
        return IndexWait(self, *args)

    def status(self, *args):
        return Status(self, *args)

    def config(self, *args):
        return Config(self, *args)

    def wait(self, *args, **kwargs):
        return Wait(self, *args, **kwargs)

    def reconfigure(self, *args, **kwargs):
        return Reconfigure(self, *args, **kwargs)

    def rebalance(self, *args, **kwargs):
        return Rebalance(self, *args, **kwargs)

    def sync(self, *args):
        return Sync(self, *args)

    def grant(self, *args, **kwargs):
        return Grant(self, *args, **kwargs)

    def get_intersecting(self, *args, **kwargs):
        return GetIntersecting(self, *args, **kwargs)

    def get_nearest(self, *args, **kwargs):
        return GetNearest(self, *args, **kwargs)

    def uuid(self, *args, **kwargs):
        return UUID(self, *args, **kwargs)

    def compose(self, args, kwargs):
        args.extend([EnhancedTuple(k, "=", v) for k, v in kwargs.items()])

        if isinstance(self._args[0], DB):
            return EnhancedTuple(
                args[0], ".table(", EnhancedTuple(*(args[1:]), int_separator=", "), ")"
            )

        return EnhancedTuple(
            "r.table(", EnhancedTuple(*(args), int_separator=", "), ")"
        )


class Get(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.GET
        self.statement = "get"


class GetAll(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.GET_ALL
        self.statement = "get_all"


class GetIntersecting(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.GET_INTERSECTING
        self.statement = "get_intersecting"


class GetNearest(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.GET_NEAREST
        self.statement = "get_nearest"


class UUID(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.UUID
        self.statement = "uuid"


class Reduce(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.REDUCE
        self.statement = "reduce"


class Sum(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.SUM
        self.statement = "sum"


class Avg(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.AVG
        self.statement = "avg"


class Min(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.MIN
        self.statement = "min"


class Max(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.MAX
        self.statement = "max"


class Map(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.MAP
        self.statement = "map"


class Fold(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.FOLD
        self.statement = "fold"


class Filter(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.FILTER
        self.statement = "filter"


class ConcatMap(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.CONCAT_MAP
        self.statement = "concat_map"


class OrderBy(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.ORDER_BY
        self.statement = "order_by"


class Distinct(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DISTINCT
        self.statement = "distinct"


class Count(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.COUNT
        self.statement = "count"


class Union(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.UNION
        self.statement = "union"


class Nth(RqlBracketQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.NTH
        self.statement = "nth"


class Match(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.MATCH
        self.statement = "match"


class Format(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.FORMAT
        self.statement = "format"


class ToJsonString(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.TO_JSON_STRING
        self.statement = "to_json_string"


class Split(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.SPLIT
        self.statement = "split"


class Upcase(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.UPCASE
        self.statement = "upcase"


class Downcase(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DOWNCASE
        self.statement = "downcase"


class OffsetsOf(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.OFFSETS_OF
        self.statement = "offsets_of"


class IsEmpty(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.IS_EMPTY
        self.statement = "is_empty"


class Group(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.GROUP
        self.statement = "group"


class InnerJoin(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.INNER_JOIN
        self.statement = "inner_join"


class OuterJoin(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.OUTER_JOIN
        self.statement = "outer_join"


class EqJoin(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.EQ_JOIN
        self.statement = "eq_join"


class Zip(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.ZIP
        self.statement = "zip"


class CoerceTo(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.COERCE_TO
        self.statement = "coerce_to"


class Ungroup(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.UNGROUP
        self.statement = "ungroup"


class TypeOf(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.TYPE_OF
        self.statement = "type_of"


class Update(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.UPDATE
        self.statement = "update"


class Delete(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DELETE
        self.statement = "delete"


class Replace(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.REPLACE
        self.statement = "replace"


class Insert(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.INSERT
        self.statement = "insert"


class DbCreate(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DB_CREATE
        self.statement = "db_create"


class DbDrop(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DB_DROP
        self.statement = "db_drop"


class DbList(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DB_LIST
        self.statement = "db_list"


class TableCreate(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.TABLE_CREATE
        self.statement = "table_create"


class TableCreateTL(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.TABLE_CREATE
        self.statement = "table_create"


class TableDrop(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.TABLE_DROP
        self.statement = "table_drop"


class TableDropTL(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.TABLE_DROP
        self.statement = "table_drop"


class TableList(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.TABLE_LIST
        self.statement = "table_list"


class TableListTL(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.TABLE_LIST
        self.statement = "table_list"


class SetWriteHook(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.SET_WRITE_HOOK
        self.statement = "set_write_hook"


class GetWriteHook(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.GET_WRITE_HOOK
        self.statement = "get_write_hook"


class IndexCreate(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.INDEX_CREATE
        self.statement = "index_create"


class IndexDrop(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.INDEX_DROP
        self.statement = "index_drop"


class IndexRename(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.INDEX_RENAME
        self.statement = "index_rename"


class IndexList(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.INDEX_LIST
        self.statement = "index_list"


class IndexStatus(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.INDEX_STATUS
        self.statement = "index_status"


class IndexWait(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.INDEX_WAIT
        self.statement = "index_wait"


class Config(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.CONFIG
        self.statement = "config"


class Status(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.STATUS
        self.statement = "status"


class Wait(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.WAIT
        self.statement = "wait"


class Reconfigure(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.RECONFIGURE
        self.statement = "reconfigure"


class Rebalance(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.REBALANCE
        self.statement = "rebalance"


class Sync(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.SYNC
        self.statement = "sync"


class Grant(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.GRANT
        self.statement = "grant"


class GrantTL(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.GRANT
        self.statement = "grant"


class Branch(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.BRANCH
        self.statement = "branch"


class Or(RqlBoolOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.OR
        self.statement = "or_"
        self.statement_infix = "|"


class And(RqlBoolOperQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.AND
        self.statement = "and_"
        self.statement_infix = "&"


class ForEach(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.FOR_EACH
        self.statement = "for_each"


class Info(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.INFO
        self.statement = "info"


class InsertAt(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.INSERT_AT
        self.statement = "insert_at"


class SpliceAt(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.SPLICE_AT
        self.statement = "splice_at"


class DeleteAt(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DELETE_AT
        self.statement = "delete_at"


class ChangeAt(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.CHANGE_AT
        self.statement = "change_at"


class Sample(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.SAMPLE
        self.statement = "sample"


class Json(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.JSON
        self.statement = "json"


class Args(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.ARGS
        self.statement = "args"


# Use this class as a wrapper to 'bytes' so we can tell the difference
# in Python2 (when reusing the result of a previous query).
class RqlBinary(bytes):
    def __new__(cls, *args, **kwargs):
        return bytes.__new__(cls, *args, **kwargs)

    def __repr__(self):
        ellipsis = "..." if len(self) > 6 else ""
        excerpt = binascii.hexlify(self[0:6]).decode("utf-8")
        excerpt = " ".join([excerpt[i : i + 2] for i in range(0, len(excerpt), 2)])
        excerpt = f", '{excerpt}{ellipsis}'" if len(self) > 0 else ""

        plural = "s" if len(self) != 1 else ""
        return f"<binary, {str(len(self))} byte{plural}{excerpt}>"


class Binary(RqlTopLevelQuery):
    # Note: this term isn't actually serialized, it should exist only
    # in the client
    def __init__(self, data, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.BINARY
        self.statement = "binary"

        if isinstance(data, RqlQuery):
            self._args.append(data)
            return

        # We only allow 'bytes' objects to be serialized as binary
        if isinstance(data, str):
            raise ReqlDriverCompileError(
                "Cannot convert a unicode string to binary, "
                "use `unicode.encode()` to specify the "
                "encoding."
            )
        if not isinstance(data, bytes):
            raise ReqlDriverCompileError(
                f"Cannot convert {type(data).__name__} to binary, convert the object to a `bytes` "
                f"object first."
            )

        self.base64_data = base64.b64encode(data)

        # Kind of a hack to get around composing
        self._args = []
        self.kwargs = {}

    def compose(self, args, kwargs):
        if len(self._args) == 0:
            return EnhancedTuple("r.", self.statement, "(bytes(<data>))")

        return RqlTopLevelQuery.compose(self, args, kwargs)

    def build(self):
        if len(self._args) == 0:
            return {"$reql_type$": "BINARY", "data": self.base64_data.decode("utf-8")}

        return RqlTopLevelQuery.build(self)


class Range(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.RANGE
        self.statement = "range"


class ToISO8601(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.TO_ISO8601
        self.statement = "to_iso8601"


class During(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DURING
        self.statement = "during"


class Date(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DATE
        self.statement = "date"


class TimeOfDay(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.TIME_OF_DAY
        self.statement = "time_of_day"


class Timezone(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.TIMEZONE
        self.statement = "timezone"


class Year(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.YEAR
        self.statement = "year"


class Month(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.MONTH
        self.statement = "month"


class Day(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DAY
        self.statement = "day"


class DayOfWeek(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DAY_OF_WEEK
        self.statement = "day_of_week"


class DayOfYear(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DAY_OF_YEAR
        self.statement = "day_of_year"


class Hours(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.HOURS
        self.statement = "hours"


class Minutes(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.MINUTES
        self.statement = "minutes"


class Seconds(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.SECONDS
        self.statement = "seconds"


class Time(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.TIME
        self.statement = "time"


class ISO8601(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.ISO8601
        self.statement = "iso8601"


class EpochTime(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.EPOCH_TIME
        self.statement = "epoch_time"


class Now(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.NOW
        self.statement = "now"


class InTimezone(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.IN_TIMEZONE
        self.statement = "in_timezone"


class ToEpochTime(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.TO_EPOCH_TIME
        self.statement = "to_epoch_time"


class GeoJson(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.GEOJSON
        self.statement = "geojson"


class ToGeoJson(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.TO_GEOJSON
        self.statement = "to_geojson"


class Point(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.POINT
        self.statement = "point"


class Line(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.LINE
        self.statement = "line"


class Polygon(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.POLYGON
        self.statement = "polygon"


class Distance(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DISTANCE
        self.statement = "distance"


class Intersects(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.INTERSECTS
        self.statement = "intersects"


class Includes(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.INCLUDES
        self.statement = "includes"


class Circle(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.CIRCLE
        self.statement = "circle"


class Fill(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.FILL
        self.statement = "fill"


class PolygonSub(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.POLYGON_SUB
        self.statement = "polygon_sub"


class Func(RqlQuery):
    lock = threading.Lock()
    nextVarId = 1

    def __init__(self, lmbd, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.FUNC

        variables = []
        variable_ids = []

        try:
            code = lmbd.func_code
        except AttributeError:
            code = lmbd.__code__

        for _ in range(code.co_argcount):
            with Func.lock:
                var_id = Func.nextVarId
                Func.nextVarId += 1

            variables.append(Var(var_id))
            variable_ids.append(var_id)

        self.variables = variables
        self._args.extend([MakeArray(*variable_ids), expr(lmbd(*variables))])

    def compose(self, args, kwargs):  # pylint: disable=unused-argument
        return EnhancedTuple(
            "lambda ",
            EnhancedTuple(
                *[
                    v.compose(
                        # pylint: disable=protected-access
                        [v._args[0].compose(None, None)],
                        [],
                    )
                    for v in self.variables
                ],
                int_separator=", ",
            ),
            ": ",
            args[1],
        )


class Asc(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.ASC
        self.statement = "asc"


class Desc(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.DESC
        self.statement = "desc"


class Literal(RqlTopLevelQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.term_type = P_TERM.LITERAL
        self.statement = "literal"


# Returns True if IMPLICIT_VAR is found in the subquery
def _ivar_scan(query) -> bool:
    if not isinstance(query, RqlQuery):
        return False

    if isinstance(query, ImplicitVar):
        return True

    # pylint: disable=protected-access,use-a-generator
    if any([_ivar_scan(arg) for arg in query._args]):
        return True

    # pylint: disable=use-a-generator
    if any([_ivar_scan(arg) for arg in query.kwargs.values()]):
        return True

    return False


def needs_wrap(arg):
    """
    These classes define how nodes are printed by overloading `compose`.
    """

    return isinstance(arg, (Datum, MakeArray, MakeObj))


# pylint: disable=too-many-return-statements
def expr(
    val: TUnion[
        str,
        bytes,
        RqlQuery,
        RqlBinary,
        datetime.date,
        datetime.datetime,
        Mapping,
        Iterable,
        Callable,
    ],
    nesting_depth: int = 20,
):
    """
    Convert a Python primitive into a Reql primitive value.
    """

    if not isinstance(nesting_depth, int):
        raise ReqlDriverCompileError("Second argument to `r.expr` must be a number.")

    if nesting_depth <= 0:
        raise ReqlDriverCompileError("Nesting depth limit exceeded.")

    if isinstance(val, RqlQuery):
        return val

    if callable(val):
        return Func(val)

    if isinstance(val, str):  # TODO: Default is to return Datum - Remove?
        return Datum(val)

    if isinstance(val, (bytes, RqlBinary)):
        return Binary(val)

    if isinstance(val, abc.Mapping):
        return MakeObj({k: expr(v, nesting_depth - 1) for k, v in val.items()})

    if isinstance(val, abc.Iterable):
        return MakeArray(*[expr(v, nesting_depth - 1) for v in val])  # type: ignore

    if isinstance(val, datetime.date):
        if isinstance(val, datetime.datetime):
            if val.tzinfo is None:
                raise ReqlDriverCompileError(
                    f"""
                Cannot convert {type(val).__name__} to Reql time object
                without timezone information. You can add timezone information with
                the third party module \"pytz\" or by constructing Reql compatible
                timezone values with r.make_timezone(\"[+-]HH:MM\"). Alternatively,
                use one of Reql's bultin time constructors, r.now, r.time,
                or r.iso8601.
                """
                )
            return ISO8601(val.isoformat())

        raise ReqlDriverCompileError(
            f"""
        Cannot convert {type(val).__name__} to Reql time object
        without timezone information. You can add timezone information with
        the third party module \"pytz\" or by constructing Reql compatible
        timezone values with r.make_timezone(\"[+-]HH:MM\"). Alternatively,
        use one of Reql's bultin time constructors, r.now, r.time,
        or r.iso8601.
        """
        )

    return Datum(val)


# Called on arguments that should be functions
# TODO: expr may return different value types. Maybe use a base one?
def func_wrap(val: TUnion[RqlQuery, ImplicitVar, list, dict]):
    val = expr(val)
    if _ivar_scan(val):
        return Func(lambda x: val)

    return val
