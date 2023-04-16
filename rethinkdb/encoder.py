# Copyright 2022 - present RethinkDB
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
Encoder module contains classes and helper functions to encode queries and
decode pseudo-type objects to Python native objects.
"""

import base64
from datetime import datetime
import json
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from rethinkdb.ast import ReqlBinary, ReqlQuery, ReqlTzinfo
from rethinkdb.errors import ReqlDriverError

__all__ = ["ReqlEncoder", "ReqlDecoder"]


class ReqlEncoder(json.JSONEncoder):
    """
    Default JSONEncoder subclass to handle query conversion.
    """

    def __init__(
        self,
        *,
        skipkeys: bool = False,
        ensure_ascii: bool = False,
        check_circular: bool = False,
        allow_nan: bool = False,
        sort_keys: bool = False,
        indent: Optional[int] = None,
        separators: Optional[Tuple[str, str]] = (",", ":"),
        default: Optional[Callable[[Any], Any]] = None,
    ):
        super().__init__(
            skipkeys=skipkeys,
            ensure_ascii=ensure_ascii,
            check_circular=check_circular,
            allow_nan=allow_nan,
            sort_keys=sort_keys,
            indent=indent,
            separators=separators,
            default=default,
        )

    def default(self, o: Any) -> Any:
        """
        Return a serializable object for ``o``.

        :raises: TypeError
        """

        if isinstance(o, ReqlQuery):
            return o.build()

        return super().default(o)


class ReqlDecoder(json.JSONDecoder):
    """
    Default JSONDecoder subclass to handle pseudo-type conversion.
    """

    def __init__(
        self,
        object_hook: Optional[Callable[[Dict[str, Any]], Any]] = None,
        parse_float: Optional[Callable[[str], Any]] = None,
        parse_int: Optional[Callable[[str], Any]] = None,
        parse_constant: Optional[Callable[[str], Any]] = None,
        strict: bool = True,
        object_pairs_hook: Optional[Callable[[List[Tuple[str, Any]]], Any]] = None,
        reql_format_opts: Optional[Dict[str, Any]] = None,
    ):
        custom_object_hook = object_hook or self.convert_pseudo_type

        super().__init__(
            object_hook=custom_object_hook,
            parse_float=parse_float,
            parse_int=parse_int,
            parse_constant=parse_constant,
            strict=strict,
            object_pairs_hook=object_pairs_hook,
        )

        self.reql_format_opts = reql_format_opts or {}

    @staticmethod
    def convert_time(obj: Dict[str, Any]) -> datetime:
        """
        Convert pseudo-type TIME object to Python datetime object.

        :raises: ReqlDriverError
        """

        if "epoch_time" not in obj:
            raise ReqlDriverError(
                f"pseudo-type TIME object {json.dumps(obj)} does not "
                'have expected field "epoch_time".'
            )

        if "timezone" in obj:
            return datetime.fromtimestamp(
                obj["epoch_time"], ReqlTzinfo(obj["timezone"])
            )

        return datetime.utcfromtimestamp(obj["epoch_time"])

    @staticmethod
    def convert_grouped_data(obj: Dict[str, Any]) -> dict:
        """
        Convert pseudo-type GROUPED_DATA object to Python dictionary.

        :raises: ReqlDriverError
        """

        if "data" not in obj:
            raise ReqlDriverError(
                f"pseudo-type GROUPED_DATA object {json.dumps(obj)} does not"
                'have the expected field "data".'
            )

        return {make_hashable(k): v for k, v in obj["data"]}

    @staticmethod
    def convert_binary(obj: Dict[str, Any]) -> bytes:
        """
        Convert pseudo-type BINARY object to Python bytes object.

        :raises: ReqlDriverError
        """

        if "data" not in obj:
            raise ReqlDriverError(
                f"pseudo-type BINARY object {json.dumps(obj)} does not have "
                'the expected field "data".'
            )

        return ReqlBinary(base64.b64decode(obj["data"].encode("utf-8")))

    def __convert_pseudo_type(
        self, obj: Dict[str, Any], format_name: str, converter: Callable
    ) -> Any:
        """
        Convert pseudo-type objects using the given converter.

        :raises: ReqlDriverError
        """

        pseudo_type_format = self.reql_format_opts.get(format_name)

        if pseudo_type_format is None or pseudo_type_format == "native":
            return converter(obj)

        if pseudo_type_format != "raw":
            raise ReqlDriverError(
                f'Unknown {format_name} run option "{pseudo_type_format}".'
            )

        return None

    def convert_pseudo_type(self, obj: Dict[str, Any]) -> Any:
        """
        Convert pseudo-type objects using the given converter.

        :raises: ReqlDriverError
        """

        reql_type = obj.get("$reql_type$")

        converter = {
            None: lambda x: x,
            "GEOMETRY": lambda x: x,
            "BINARY": lambda x: self.__convert_pseudo_type(
                x, "binary_format", self.convert_binary
            ),
            "GROUPED_DATA": lambda x: self.__convert_pseudo_type(
                x, "group_format", self.convert_grouped_data
            ),
            "TIME": lambda x: self.__convert_pseudo_type(
                x, "time_format", self.convert_time
            ),
        }

        converted_type = converter.get(reql_type, lambda x: None)(obj)

        if converted_type is not None:
            return converted_type

        raise ReqlDriverError(f'Unknown pseudo-type "{reql_type}"')


# pylint: disable=consider-using-generator
def make_hashable(obj: Dict[str, Any]) -> Union[tuple, frozenset, dict]:
    """
    Python only allows immutable built-in types to be hashed, such as for keys in
    a dict. This means we can't use lists or dicts as keys in grouped data objects,
    so we convert them to tuples and frozen sets, respectively. This may make it a
    little harder for users to work with converted grouped data, unless they do a
    simple iteration over the result.
    """

    if isinstance(obj, list):
        return tuple([make_hashable(i) for i in obj])

    if isinstance(obj, dict):
        return frozenset([(k, make_hashable(v)) for k, v in obj.items()])

    return obj
