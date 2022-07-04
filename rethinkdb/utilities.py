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

"""
Utility functions to make it easier to work with other parts of the code.
"""

from typing import Union

__all__ = ["EnhancedTuple", "chain_to_bytes"]


class EnhancedTuple:  # pylint: disable=too-few-public-methods
    """
    This 'enhanced' tuple recursively iterates over it's elements allowing us to
    construct nested hierarchies that insert subsequences into tree. It's used
    to construct the query representation used by the pretty printer.
    """

    def __init__(self, *sequence, int_separator=""):
        self.sequence = sequence
        self.int_separator = int_separator

    def __iter__(self):
        iterator = iter(self.sequence)

        try:
            for sub in next(iterator):
                yield sub
        except StopIteration:
            return

        for token in iterator:
            for sub in self.int_separator:
                yield sub

            for sub in token:
                yield sub


def chain_to_bytes(*strings: Union[bytes, str]) -> bytes:
    """
    Ensure the bytes and/or strings are chained as bytes.
    """

    return b"".join(
        [
            string.encode("latin-1") if isinstance(string, str) else string
            for string in strings
        ]
    )
