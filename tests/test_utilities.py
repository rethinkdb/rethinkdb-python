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

from rethinkdb.utilities import EnhancedTuple, chain_to_bytes


def test_string_chaining():
    """
    Test strings can be chained into bytes.
    """

    expected_string = b"iron man"
    result = chain_to_bytes("iron", " ", "man")
    assert result == expected_string


def test_byte_chaining():
    """
    Test multiple bytes can be chained into one byte string.
    """

    expected_string = b"iron man"
    result = chain_to_bytes(b"iron", b" ", b"man")
    assert result == expected_string


def test_mixed_chaining():
    """
    Test both strings and bytes can be chained together.
    """

    expected_string = b"iron man"
    result = chain_to_bytes("iron", " ", b"man")
    assert result == expected_string


def test_enhanced_tuple_simple_iteration():
    """
    Test EnhancedTuple iterates on array.
    """

    expected_sequence = [1, 2, 3]
    enhanced_tuple = EnhancedTuple(expected_sequence)

    assert list(enhanced_tuple) == expected_sequence


def test_enhanced_tuple_simple_query():
    """
    Test EnhancedTuple iterates on array.
    """

    expected_sequence = ["r", ".", "e", "x", "p", "r", "(", 1, 2, 3, ")"]
    enhanced_tuple = EnhancedTuple("r.expr(", [1, 2, 3], ")")

    assert list(enhanced_tuple) == expected_sequence


def test_enhanced_tuple_recursive_iteration():
    """
    Test EnhancedTuple iterates recursively.
    """

    expected_sequence = [
        "r",
        ".",
        "e",
        "x",
        "p",
        "r",
        "(",
        "r",
        ".",
        "e",
        "x",
        "p",
        "r",
        "(",
        1,
        2,
        3,
        ")",
        ")",
    ]

    enhanced_tuple = EnhancedTuple(
        "r.expr(", EnhancedTuple("r.expr(", [1, 2, 3], ")"), ")"
    )

    assert list(enhanced_tuple) == expected_sequence
