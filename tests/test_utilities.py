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

import io
import json
import sys

from rethinkdb.cli.utils import json_default, parse_list_args, print_progress
from rethinkdb.utils import EnhancedTuple, chain_to_bytes


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


def test_print_progress_basic():
    """Test print_progress outputs correct progress bar and percent."""
    import logging

    # Capture logging output
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setLevel(logging.INFO)

    # Get the logger and add our handler
    logger = logging.getLogger("rethinkdb.cli")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    try:
        print_progress(5, 10, prefix="Test")
        print_progress(10, 10, prefix="Test")
    finally:
        logger.removeHandler(handler)

    output = buf.getvalue()
    assert "Test:" in output
    assert "50% (5/10)" in output
    assert "100% (10/10)" in output


def test_parse_list_args():
    """Test parse_list_args parses db and db.table correctly."""
    result = parse_list_args(["db1", "db2.table1", "db2.table2"])
    assert result == {"db1": [], "db2": ["table1", "table2"]}
    result = parse_list_args([])
    assert result == {}


def test_parse_list_args():
    """Test parse_list_args parses db and db.table correctly."""
    result = parse_list_args(["db1", "db2.table1", "db2.table2"])
    assert result == {"db1": [], "db2": ["table1", "table2"]}
    result = parse_list_args([])
    assert result == {}


def test_json_default_datetime():
    """Test json_default serializes objects with isoformat."""

    class Dummy:
        def isoformat(self):
            return "2024-01-01T00:00:00"

    assert json_default(Dummy()) == "2024-01-01T00:00:00"


def test_json_default_other():
    """Test json_default serializes other objects as str."""

    class Dummy:
        def __str__(self):
            return "dummy"

    assert json_default(Dummy()) == "dummy"
    assert json_default(123) == "123"
    assert json_default(None) == "None"
