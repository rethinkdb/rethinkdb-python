# Copyright 2016-2026 RethinkDB
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

from datetime import UTC, date, datetime

import pytest

from rethinkdb import ast, r
from rethinkdb.errors import ReqlDriverCompileError


@pytest.mark.unit
class TestNow:
    def setup_method(self):
        pass

    def test_get_now(self):
        now = r.now()
        assert type(now) == ast.Now


@pytest.mark.unit
def test_expr_date_becomes_iso8601():
    """A calendar date is a date-only ISO 8601 value, midnight UTC in ReQL."""
    result = ast.expr(date(2016, 1, 1))

    assert isinstance(result, ast.ISO8601)
    assert result._args[0].data == "2016-01-01"


@pytest.mark.unit
def test_expr_aware_datetime_becomes_iso8601():
    """An aware datetime keeps its offset in the ISO 8601 string."""
    value = datetime(2016, 1, 1, 12, 30, tzinfo=UTC)
    result = ast.expr(value)

    assert isinstance(result, ast.ISO8601)
    assert result._args[0].data == value.isoformat()


@pytest.mark.unit
def test_expr_naive_datetime_requires_timezone():
    """A naive datetime has no offset, so it cannot become a ReQL time."""
    with pytest.raises(ReqlDriverCompileError, match="without timezone information"):
        ast.expr(datetime(2016, 1, 1, 12, 30))
