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

from unittest.mock import Mock, patch

from rethinkdb import query


@patch("rethinkdb.query.ast")
def test_json(mock_ast):
    mock_ast.Json.return_value = Mock()

    result = query.json("foo")
    mock_ast.Json.assert_called_once_with("foo")

    assert result == mock_ast.Json.return_value


@patch("rethinkdb.query.ast")
def test_js(mock_ast):
    mock_ast.JavaScript.return_value = Mock()

    result = query.js("foo", foo="foo")
    mock_ast.JavaScript.assert_called_once_with("foo", foo="foo")

    assert result == mock_ast.JavaScript.return_value


@patch("rethinkdb.query.ast")
def test_args(mock_ast):
    mock_ast.Args.return_value = Mock()

    result = query.args("foo")
    mock_ast.Args.assert_called_once_with("foo")

    assert result == mock_ast.Args.return_value


@patch("rethinkdb.query.ast")
def test_http(mock_ast):
    mock_ast.Http.return_value = Mock()
    mock_ast.func_wrap.return_value = Mock()

    result = query.http("foo", foo="foo")

    mock_ast.func_wrap.assert_called_once_with("foo")
    mock_ast.Http.assert_called_once_with(mock_ast.func_wrap.return_value, foo="foo")
    assert result == mock_ast.Http.return_value


@patch("rethinkdb.query.ast")
def test_error(mock_ast):
    mock_ast.UserError.return_value = Mock()

    result = query.error("foo")
    mock_ast.UserError.assert_called_once_with("foo")

    assert result == mock_ast.UserError.return_value


@patch("rethinkdb.query.ast")
def test_random(mock_ast):
    mock_ast.Random.return_value = Mock()

    result = query.random("foo", foo="foo")
    mock_ast.Random.assert_called_once_with("foo", foo="foo")

    assert result == mock_ast.Random.return_value


@patch("rethinkdb.query.ast")
def test_do(mock_ast):
    mock_ast.FunCall.return_value = Mock()

    result = query.do("foo")
    mock_ast.FunCall.assert_called_once_with("foo")

    assert result == mock_ast.FunCall.return_value


@patch("rethinkdb.query.ast")
def test_table(mock_ast):
    mock_ast.Table.return_value = Mock()

    result = query.table("foo", foo="foo")
    mock_ast.Table.assert_called_once_with("foo", foo="foo")

    assert result == mock_ast.Table.return_value


@patch("rethinkdb.query.ast")
def test_db(mock_ast):
    mock_ast.DB.return_value = Mock()

    result = query.db("foo")
    mock_ast.DB.assert_called_once_with("foo")

    assert result == mock_ast.DB.return_value


@patch("rethinkdb.query.ast")
def test_db_create(mock_ast):
    mock_ast.DbCreate.return_value = Mock()

    result = query.db_create("foo")
    mock_ast.DbCreate.assert_called_once_with("foo")

    assert result == mock_ast.DbCreate.return_value


@patch("rethinkdb.query.ast")
def test_db_drop(mock_ast):
    mock_ast.DbDrop.return_value = Mock()

    result = query.db_drop("foo")
    mock_ast.DbDrop.assert_called_once_with("foo")

    assert result == mock_ast.DbDrop.return_value


@patch("rethinkdb.query.ast")
def test_db_list(mock_ast):
    mock_ast.DbList.return_value = Mock()

    result = query.db_list("foo")
    mock_ast.DbList.assert_called_once_with("foo")

    assert result == mock_ast.DbList.return_value


@patch("rethinkdb.query.ast")
def test_db_config(mock_ast):
    mock_ast.TableCreateTL.return_value = Mock()

    result = query.db_config("foo")
    mock_ast.Config.assert_called_once_with("foo")

    assert result == mock_ast.Config.return_value


@patch("rethinkdb.query.ast")
def test_table_create(mock_ast):
    mock_ast.TableCreateTL.return_value = Mock()

    result = query.table_create("foo", foo="foo")
    mock_ast.TableCreateTL.assert_called_once_with("foo", foo="foo")

    assert result == mock_ast.TableCreateTL.return_value


@patch("rethinkdb.query.ast")
def test_table_drop(mock_ast):
    mock_ast.TableDropTL.return_value = Mock()

    result = query.table_drop("foo")
    mock_ast.TableDropTL.assert_called_once_with("foo")

    assert result == mock_ast.TableDropTL.return_value


@patch("rethinkdb.query.ast")
def test_table_list(mock_ast):
    mock_ast.TableListTL.return_value = Mock()

    result = query.table_list("foo")
    mock_ast.TableListTL.assert_called_once_with("foo")

    assert result == mock_ast.TableListTL.return_value


@patch("rethinkdb.query.ast")
def test_grant(mock_ast):
    mock_ast.GrantTL.return_value = Mock()

    result = query.grant("foo", foo="foo")
    mock_ast.GrantTL.assert_called_once_with("foo", foo="foo")

    assert result == mock_ast.GrantTL.return_value


@patch("rethinkdb.query.ast")
def test_branch(mock_ast):
    mock_ast.Branch.return_value = Mock()

    result = query.branch("foo")
    mock_ast.Branch.assert_called_once_with("foo")

    assert result == mock_ast.Branch.return_value


@patch("rethinkdb.query.ast")
def test_union(mock_ast):
    mock_ast.Union.return_value = Mock()

    result = query.union("foo")
    mock_ast.Union.assert_called_once_with("foo")

    assert result == mock_ast.Union.return_value


@patch("rethinkdb.query.ast")
def test_map(mock_ast):
    mock_ast.Map.return_value = Mock()

    result = query.map()
    mock_ast.Map.assert_called_once_with()

    assert result == mock_ast.Map.return_value


@patch("rethinkdb.query.ast")
def test_map_with_one_arg(mock_ast):
    mock_ast.Map.return_value = Mock()
    mock_ast.func_wrap.return_value = Mock()

    result = query.map(["foo"])

    mock_ast.func_wrap.assert_called_once_with(["foo"])
    mock_ast.Map.assert_called_once_with(mock_ast.func_wrap.return_value)
    assert result == mock_ast.Map.return_value


@patch("rethinkdb.query.ast")
def test_group(mock_ast):
    mock_ast.Group.return_value = Mock()
    mock_ast.func_wrap.return_value = Mock()

    result = query.group(["foo"])

    mock_ast.func_wrap.assert_called_once_with(["foo"])
    mock_ast.Group.assert_called_once_with(mock_ast.func_wrap.return_value)
    assert result == mock_ast.Group.return_value


@patch("rethinkdb.query.ast")
def test_reduce(mock_ast):
    mock_ast.Reduce.return_value = Mock()
    mock_ast.func_wrap.return_value = Mock()

    result = query.reduce(["foo"])

    mock_ast.func_wrap.assert_called_once_with(["foo"])
    mock_ast.Reduce.assert_called_once_with(mock_ast.func_wrap.return_value)
    assert result == mock_ast.Reduce.return_value


@patch("rethinkdb.query.ast")
def test_count(mock_ast):
    mock_ast.Count.return_value = Mock()
    mock_ast.func_wrap.return_value = Mock()

    result = query.count(["foo"])

    mock_ast.func_wrap.assert_called_once_with(["foo"])
    mock_ast.Count.assert_called_once_with(mock_ast.func_wrap.return_value)
    assert result == mock_ast.Count.return_value


@patch("rethinkdb.query.ast")
def test_sum(mock_ast):
    mock_ast.Sum.return_value = Mock()
    mock_ast.func_wrap.return_value = Mock()

    result = query.sum(["foo"])

    mock_ast.func_wrap.assert_called_once_with(["foo"])
    mock_ast.Sum.assert_called_once_with(mock_ast.func_wrap.return_value)
    assert result == mock_ast.Sum.return_value


@patch("rethinkdb.query.ast")
def test_avg(mock_ast):
    mock_ast.Avg.return_value = Mock()
    mock_ast.func_wrap.return_value = Mock()

    result = query.avg(["foo"])

    mock_ast.func_wrap.assert_called_once_with(["foo"])
    mock_ast.Avg.assert_called_once_with(mock_ast.func_wrap.return_value)
    assert result == mock_ast.Avg.return_value


@patch("rethinkdb.query.ast")
def test_min(mock_ast):
    mock_ast.Min.return_value = Mock()
    mock_ast.func_wrap.return_value = Mock()

    result = query.min(["foo"])

    mock_ast.func_wrap.assert_called_once_with(["foo"])
    mock_ast.Min.assert_called_once_with(mock_ast.func_wrap.return_value)
    assert result == mock_ast.Min.return_value


@patch("rethinkdb.query.ast")
def test_max(mock_ast):
    mock_ast.Max.return_value = Mock()
    mock_ast.func_wrap.return_value = Mock()

    result = query.max(["foo"])

    mock_ast.func_wrap.assert_called_once_with(["foo"])
    mock_ast.Max.assert_called_once_with(mock_ast.func_wrap.return_value)
    assert result == mock_ast.Max.return_value


@patch("rethinkdb.query.ast")
def test_distinct(mock_ast):
    mock_ast.Distinct.return_value = Mock()
    mock_ast.func_wrap.return_value = Mock()

    result = query.distinct(["foo"])

    mock_ast.func_wrap.assert_called_once_with(["foo"])
    mock_ast.Distinct.assert_called_once_with(mock_ast.func_wrap.return_value)
    assert result == mock_ast.Distinct.return_value


@patch("rethinkdb.query.ast")
def test_contains(mock_ast):
    mock_ast.Contains.return_value = Mock()
    mock_ast.func_wrap.return_value = Mock()

    result = query.contains(["foo"])

    mock_ast.func_wrap.assert_called_once_with(["foo"])
    mock_ast.Contains.assert_called_once_with(mock_ast.func_wrap.return_value)
    assert result == mock_ast.Contains.return_value


@patch("rethinkdb.query.ast")
def test_asc(mock_ast):
    mock_ast.Asc.return_value = Mock()
    mock_ast.func_wrap.return_value = Mock()

    result = query.asc(["foo"])

    mock_ast.func_wrap.assert_called_once_with(["foo"])
    mock_ast.Asc.assert_called_once_with(mock_ast.func_wrap.return_value)

    assert result == mock_ast.Asc.return_value


@patch("rethinkdb.query.ast")
def test_desc(mock_ast):
    mock_ast.Desc.return_value = Mock()
    mock_ast.func_wrap.return_value = Mock()

    result = query.desc(["foo"])

    mock_ast.func_wrap.assert_called_once_with(["foo"])
    mock_ast.Desc.assert_called_once_with(mock_ast.func_wrap.return_value)
    assert result == mock_ast.Desc.return_value


@patch("rethinkdb.query.ast")
def test_eq(mock_ast):
    mock_ast.Eq.return_value = Mock()

    result = query.eq("foo")
    mock_ast.Eq.assert_called_once_with("foo")

    assert result == mock_ast.Eq.return_value


@patch("rethinkdb.query.ast")
def test_ne(mock_ast):
    mock_ast.Ne.return_value = Mock()

    result = query.ne("foo")
    mock_ast.Ne.assert_called_once_with("foo")

    assert result == mock_ast.Ne.return_value


@patch("rethinkdb.query.ast")
def test_lt(mock_ast):
    mock_ast.Lt.return_value = Mock()

    result = query.lt("foo")
    mock_ast.Lt.assert_called_once_with("foo")

    assert result == mock_ast.Lt.return_value


@patch("rethinkdb.query.ast")
def test_le(mock_ast):
    mock_ast.Le.return_value = Mock()

    result = query.le("foo")
    mock_ast.Le.assert_called_once_with("foo")

    assert result == mock_ast.Le.return_value


@patch("rethinkdb.query.ast")
def test_gt(mock_ast):
    mock_ast.Gt.return_value = Mock()

    result = query.gt("foo")
    mock_ast.Gt.assert_called_once_with("foo")

    assert result == mock_ast.Gt.return_value


@patch("rethinkdb.query.ast")
def test_ge(mock_ast):
    mock_ast.Ge.return_value = Mock()

    result = query.ge("foo")
    mock_ast.Ge.assert_called_once_with("foo")

    assert result == mock_ast.Ge.return_value


@patch("rethinkdb.query.ast")
def test_add(mock_ast):
    mock_ast.Add.return_value = Mock()

    result = query.add("foo")
    mock_ast.Add.assert_called_once_with("foo")

    assert result == mock_ast.Add.return_value


@patch("rethinkdb.query.ast")
def test_sub(mock_ast):
    mock_ast.Sub.return_value = Mock()

    result = query.sub("foo")
    mock_ast.Sub.assert_called_once_with("foo")

    assert result == mock_ast.Sub.return_value


@patch("rethinkdb.query.ast")
def test_mul(mock_ast):
    mock_ast.Mul.return_value = Mock()

    result = query.mul("foo")
    mock_ast.Mul.assert_called_once_with("foo")

    assert result == mock_ast.Mul.return_value


@patch("rethinkdb.query.ast")
def test_div(mock_ast):
    mock_ast.Div.return_value = Mock()

    result = query.div("foo")
    mock_ast.Div.assert_called_once_with("foo")

    assert result == mock_ast.Div.return_value


@patch("rethinkdb.query.ast")
def test_mod(mock_ast):
    mock_ast.Mod.return_value = Mock()

    result = query.mod("foo")
    mock_ast.Mod.assert_called_once_with("foo")

    assert result == mock_ast.Mod.return_value


@patch("rethinkdb.query.ast")
def test_bit_and(mock_ast):
    mock_ast.BitAnd.return_value = Mock()

    result = query.bit_and("foo")
    mock_ast.BitAnd.assert_called_once_with("foo")

    assert result == mock_ast.BitAnd.return_value


@patch("rethinkdb.query.ast")
def test_bit_or(mock_ast):
    mock_ast.BitOr.return_value = Mock()

    result = query.bit_or("foo")
    mock_ast.BitOr.assert_called_once_with("foo")

    assert result == mock_ast.BitOr.return_value


@patch("rethinkdb.query.ast")
def test_bit_xor(mock_ast):
    mock_ast.BitXor.return_value = Mock()

    result = query.bit_xor("foo")
    mock_ast.BitXor.assert_called_once_with("foo")

    assert result == mock_ast.BitXor.return_value


@patch("rethinkdb.query.ast")
def test_bit_not(mock_ast):
    mock_ast.BitNot.return_value = Mock()

    result = query.bit_not("foo")
    mock_ast.BitNot.assert_called_once_with("foo")

    assert result == mock_ast.BitNot.return_value


@patch("rethinkdb.query.ast")
def test_bit_sal(mock_ast):
    mock_ast.BitSal.return_value = Mock()

    result = query.bit_sal("foo")
    mock_ast.BitSal.assert_called_once_with("foo")

    assert result == mock_ast.BitSal.return_value


@patch("rethinkdb.query.ast")
def test_bit_sar(mock_ast):
    mock_ast.BitSar.return_value = Mock()

    result = query.bit_sar("foo")
    mock_ast.BitSar.assert_called_once_with("foo")

    assert result == mock_ast.BitSar.return_value


@patch("rethinkdb.query.ast")
def test_floor(mock_ast):
    mock_ast.Floor.return_value = Mock()

    result = query.floor("foo")
    mock_ast.Floor.assert_called_once_with("foo")

    assert result == mock_ast.Floor.return_value


@patch("rethinkdb.query.ast")
def test_ceil(mock_ast):
    mock_ast.Ceil.return_value = Mock()

    result = query.ceil("foo")
    mock_ast.Ceil.assert_called_once_with("foo")

    assert result == mock_ast.Ceil.return_value


@patch("rethinkdb.query.ast")
def test_round(mock_ast):
    mock_ast.Round.return_value = Mock()

    result = query.round("foo")
    mock_ast.Round.assert_called_once_with("foo")

    assert result == mock_ast.Round.return_value


@patch("rethinkdb.query.ast")
def test_not_(mock_ast):
    mock_ast.Not.return_value = Mock()

    result = query.not_("foo")
    mock_ast.Not.assert_called_once_with("foo")

    assert result == mock_ast.Not.return_value


@patch("rethinkdb.query.ast")
def test_and_(mock_ast):
    mock_ast.And.return_value = Mock()

    result = query.and_("foo")
    mock_ast.And.assert_called_once_with("foo")

    assert result == mock_ast.And.return_value


@patch("rethinkdb.query.ast")
def test_or_(mock_ast):
    mock_ast.Or.return_value = Mock()

    result = query.or_("foo")
    mock_ast.Or.assert_called_once_with("foo")

    assert result == mock_ast.Or.return_value


@patch("rethinkdb.query.ast")
def test_type_of(mock_ast):
    mock_ast.TypeOf.return_value = Mock()

    result = query.type_of("foo")
    mock_ast.TypeOf.assert_called_once_with("foo")

    assert result == mock_ast.TypeOf.return_value


@patch("rethinkdb.query.ast")
def test_info(mock_ast):
    mock_ast.info.return_value = Mock()

    result = query.info("foo")
    mock_ast.Info.assert_called_once_with("foo")

    assert result == mock_ast.Info.return_value


@patch("rethinkdb.query.ast")
def test_binary(mock_ast):
    mock_ast.Binary.return_value = Mock()

    result = query.binary("foo")
    mock_ast.Binary.assert_called_once_with("foo")

    assert result == mock_ast.Binary.return_value


@patch("rethinkdb.query.ast")
def test_range(mock_ast):
    mock_ast.Range.return_value = Mock()

    result = query.range(["foo"])

    mock_ast.Range.assert_called_once_with(["foo"])
    assert result == mock_ast.Range.return_value


@patch("rethinkdb.query.ast")
def test_make_timezone(mock_ast):
    mock_ast.ReqlTzinfo.return_value = Mock()

    result = query.make_timezone("foo")
    mock_ast.ReqlTzinfo.assert_called_once_with("foo")

    assert result == mock_ast.ReqlTzinfo.return_value


@patch("rethinkdb.query.ast")
def test_time(mock_ast):
    mock_ast.Time.return_value = Mock()

    result = query.time("foo")
    mock_ast.Time.assert_called_once_with("foo")

    assert result == mock_ast.Time.return_value


@patch("rethinkdb.query.ast")
def test_iso8601(mock_ast):
    mock_ast.ISO8601.return_value = Mock()

    result = query.iso8601("foo", foo="foo")
    mock_ast.ISO8601.assert_called_once_with("foo", foo="foo")

    assert result == mock_ast.ISO8601.return_value


@patch("rethinkdb.query.ast")
def test_epoch_time(mock_ast):
    mock_ast.EpochTime.return_value = Mock()

    result = query.epoch_time("foo")
    mock_ast.EpochTime.assert_called_once_with("foo")

    assert result == mock_ast.EpochTime.return_value


@patch("rethinkdb.query.ast")
def test_now(mock_ast):
    mock_ast.Now.return_value = Mock()

    result = query.now("foo")
    mock_ast.Now.assert_called_once_with("foo")

    assert result == mock_ast.Now.return_value


@patch("rethinkdb.query.ast")
def test_literal(mock_ast):
    mock_ast.Literal.return_value = Mock()

    result = query.literal("foo")
    mock_ast.Literal.assert_called_once_with("foo")

    assert result == mock_ast.Literal.return_value


@patch("rethinkdb.query.ast")
def test_object(mock_ast):
    mock_ast.Object.return_value = Mock()

    result = query.object("foo")
    mock_ast.Object.assert_called_once_with("foo")

    assert result == mock_ast.Object.return_value


@patch("rethinkdb.query.ast")
def test_uuid(mock_ast):
    mock_ast.UUID.return_value = Mock()

    result = query.uuid("foo")
    mock_ast.UUID.assert_called_once_with("foo")

    assert result == mock_ast.UUID.return_value


@patch("rethinkdb.query.ast")
def test_geojson(mock_ast):
    mock_ast.GeoJson.return_value = Mock()

    result = query.geojson("foo")
    mock_ast.GeoJson.assert_called_once_with("foo")

    assert result == mock_ast.GeoJson.return_value


@patch("rethinkdb.query.ast")
def test_point(mock_ast):
    mock_ast.Point.return_value = Mock()

    result = query.point("foo")
    mock_ast.Point.assert_called_once_with("foo")

    assert result == mock_ast.Point.return_value


@patch("rethinkdb.query.ast")
def test_line(mock_ast):
    mock_ast.Line.return_value = Mock()

    result = query.line("foo")
    mock_ast.Line.assert_called_once_with("foo")

    assert result == mock_ast.Line.return_value


@patch("rethinkdb.query.ast")
def test_polygon(mock_ast):
    mock_ast.Polygon.return_value = Mock()

    result = query.polygon("foo")
    mock_ast.Polygon.assert_called_once_with("foo")

    assert result == mock_ast.Polygon.return_value


@patch("rethinkdb.query.ast")
def test_distance(mock_ast):
    mock_ast.Distance.return_value = Mock()

    result = query.distance("foo", foo="foo")
    mock_ast.Distance.assert_called_once_with("foo", foo="foo")

    assert result == mock_ast.Distance.return_value


@patch("rethinkdb.query.ast")
def test_intersects(mock_ast):
    mock_ast.Intersects.return_value = Mock()

    result = query.intersects("foo")
    mock_ast.Intersects.assert_called_once_with("foo")

    assert result == mock_ast.Intersects.return_value


@patch("rethinkdb.query.ast")
def test_circle(mock_ast):
    mock_ast.Circle.return_value = Mock()

    result = query.circle("foo", foo="foo")
    mock_ast.Circle.assert_called_once_with("foo", foo="foo")

    assert result == mock_ast.Circle.return_value


def test_monday():
    assert query.monday.compose([], {}) == "r.monday"


def test_tuesday():
    assert query.tuesday.compose([], {}) == "r.tuesday"


def test_wednesday():
    assert query.wednesday.compose([], {}) == "r.wednesday"


def test_thursday():
    assert query.thursday.compose([], {}) == "r.thursday"


def test_friday():
    assert query.friday.compose([], {}) == "r.friday"


def test_saturday():
    assert query.saturday.compose([], {}) == "r.saturday"


def test_sunday():
    assert query.sunday.compose([], {}) == "r.sunday"


def test_january():
    assert query.january.compose([], {}) == "r.january"


def test_february():
    assert query.february.compose([], {}) == "r.february"


def test_march():
    assert query.march.compose([], {}) == "r.march"


def test_april():
    assert query.april.compose([], {}) == "r.april"


def test_may():
    assert query.may.compose([], {}) == "r.may"


def test_june():
    assert query.june.compose([], {}) == "r.june"


def test_july():
    assert query.july.compose([], {}) == "r.july"


def test_august():
    assert query.august.compose([], {}) == "r.august"


def test_september():
    assert query.september.compose([], {}) == "r.september"


def test_october():
    assert query.october.compose([], {}) == "r.october"


def test_november():
    assert query.november.compose([], {}) == "r.november"


def test_december():
    assert query.december.compose([], {}) == "r.december"


def test_minval():
    assert query.minval.compose([], {}) == "r.minval"


def test_maxval():
    assert query.maxval.compose([], {}) == "r.maxval"
