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

from datetime import datetime
from uuid import UUID

import pytest

from rethinkdb import errors, query
from tests.helpers import (
    Scenario,
    assert_test_table,
    conn,
    create_db,
    create_table,
    create_user,
    delete_user,
    drop_db,
    drop_table,
    get_user,
)

assert conn


@pytest.mark.integration
def test_add(conn):
    scenarios = [
        Scenario(name="two ints", args=[query.args([2, 2])], expected=4),
        Scenario(name="two floats", args=[query.args([2.0, 2.5])], expected=4.5),
        Scenario(name="int and float", args=[query.args([2, 2.5])], expected=4.5),
        Scenario(
            name="strings with args",
            args=[query.args(["Hello", " ", "world"])],
            expected="Hello world",
        ),
    ]

    assert_test_table(query.add, conn, scenarios)


@pytest.mark.integration
def test_and_(conn):
    scenarios = [
        Scenario(name="True-True", args=[True, True], expected=True),
        Scenario(name="True-False", args=[True, False], expected=False),
        Scenario(name="False-True", args=[False, True], expected=False),
        Scenario(name="False-False", args=[False, False], expected=False),
    ]

    assert_test_table(query.and_, conn, scenarios)


@pytest.mark.integration
def test_args(conn):
    scenarios = [
        Scenario(name="empty args", args=[[]], expected=[]),
        Scenario(name="one arg", args=[[1]], expected=[1]),
        Scenario(name="mixed args", args=[[1, "two"]], expected=[1, "two"]),
        Scenario(name="three args", args=[[1, "two", 3.0]], expected=[1, "two", 3.0]),
        Scenario(
            name="all type args",
            args=[[1, "two", 3.0, True]],
            expected=[1, "two", 3.0, True],
        ),
    ]

    assert_test_table(query.args, conn, scenarios)


@pytest.mark.integration
def test_avg(conn):
    scenarios = [
        Scenario(name="int avg", args=[[1, 1]], expected=1),
        Scenario(name="float avg", args=[[1, 2]], expected=1.5),
        Scenario(name="float avg with expr", expr_args=[[1, 2]], expected=1.5),
    ]

    assert_test_table(query.avg, conn, scenarios)


@pytest.mark.integration
def test_bit_and(conn):
    scenarios = [
        Scenario(name="bit and", args=[3], expr_args=[5], expected=1),
    ]

    assert_test_table(query.bit_and, conn, scenarios)


@pytest.mark.integration
def test_bit_not(conn):
    scenarios = [
        Scenario(name="bit not", expr_args=[7], expected=-8),
    ]

    assert_test_table(query.bit_not, conn, scenarios)


@pytest.mark.integration
def test_bit_or(conn):
    scenarios = [
        Scenario(name="bit or", args=[3], expr_args=[5], expected=7),
    ]

    assert_test_table(query.bit_or, conn, scenarios)


@pytest.mark.integration
def test_bit_sal(conn):
    scenarios = [
        Scenario(name="bit sal", args=[4], expr_args=[5], expected=80),
    ]

    assert_test_table(query.bit_sal, conn, scenarios)


@pytest.mark.integration
def test_bit_sar(conn):
    scenarios = [
        Scenario(name="bit or", args=[3], expr_args=[32], expected=4),
    ]

    assert_test_table(query.bit_sar, conn, scenarios)


@pytest.mark.integration
def test_bit_xor(conn):
    scenarios = [
        Scenario(name="bit or", args=[4], expr_args=[6], expected=2),
    ]

    assert_test_table(query.bit_xor, conn, scenarios)


@pytest.mark.integration
def test_branch(conn):
    scenarios = [
        Scenario(name="if-else", args=[True, 1, 2], expected=1),
        Scenario(name="if-elif-else", args=[False, 1, True, 2, 3], expected=2),
    ]

    assert_test_table(query.branch, conn, scenarios)


@pytest.mark.integration
def test_ceil(conn):
    scenarios = [
        Scenario(name="positive number", args=[12.345], expected=13.0),
        Scenario(name="negative number", args=[-12.345], expected=-12.0),
    ]

    assert_test_table(query.ceil, conn, scenarios)


@pytest.mark.integration
def test_circle(conn):
    scenarios = [
        Scenario(
            name="circle",
            args=[[-122.423246, 37.779388], 1000],
            expected={
                "$reql_type$": "GEOMETRY",
                "coordinates": [
                    [
                        [-122.423246, 37.77037835958964],
                        [-122.4254602829791, 37.770551456627935],
                        [-122.42758950229495, 37.771064098057934],
                        [-122.42955185866998, 37.77189659001295],
                        [-122.43127195684315, 37.7730169502562],
                        [-122.43268370038074, 37.77438213572423],
                        [-122.43373283081442, 37.775939694859375],
                        [-122.43437901366829, 37.77762978150455],
                        [-122.43459739107298, 37.779387453240034],
                        [-122.43437954090827, 37.78114516606947],
                        [-122.43373380502685, 37.78283536974031],
                        [-122.43268497325062, 37.78439310401677],
                        [-122.4312733345876, 37.78575849607807],
                        [-122.42955313153989, 37.78687906291455],
                        [-122.42759047650742, 37.787711730010884],
                        [-122.42546081021908, 37.78822448846657],
                        [-122.423246, 37.78839762659882],
                        [-122.42103118978093, 37.78822448846657],
                        [-122.4189015234926, 37.787711730010884],
                        [-122.41693886846012, 37.78687906291455],
                        [-122.41521866541241, 37.78575849607807],
                        [-122.4138070267494, 37.78439310401677],
                        [-122.41275819497316, 37.78283536974031],
                        [-122.41211245909174, 37.78114516606947],
                        [-122.41189460892703, 37.779387453240034],
                        [-122.41211298633172, 37.77762978150455],
                        [-122.41275916918559, 37.775939694859375],
                        [-122.41380829961928, 37.77438213572423],
                        [-122.41522004315686, 37.7730169502562],
                        [-122.41694014133003, 37.77189659001295],
                        [-122.41890249770506, 37.771064098057934],
                        [-122.42103171702091, 37.770551456627935],
                        [-122.423246, 37.77037835958964],
                    ]
                ],
                "type": "Polygon",
            },
        ),
    ]

    assert_test_table(query.circle, conn, scenarios)


@pytest.mark.integration
def test_contains(conn):
    scenarios = [
        Scenario(name="has number", args=[1], expr_args=[[1, 2]], expected=True),
        Scenario(name="has string", args=["1"], expr_args=[["1", "2"]], expected=True),
        Scenario(name="has no number", args=[3], expr_args=[[1]], expected=False),
        Scenario(name="has no string", args=["3"], expr_args=[["1"]], expected=False),
    ]

    assert_test_table(query.contains, conn, scenarios)


@pytest.mark.integration
def test_count(conn):
    scenarios = [
        Scenario(name="sequence", expr_args=[[0, 1, 2]], expected=3),
        Scenario(name="string", expr_args=["hello"], expected=5),
        Scenario(
            name="predicate", args=[lambda x: x > 1], expr_args=[[0, 1, 2]], expected=1
        ),
    ]

    assert_test_table(query.count, conn, scenarios)


@pytest.mark.integration
def test_db_create(conn):
    scenarios = [
        Scenario(
            name="create db",
            args=["superheroes"],
            expected=1,
            expected_field="dbs_created",
            callback=lambda: drop_db("superheroes", conn),
        ),
    ]

    assert_test_table(query.db_create, conn, scenarios)


@pytest.mark.integration
def test_db_create(conn):
    # NOTE: This can leave resource behind -- manual cleanup maybe necessary
    # Create first to have something to drop
    create_db("superheroes", conn)

    scenarios = [
        Scenario(
            name="drop db",
            args=["superheroes"],
            expected=1,
            expected_field="dbs_dropped",
        ),
    ]

    assert_test_table(query.db_drop, conn, scenarios)


@pytest.mark.integration
def test_db_list(conn):
    scenarios = [
        Scenario(name="drop list", expected=["rethinkdb", "test"]),
    ]

    assert_test_table(query.db_list, conn, scenarios)


@pytest.mark.integration
def test_distance(conn):
    scenarios = [
        Scenario(
            name="distance of two points",
            args=[
                query.point(-122.423246, 37.779388),
                query.point(-117.220406, 32.719464),
            ],
            kwargs={"unit": "km"},
            expected=734.125249602186,
        ),
    ]

    assert_test_table(query.distance, conn, scenarios)


@pytest.mark.integration
def test_distinct(conn):
    scenarios = [
        Scenario(
            name="distinct list",
            expr_args=[[1, 1, 2, 3, 3, 4, 5]],
            expected=[1, 2, 3, 4, 5],
        ),
    ]

    assert_test_table(query.distinct, conn, scenarios)


@pytest.mark.integration
def test_div(conn):
    scenarios = [
        Scenario(name="int to int", args=[query.args([2, 1])], expected=2),
        Scenario(name="int to float", args=[query.args([1, 2])], expected=0.5),
        Scenario(name="float to float", args=[query.args([0.5, 0.4])], expected=1.25),
    ]

    assert_test_table(query.div, conn, scenarios)


@pytest.mark.integration
def test_do(conn):
    scenarios = [
        Scenario(
            name="do branch",
            args=[lambda x: query.branch(x, 1, 2)],
            expr_args=[True],
            expected=1,
        ),
    ]

    assert_test_table(query.do, conn, scenarios)


@pytest.mark.integration
def test_epoch_time(conn):
    scenarios = [
        Scenario(
            name="epoch zero",
            args=[0],
            expected="1970-01-01 00:00:00+00:00",
            result_as_string=True,
        ),
        Scenario(
            name="development epoch time",
            args=[1652430156],
            expected="2022-05-13 08:22:36+00:00",
            result_as_string=True,
        ),
    ]

    assert_test_table(query.epoch_time, conn, scenarios)


@pytest.mark.integration
def test_eq(conn):
    scenarios = [
        Scenario(name="two eq", args=[2, 2], expected=True),
        Scenario(name="two eq string", args=["two", "two"], expected=True),
        Scenario(name="three eq", args=[3, 3, 3], expected=True),
        Scenario(
            name="three eq string", args=["three", "three", "three"], expected=True
        ),
    ]

    assert_test_table(query.eq, conn, scenarios)


@pytest.mark.integration
def test_error(conn):
    with pytest.raises(errors.ReqlUserError):
        query.error("error message").run(conn)


@pytest.mark.integration
def test_floor(conn):
    scenarios = [
        Scenario(name="positive number", args=[12.345], expected=12.0),
        Scenario(name="negative number", args=[-12.345], expected=-13.0),
    ]

    assert_test_table(query.floor, conn, scenarios)


@pytest.mark.integration
@pytest.mark.v2_5
def test_format(conn):
    scenarios = [
        Scenario(
            name="text", args=["hello {name}", {"name": "bob"}], expected="hello bob"
        ),
        Scenario(
            name="numbers", args=["1..2..{count}", {"count": 3}], expected="1..2..3"
        ),
        Scenario(
            name="object",
            args=["object: {obj}", {"obj": {"foo": "bar"}}],
            expected='object: {"foo":"bar"}',
        ),
        Scenario(
            name="array",
            args=["array: {arr}", {"arr": [1, 2, 3]}],
            expected="array: [1,2,3]",
        ),
    ]

    assert_test_table(query.format, conn, scenarios)


@pytest.mark.integration
def test_ge(conn):
    scenarios = [
        Scenario(name="int equal", args=[2, 2], expected=True),
        Scenario(name="int greater", args=[3, 2], expected=True),
        Scenario(name="string equal", args=["b", "b"], expected=True),
        Scenario(name="string greater", args=["b", "a"], expected=True),
        Scenario(name="int less", args=[1, 2], expected=False),
        Scenario(name="string less", args=["a", "b"], expected=False),
    ]

    assert_test_table(query.ge, conn, scenarios)


@pytest.mark.integration
def test_geojson(conn):
    scenarios = [
        Scenario(
            name="covert geojson to Reql object",
            args=[{"type": "Point", "coordinates": [-122.423246, 37.779388]}],
            expected={
                "$reql_type$": "GEOMETRY",
                "coordinates": [-122.423246, 37.779388],
                "type": "Point",
            },
        )
    ]

    assert_test_table(query.geojson, conn, scenarios)


@pytest.mark.integration
def test_grant(conn):
    test_user = "bob"

    if get_user(test_user, conn):
        delete_user(test_user, conn)

    create_user(test_user, conn)

    scenarios = [
        Scenario(
            name="grant everything to user",
            args=[test_user, {"read": True, "write": True, "connect": True}],
            expected={
                "granted": 1,
                "permissions_changes": [
                    {
                        "new_val": {"connect": True, "read": True, "write": True},
                        "old_val": None,
                    }
                ],
            },
        ),
        Scenario(
            name="remove connect permission",
            args=[test_user, {"read": True, "write": True, "connect": False}],
            expected={
                "granted": 1,
                "permissions_changes": [
                    {
                        "new_val": {"connect": False, "read": True, "write": True},
                        "old_val": {"connect": True, "read": True, "write": True},
                    }
                ],
            },
        ),
        Scenario(
            name="remove connect and write permissions",
            args=[test_user, {"read": False, "write": True, "connect": False}],
            expected={
                "granted": 1,
                "permissions_changes": [
                    {
                        "new_val": {"connect": False, "read": False, "write": True},
                        "old_val": {"connect": False, "read": True, "write": True},
                    }
                ],
            },
        ),
        Scenario(
            name="remove all permissions",
            args=[test_user, {"read": False, "write": False, "connect": False}],
            expected={
                "granted": 1,
                "permissions_changes": [
                    {
                        "new_val": {"connect": False, "read": False, "write": False},
                        "old_val": {"connect": False, "read": False, "write": True},
                    }
                ],
            },
        ),
    ]

    assert_test_table(query.grant, conn, scenarios)

    # Cleanup the user
    delete_user(test_user, conn)


@pytest.mark.integration
def test_group(conn):
    raw_data = [
        {"id": 2, "player": "Bob", "points": 15, "type": "ranked"},
        {"id": 5, "player": "Alice", "points": 7, "type": "free"},
        {"id": 11, "player": "Bob", "points": 10, "type": "free"},
        {"id": 12, "player": "Alice", "points": 2, "type": "free"},
    ]

    scenarios = [
        Scenario(
            name="group by player",
            args=[raw_data, "player"],
            expected={
                "Alice": [
                    {"id": 5, "player": "Alice", "points": 7, "type": "free"},
                    {"id": 12, "player": "Alice", "points": 2, "type": "free"},
                ],
                "Bob": [
                    {"id": 2, "player": "Bob", "points": 15, "type": "ranked"},
                    {"id": 11, "player": "Bob", "points": 10, "type": "free"},
                ],
            },
        ),
        Scenario(
            name="group by type",
            args=[raw_data, "type"],
            expected={
                "free": [
                    {"id": 5, "player": "Alice", "points": 7, "type": "free"},
                    {"id": 11, "player": "Bob", "points": 10, "type": "free"},
                    {"id": 12, "player": "Alice", "points": 2, "type": "free"},
                ],
                "ranked": [{"id": 2, "player": "Bob", "points": 15, "type": "ranked"}],
            },
        ),
        Scenario(
            name="group by points",
            args=[raw_data, "points"],
            expected={
                2: [{"id": 12, "player": "Alice", "points": 2, "type": "free"}],
                7: [{"id": 5, "player": "Alice", "points": 7, "type": "free"}],
                10: [{"id": 11, "player": "Bob", "points": 10, "type": "free"}],
                15: [{"id": 2, "player": "Bob", "points": 15, "type": "ranked"}],
            },
        ),
    ]

    assert_test_table(query.group, conn, scenarios)


@pytest.mark.integration
def test_gt(conn):
    scenarios = [
        Scenario(name="int gt", args=[3, 2], expected=True),
        Scenario(name="string gt", args=["b", "a"], expected=True),
        Scenario(name="int ge", args=[2, 2], expected=False),
        Scenario(name="string eq", args=["b", "b"], expected=False),
    ]

    assert_test_table(query.gt, conn, scenarios)


@pytest.mark.integration
def test_http(conn):
    scenarios = [
        Scenario(
            name="test rethinkdb admin",
            args=["http://localhost:8080"],
            expected="""<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8">
        <title>RethinkDB Administration Console</title>
        <meta name="description" content="">
        <meta name="author" content="">
        <meta http-equiv="no-cache">
        <meta http-equiv="Expires" content="-1">
        <meta http-equiv="Cache-Control" content="no-cache">

        <link rel="stylesheet" type="text/css" href="cluster.css?v=2.4.1-24-g25364c" />
        <link rel="stylesheet" type="text/css" href="js/chosen/chosen.css?v=2.4.1-24-g25364c" />
        <link rel="stylesheet" type="text/css" href="js/codemirror/codemirror.css?v=2.4.1-24-g25364c" />
        <link rel="stylesheet" type="text/css" href="js/codemirror/ambiance.css?v=2.4.1-24-g25364c" />
        <link rel="stylesheet" type="text/css" href="fonts/stylesheet.css?v=2.4.1-24-g25364c" />
        <link rel="stylesheet" type="text/css" href="js/nanoscroller/nanoscroller.css?v=2.4.1-24-g25364c" />

        <script type="text/javascript" src="js/jquery-1.7.2.min.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/ZeroClipboard.min.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/underscore-min.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/backbone.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/bootstrap/bootstrap-modal.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/bootstrap/bootstrap-alert.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/bootstrap/bootstrap-typeahead.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/bootstrap/bootstrap-button.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/bootstrap/bootstrap-dropdown.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/bootstrap/bootstrap-tooltip.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/bootstrap/bootstrap-popover.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/jquery.form.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/nanoscroller/jquery.nanoscroller.min.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/jquery.color.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/jquery.timeago.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/date-en-US.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/d3.v2.min.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/rdb_cubism.v1.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/xdate.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/chosen/chosen.jquery.min.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="cluster-min.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/codemirror/codemirror.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/codemirror/javascript.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/codemirror/matchbrackets.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/reql_docs.js?v=2.4.1-24-g25364c"></script>
        <script type="text/javascript" src="js/tableview.js?v=2.4.1-24-g25364c"></script>

    </head>
    <body>
        <div class="alert global_loading">Loading...</div>
    </body>
</html>
""",
        )
    ]

    assert_test_table(query.http, conn, scenarios)


@pytest.mark.integration
def test_info(conn):
    scenarios = [
        Scenario(
            name="string info",
            args=["test"],
            expected={"type": "STRING", "value": '"test"'},
        ),
        Scenario(
            name="int info",
            args=[1],
            expected={"type": "NUMBER", "value": "1"},
        ),
        Scenario(
            name="float info",
            args=[1.1],
            expected={"type": "NUMBER", "value": "1.1"},
        ),
        Scenario(
            name="array info",
            args=[[1]],
            expected={"type": "ARRAY", "value": "[\n\t1\n]"},
        ),
        Scenario(
            name="object info",
            args=[{"name": "Bob"}],
            expected={"type": "OBJECT", "value": '{\n\t"name":\t"Bob"\n}'},
        ),
    ]

    assert_test_table(query.info, conn, scenarios)


@pytest.mark.integration
def test_intersects(conn):
    # NOTE: intersects are hard to test using table tests, so test it in the regular way
    expected = True

    point1 = query.point(-117.220406, 32.719464)
    point2 = query.point(-117.220406, 32.719464)
    result = query.circle(point1, 2000).intersects(point2).run(conn)

    assert result is expected, (
        'Failed scenario: "intersect of geometry objects"\n'
        f"\t  wanted: {expected}\n"
        f"\t  got: {result}"
    )


@pytest.mark.integration
def test_iso8601(conn):
    scenarios = [
        Scenario(
            name="return iso8601 format",
            args=["1986-11-03T08:30:00-07:00"],
            expected="1986-11-03 08:30:00-07:00",
            result_as_string=True,
        ),
    ]

    assert_test_table(query.iso8601, conn, scenarios)


@pytest.mark.integration
def test_json(conn):
    scenarios = [
        Scenario(
            name="json array to array",
            args=["[1,2,3]"],
            expected=[1, 2, 3],
        ),
        Scenario(
            name="complex json data to python",
            args=[
                """{
  "product": "Live JSON generator",
  "version": 3.1,
  "releaseDate": "2014-06-25T00:00:00.000Z",
  "demo": true,
  "person": {
    "id": 12345,
    "name": "John Doe",
    "phones": {
      "home": "800-123-4567",
      "mobile": "877-123-1234"
    },
    "email": [
      "jd@example.com",
      "jd@example.org"
    ],
    "dateOfBirth": "1980-01-02T00:00:00.000Z",
    "registered": true,
    "emergencyContacts": [
      {
        "name": "Jane Doe",
        "phone": {}
      }
    ]
  }
}"""
            ],
            expected={
                "demo": True,
                "person": {
                    "dateOfBirth": "1980-01-02T00:00:00.000Z",
                    "email": ["jd@example.com", "jd@example.org"],
                    "emergencyContacts": [{"name": "Jane Doe", "phone": {}}],
                    "id": 12345,
                    "name": "John Doe",
                    "phones": {"home": "800-123-4567", "mobile": "877-123-1234"},
                    "registered": True,
                },
                "product": "Live JSON generator",
                "releaseDate": "2014-06-25T00:00:00.000Z",
                "version": 3.1,
            },
        ),
    ]

    assert_test_table(query.json, conn, scenarios)


@pytest.mark.integration
def test_le(conn):
    scenarios = [
        Scenario(name="int equal", args=[2, 2], expected=True),
        Scenario(name="int less", args=[1, 2], expected=True),
        Scenario(name="string equal", args=["b", "b"], expected=True),
        Scenario(name="string less", args=["a", "b"], expected=True),
        Scenario(name="int greater", args=[2, 1], expected=False),
        Scenario(name="string greater", args=["b", "a"], expected=False),
    ]

    assert_test_table(query.le, conn, scenarios)


@pytest.mark.integration
def test_line(conn):
    scenarios = [
        Scenario(
            name="get a line",
            args=[[-122.423246, 37.779388], [-121.886420, 37.329898]],
            expected={
                "$reql_type$": "GEOMETRY",
                "coordinates": [[-122.423246, 37.779388], [-121.88642, 37.329898]],
                "type": "LineString",
            },
        ),
    ]

    assert_test_table(query.line, conn, scenarios)


@pytest.mark.integration
def test_literal(conn):
    # NOTE: intersects are hard to test using table tests, so test it in the regular way
    expected = {
        "deleted": 0,
        "errors": 0,
        "inserted": 0,
        "replaced": 1,
        "skipped": 0,
        "unchanged": 0,
    }

    create_table("test", "literal", conn)

    query.db("test").table("literal").insert({"id": 1, "data": {"apple": "tree"}}).run(
        conn
    )

    result = (
        query.db("test")
        .table("literal")
        .get(1)
        .update({"data": query.literal({"banana": "tree"})})
        .run(conn)
    )

    drop_table("test", "literal", conn)

    assert result == expected, (
        f'Failed scenario: "literal update"\n'
        f"\t  wanted: \n{expected}"
        f"\t  got: {result}"
    )


@pytest.mark.integration
def test_lt(conn):
    scenarios = [
        Scenario(name="int less", args=[1, 2], expected=True),
        Scenario(name="string less", args=["a", "b"], expected=True),
        Scenario(name="int greater", args=[3, 2], expected=False),
        Scenario(name="string greater", args=["b", "a"], expected=False),
        Scenario(name="int equal", args=[2, 2], expected=False),
        Scenario(name="string equal", args=["b", "b"], expected=False),
    ]

    assert_test_table(query.lt, conn, scenarios)


@pytest.mark.integration
def test_map(conn):
    scenarios = [
        Scenario(
            name="map add",
            args=[lambda x: x + 1],
            expr_args=[[1, 2, 3, 4, 5]],
            expected=[2, 3, 4, 5, 6],
        ),
        Scenario(
            name="map multiply",
            args=[lambda x: x * 2],
            expr_args=[[1, 2, 3, 4, 5]],
            expected=[2, 4, 6, 8, 10],
        ),
    ]

    assert_test_table(query.map, conn, scenarios)


@pytest.mark.integration
def test_max(conn):
    scenarios = [
        Scenario(name="positive ints", expr_args=[[1, 2, 3]], expected=3),
        Scenario(name="negative ints", expr_args=[[-1, -2, -3]], expected=-1),
        Scenario(name="identical ints", expr_args=[[1, 1, 1]], expected=1),
        Scenario(name="identical floats", expr_args=[[1.0, 1.0, 1.0]], expected=1.0),
        Scenario(name="mixed types", expr_args=[[1, 1.1]], expected=1.1),
        Scenario(name="negative mixed types", expr_args=[[-1, -1.1]], expected=-1),
        Scenario(name="strings", expr_args=[["a", "b"]], expected="b"),
        Scenario(name="identical strings", expr_args=[["b", "b"]], expected="b"),
    ]

    assert_test_table(query.max, conn, scenarios)


@pytest.mark.integration
def test_min(conn):
    scenarios = [
        Scenario(name="positive ints", expr_args=[[1, 2, 3]], expected=1),
        Scenario(name="negative ints", expr_args=[[-1, -2, -3]], expected=-3),
        Scenario(name="identical ints", expr_args=[[1, 1, 1]], expected=1),
        Scenario(name="identical floats", expr_args=[[1.0, 1.0, 1.0]], expected=1.0),
        Scenario(name="mixed types", expr_args=[[1, 1.1]], expected=1),
        Scenario(name="negative mixed types", expr_args=[[-1, -1.1]], expected=-1.1),
        Scenario(name="strings", expr_args=[["a", "b"]], expected="a"),
        Scenario(name="identical strings", expr_args=[["b", "b"]], expected="b"),
    ]

    assert_test_table(query.min, conn, scenarios)


@pytest.mark.integration
def test_mod(conn):
    scenarios = [
        Scenario(name="even", args=[2, 2], expected=0),
        Scenario(name="odd", args=[2, 3], expected=2),
        Scenario(name="negative even", args=[-2, 2], expected=0),
        Scenario(name="negative odd", args=[-2, 3], expected=-2),
    ]

    assert_test_table(query.mod, conn, scenarios)


@pytest.mark.integration
def test_mul(conn):
    scenarios = [
        Scenario(name="1x1", args=[1, 1], expected=1),
        Scenario(name="1x2", args=[1, 2], expected=2),
        Scenario(name="2x1", args=[2, 1], expected=2),
        Scenario(name="2x2", args=[2, 2], expected=4),
        Scenario(name="-1x-1", args=[-1, -1], expected=1),
        Scenario(name="-2x-2", args=[-2, -2], expected=4),
        Scenario(name="0x0", args=[0, 0], expected=0),
    ]

    assert_test_table(query.mul, conn, scenarios)


@pytest.mark.integration
def test_ne(conn):
    scenarios = [
        Scenario(name="int equal", args=[2, 2], expected=False),
        Scenario(name="int not equal", args=[3, 2], expected=True),
        Scenario(name="string equal", args=["b", "b"], expected=False),
        Scenario(name="string not equal", args=["b", "a"], expected=True),
    ]

    assert_test_table(query.ne, conn, scenarios)


@pytest.mark.integration
def test_not_(conn):
    scenarios = [
        Scenario(name="not true", args=[True], expected=False),
        Scenario(name="not false", args=[False], expected=True),
    ]

    assert_test_table(query.not_, conn, scenarios)


@pytest.mark.integration
def test_now(conn):
    # This test is not accurate. The sole purpose is to validate the server is
    # returning an approximately correct date, and we are able to parse it.
    client_now = datetime.now()
    server_now: datetime = query.now().run(conn)
    assert server_now.year == client_now.year
    assert server_now.month == client_now.month
    assert server_now.day == client_now.day


@pytest.mark.integration
def test_object(conn):
    scenarios = [
        Scenario(
            name="sample object",
            args=["id", 5, "data", ["foo", "bar"]],
            expected={"id": 5, "data": ["foo", "bar"]},
        )
    ]

    assert_test_table(query.object, conn, scenarios)


@pytest.mark.integration
def test_or_(conn):
    scenarios = [
        Scenario(name="or true", args=[True, True], expected=True),
        Scenario(name="or false", args=[False, False], expected=False),
        Scenario(name="or mixed", args=[True, False], expected=True),
    ]

    assert_test_table(query.or_, conn, scenarios)


@pytest.mark.integration
def test_point(conn):
    scenarios = [
        Scenario(
            name="simple point",
            args=[-122.423246, 37.779388],
            expected={
                "$reql_type$": "GEOMETRY",
                "coordinates": [-122.423246, 37.779388],
                "type": "Point",
            },
        ),
    ]

    assert_test_table(query.point, conn, scenarios)


@pytest.mark.integration
def test_polygon(conn):
    scenarios = [
        Scenario(
            name="simple polygon",
            args=[
                [-122.423246, 37.779388],
                [-122.423246, 37.329898],
                [-121.886420, 37.329898],
                [-121.886420, 37.779388],
            ],
            expected={
                "$reql_type$": "GEOMETRY",
                "coordinates": [
                    [
                        [-122.423246, 37.779388],
                        [-122.423246, 37.329898],
                        [-121.88642, 37.329898],
                        [-121.88642, 37.779388],
                        [-122.423246, 37.779388],
                    ]
                ],
                "type": "Polygon",
            },
        ),
    ]

    assert_test_table(query.polygon, conn, scenarios)


@pytest.mark.integration
def test_random(conn):
    # This test is not accurate. The sole purpose is to validate the server is
    # returning a random number that we can parse. We run the tests in a loop
    # to have some chance to catch issues.
    for _ in range(0, 99):
        assert 0 <= query.random().run(conn) < 1
        assert 0 <= query.random(100).run(conn) < 100
        assert 0 <= query.random(0, 100).run(conn) < 100
        assert -2.24 < query.random(1.59, -2.24, float=True).run(conn) <= 1.59


@pytest.mark.integration
def test_range(conn):
    scenarios = [
        Scenario(
            name="no end",
            args=[4],
            expected=[0, 1, 2, 3],
            result_as_list=True,
        ),
        Scenario(
            name="start 0 end 5",
            args=[0, 5],
            expected=[0, 1, 2, 3, 4],
            result_as_list=True,
        ),
        Scenario(
            name="start -5 end 5",
            args=[-5, 5],
            expected=[-5, -4, -3, -2, -1, 0, 1, 2, 3, 4],
            result_as_list=True,
        ),
    ]

    assert_test_table(query.range, conn, scenarios)


@pytest.mark.integration
def test_reduce(conn):
    def reduce(left, right):
        return left.add(right)

    scenarios = [
        Scenario(
            name="reduce int",
            args=[[0, 1, 2], reduce],
            expected=3,
        ),
    ]

    assert_test_table(query.reduce, conn, scenarios)


@pytest.mark.integration
def test_round(conn):
    scenarios = [
        Scenario(name="round positive down", args=[12.345], expected=12.0),
        Scenario(name="round positive up", args=[12.5], expected=13.0),
        Scenario(name="round negative down", args=[-12.5], expected=-13.0),
        Scenario(name="round negative up", args=[-12.345], expected=-12.0),
    ]

    assert_test_table(query.round, conn, scenarios)


@pytest.mark.integration
def test_row(conn):
    admin = list(
        query.db("rethinkdb")
        .table("users")
        .filter(query.row["id"] == "admin")
        .run(conn)
    )

    nobody = list(
        query.db("rethinkdb").table("users").filter(query.row["id"] == "").run(conn)
    )

    assert admin == [{"id": "admin", "password": False}]
    assert nobody == []


@pytest.mark.integration
def test_sub(conn):
    scenarios = [
        Scenario(name="two ints", args=[query.args([2, 2])], expected=0),
        Scenario(name="two floats", args=[query.args([2.5, 2.0])], expected=0.5),
        Scenario(name="int and float", args=[query.args([2, 2.5])], expected=-0.5),
    ]

    assert_test_table(query.sub, conn, scenarios)


@pytest.mark.integration
def test_sum(conn):
    scenarios = [
        Scenario(name="sum positive", expr_args=[[1, 2, 3]], expected=6),
        Scenario(name="sum negative", expr_args=[[-1, -2, -3]], expected=-6),
        Scenario(name="sum zero", expr_args=[[0, 0, 0]], expected=0),
        Scenario(name="sum mixed", expr_args=[[-1, -3, 1]], expected=-3),
    ]

    assert_test_table(query.sum, conn, scenarios)


@pytest.mark.integration
def test_table_create(conn):
    table = "test_table_create"
    scenarios = [
        Scenario(
            name="create table",
            args=[table],
            expected=1,
            expected_field="tables_created",
            callback=lambda: drop_table("test", table, conn),
        ),
    ]

    assert_test_table(query.table_create, conn, scenarios)


@pytest.mark.integration
def test_table_drop(conn):
    table = "test_table_drop"
    create_table("test", table, conn)

    scenarios = [
        Scenario(
            name="drop table",
            args=[table],
            expected=1,
            expected_field="tables_dropped",
        ),
    ]

    assert_test_table(query.table_drop, conn, scenarios)


@pytest.mark.integration
def test_table_list(conn):
    tables = query.db("rethinkdb").table_list().run(conn)
    assert tables == [
        "cluster_config",
        "current_issues",
        "db_config",
        "jobs",
        "logs",
        "permissions",
        "server_config",
        "server_status",
        "stats",
        "table_config",
        "table_status",
        "users",
    ]


@pytest.mark.integration
def test_time(conn):
    scenarios = [
        Scenario(
            name="time origin",
            args=[1970, 1, 1, "Z"],
            expected="1970-01-01 00:00:00+00:00",
            result_as_string=True,
        ),
        Scenario(
            name="time +1",
            args=[1970, 1, 1, "+01:00"],
            expected="1970-01-01 00:00:00+01:00",
            result_as_string=True,
        ),
        Scenario(
            name="time -1",
            args=[1970, 1, 1, "-01:00"],
            expected="1970-01-01 00:00:00-01:00",
            result_as_string=True,
        ),
    ]

    assert_test_table(query.time, conn, scenarios)


@pytest.mark.integration
def test_type_of(conn):
    # Not testing all types, but it is good enough to test the function
    scenarios = [
        Scenario(name="ARRAY", args=[[1, 2, 3]], expected="ARRAY"),
        Scenario(name="BOOL", args=[True], expected="BOOL"),
        Scenario(name="FUNCTION", args=[lambda x: x], expected="FUNCTION"),
        Scenario(name="NULL", args=[None], expected="NULL"),
        Scenario(name="NUMBER", args=[1], expected="NUMBER"),
        Scenario(name="OBJECT", args=[{"foo": "bar"}], expected="OBJECT"),
        Scenario(name="STRING", args=["foo"], expected="STRING"),
    ]

    assert_test_table(query.type_of, conn, scenarios)


@pytest.mark.integration
def test_union(conn):
    scenarios = [
        Scenario(
            name="array",
            args=[[3, 4], [5, 6], [7, 8, 9]],
            expr_args=[[1, 2]],
            expected=[1, 2, 3, 4, 5, 6, 7, 8, 9],
        ),
    ]

    assert_test_table(query.union, conn, scenarios)


@pytest.mark.integration
def test_uuid(conn):
    result = UUID(query.uuid().run(conn))
    assert result.version == 4


@pytest.mark.integration
def test_js(conn):
    scenarios = [
        Scenario(name="str", args=["`hello` + ` ` + `world`"], expected="hello world"),
        Scenario(name="int", args=["1 + 2"], expected=3),
        Scenario(name="array", args=["[1, 2, 3]"], expected=[1, 2, 3]),
    ]

    assert_test_table(query.js, conn, scenarios)
