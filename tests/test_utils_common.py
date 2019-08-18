import pytest
from rethinkdb import utils_common


@pytest.fixture
def parser():
    opt_parser = utils_common.CommonOptionsParser()
    opt_parser.add_option(
        "-e",
        "--export",
        dest="db_tables",
        metavar="DB|DB.TABLE",
        default=[],
        type='db_table',
        action="append")
    opt_parser.add_option(
        "--clients",
        dest="clients",
        metavar="NUM",
        default=3,
        type="pos_int")
    return opt_parser


def test_option_parser_int_pos(parser):
    options, args = parser.parse_args(['--clients', '4'], connect=False)

    assert options.clients == 4


def test_option_parser_int_pos_equals(parser):
    options, args = parser.parse_args(['--clients=4'], connect=False)

    assert options.clients == 4


def test_option_parser_int_pos_default(parser):
    options, args = parser.parse_args([], connect=False)

    assert options.clients == 3


def test_option_parser_int_pos_fail(parser):
    with pytest.raises(SystemExit):
        parser.parse_args(['--clients=asdf'], connect=False)


def test_option_parser_int_pos_zero(parser):
    with pytest.raises(SystemExit):
        parser.parse_args(['--clients=0'], connect=False)


def test_option_parser_db_table(parser):
    options, args = parser.parse_args(['--export=example.table'], connect=False)

    assert options.db_tables == [('example', 'table')]


def test_option_parser_db_table_append(parser):
    options, args = parser.parse_args(['--export=example.table', '--export=example.another'], connect=False)

    assert options.db_tables == [('example', 'table'), ('example', 'another')]


def test_option_parser_db_table_only_db(parser):
    options, args = parser.parse_args(['--export=example'], connect=False)

    assert options.db_tables == [('example', None)]


def test_option_parser_db_table_fail(parser):
    with pytest.raises(SystemExit):
        parser.parse_args(['--export='], connect=False)
