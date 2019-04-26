import pytest
from mock import call, patch, ANY, Mock
from rethinkdb import r, ast


@pytest.mark.unit
class TestNow(object):
    def setup_method(self):
        pass

    def test_get_now(self):
        now = r.now()
        assert type(now) == ast.Now
