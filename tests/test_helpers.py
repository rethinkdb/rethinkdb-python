import pytest
from mock import Mock
from rethinkdb.helpers import decode_utf8

@pytest.mark.unit
class TestDecodeUTF8Helper(object):
    def test_python2_decode_string(self):
        string = Mock(spec=str)

        decoded_string = decode_utf8(string)

        string.decode.assert_called_once_with('utf-8')

    def test_python3_decode_string(self):
        string = Mock(spec=str)
        delattr(string, 'decode')

        decoded_string = decode_utf8(string)

        assert decoded_string == string

