import pytest
from mock import Mock

from rethinkdb.helpers import chain_to_bytes, decode_utf8


@pytest.mark.unit
class TestDecodeUTF8Helper(object):
    def test_python2_decode_string(self):
        string = Mock()

        decoded_string = decode_utf8(string)

        string.decode.assert_called_once_with("utf-8")

    def test_python3_decode_string(self):
        string = Mock(spec=str)
        delattr(string, "decode")

        decoded_string = decode_utf8(string)

        assert decoded_string == string


@pytest.mark.unit
class TestChainToBytesHelper(object):
    def test_string_chaining(self):
        expected_string = b"iron man"

        result = chain_to_bytes("iron", " ", "man")

        assert result == expected_string

    def test_byte_chaining(self):
        expected_string = b"iron man"

        result = chain_to_bytes(b"iron", b" ", b"man")

        assert result == expected_string

    def test_mixed_chaining(self):
        expected_string = b"iron man"

        result = chain_to_bytes("iron", " ", b"man")

        assert result == expected_string
