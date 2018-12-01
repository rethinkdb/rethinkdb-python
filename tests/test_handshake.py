import pytest
import struct
import json
from mock import call, patch, ANY, Mock
from rethinkdb.errors import ReqlDriverError, ReqlAuthError
from rethinkdb.ql2_pb2 import VersionDummy
from rethinkdb.handshake import HandshakeV1_0, LocalThreadCache
from rethinkdb.helpers import to_bytes


@pytest.mark.unit
class TestLocalThreadCache(object):
    def setup_method(self):
        self.cache = LocalThreadCache()
        self.cache_key = 'test'
        self.cache_value = 'cache'

    def test_initialization(self):
         assert self.cache._cache == dict()

    def test_add_to_cache(self):
        self.cache.set(self.cache_key, self.cache_value)

        assert self.cache._cache == {self.cache_key: self.cache_value}

    def test_get_from_cache(self):
        self.cache._cache = {self.cache_key: self.cache_value}

        cached_value = self.cache.get(self.cache_key)

        assert cached_value == self.cache_value

@pytest.mark.unit
class TestHandshake(object):
    def setup_method(self):
        self.encoder = json.JSONEncoder()
        self.decoder = json.JSONDecoder()

        self.handshake = self._get_handshake()

    def _get_handshake(self):
        return HandshakeV1_0(
            json_encoder=self.encoder,
            json_decoder=self.decoder,
            host='localhost',
            port=28015,
            username='admin',
            password=''
        )

    @patch('rethinkdb.handshake.HandshakeV1_0._get_pbkdf2_hmac')
    @patch('rethinkdb.handshake.HandshakeV1_0._get_compare_digest')
    def test_initialization(self, mock_get_compare_digest, mock_get_pbkdf2_hmac):
        handshake = self._get_handshake()

        assert handshake.VERSION == VersionDummy.Version.V1_0
        assert handshake.PROTOCOL == VersionDummy.Protocol.JSON
        assert mock_get_compare_digest.called is True
        assert mock_get_pbkdf2_hmac.called is True

    @patch('rethinkdb.handshake.hmac')
    def test_get_builtin_compare_digest(self, mock_hmac):
        mock_hmac.compare_digest = Mock
        handshake = self._get_handshake()

        assert handshake._compare_digest == mock_hmac.compare_digest

    @patch('rethinkdb.handshake.compare_digest')
    @patch('rethinkdb.handshake.hmac')
    def test_get_own_compare_digest(self, mock_hmac, mock_compare_digest):
        delattr(mock_hmac, 'compare_digest')
        handshake = self._get_handshake()

        assert handshake._compare_digest == mock_compare_digest

    @patch('rethinkdb.handshake.hashlib')
    def test_get_builtin_get_pbkdf2_hmac(self, mock_hashlib):
        mock_hashlib.pbkdf2_hmac = Mock
        handshake = self._get_handshake()

        assert handshake._pbkdf2_hmac == mock_hashlib.pbkdf2_hmac

    @patch('rethinkdb.handshake.pbkdf2_hmac')
    @patch('rethinkdb.handshake.hashlib')
    def test_get_own_get_pbkdf2_hmac(self, mock_hashlib, mock_pbkdf2_hmac):
        delattr(mock_hashlib, 'pbkdf2_hmac')
        handshake = self._get_handshake()

        assert handshake._pbkdf2_hmac == mock_pbkdf2_hmac

    def test_decode_json_response(self):
        expected_response = {"success": True}

        decoded_response = self.handshake._decode_json_response(json.dumps(expected_response))

        assert decoded_response == expected_response

    def test_decode_json_response_utf8_encoded(self):
        expected_response = {"success": True}

        decoded_response = self.handshake._decode_json_response(json.dumps(expected_response), True)

        assert decoded_response == expected_response

    def test_decode_json_response_auth_error(self):
        expected_response = {"success": False, "error_code": 15, "error": "test error message"}

        with pytest.raises(ReqlAuthError):
            decoded_response = self.handshake._decode_json_response(json.dumps(expected_response))

    def test_decode_json_response_driver_error(self):
        expected_response = {"success": False, "error_code": 30, "error": "test error message"}

        with pytest.raises(ReqlDriverError):
            decoded_response = self.handshake._decode_json_response(json.dumps(expected_response))

    def test_next_state(self):
        previous_state = self.handshake._state
        self.handshake._next_state()
        new_state = self.handshake._state

        assert previous_state == 0
        assert new_state == 1

    def test_reset(self):
        self.handshake._random_nonce = Mock()
        self.handshake._first_client_message = Mock()
        self.handshake._server_signature = Mock()
        self.handshake._state = Mock()

        self.handshake.reset()

        assert self.handshake._random_nonce is None
        assert self.handshake._first_client_message is None
        assert self.handshake._server_signature is None
        assert self.handshake._state == 0

    @patch('rethinkdb.handshake.base64')
    def test_init_connection(self, mock_base64):
        self.handshake._next_state = Mock()
        encoded_string = 'test'
        pack = struct.pack('<L', self.handshake.VERSION)
        mock_base64.standard_b64encode.return_value = encoded_string
        first_client_message = to_bytes('n={username},r={r}'.format(username=self.handshake._username, r=encoded_string))
        message = self.handshake._json_encoder.encode({
            'protocol_version': self.handshake._protocol_version,
            'authentication_method': 'SCRAM-SHA-256',
            'authentication': to_bytes('n,,{client_message}'.format(client_message=first_client_message).decode("ascii"))
        })
        expected_result = to_bytes('{pack}{message}\0'.format(pack=pack, message=message))

        result = self.handshake._init_connection(response=None)

        assert result == expected_result
        assert self.handshake._next_state.called is True

    def test_init_connection_unexpected_response(self):
        self.handshake._next_state = Mock()

        with pytest.raises(ReqlDriverError):
            result = self.handshake._init_connection(response=Mock())

        assert self.handshake._next_state.called is False

    def test_read_response(self):
        self.handshake._next_state = Mock()
        response = {"success": True, "min_protocol_version": 0, "max_protocol_version": 1}

        result = self.handshake._read_response(json.dumps(response))

        assert result == ''
        assert self.handshake._next_state.called is True

    def test_read_response_error_received(self):
        self.handshake._next_state = Mock()

        with pytest.raises(ValueError):
            result = self.handshake._read_response('ERROR')

        assert self.handshake._next_state.called is False

    def test_read_response_protocol_mismatch(self):
        self.handshake._next_state = Mock()
        response = {"success": True, "min_protocol_version": -1, "max_protocol_version": -1}

        with pytest.raises(ReqlDriverError):
            result = self.handshake._read_response(json.dumps(response))

        assert self.handshake._next_state.called is False
