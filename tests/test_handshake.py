import base64
import json
import pytest
import six
import struct
from mock import call, patch, ANY, Mock
from rethinkdb.errors import ReqlDriverError, ReqlAuthError
from rethinkdb.handshake import HandshakeV1_0, LocalThreadCache
from rethinkdb.helpers import chain_to_bytes
from rethinkdb.ql2_pb2 import VersionDummy


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
        expected_response = {'success': True}

        decoded_response = self.handshake._decode_json_response(json.dumps(expected_response))

        assert decoded_response == expected_response

    def test_decode_json_response_utf8_encoded(self):
        expected_response = {'success': True}

        decoded_response = self.handshake._decode_json_response(json.dumps(expected_response), True)

        assert decoded_response == expected_response

    def test_decode_json_response_auth_error(self):
        expected_response = {'success': False, 'error_code': 15, 'error': 'test error message'}

        with pytest.raises(ReqlAuthError):
            decoded_response = self.handshake._decode_json_response(json.dumps(expected_response))

    def test_decode_json_response_driver_error(self):
        expected_response = {'success': False, 'error_code': 30, 'error': 'test error message'}

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
        mock_base64.standard_b64encode.return_value = encoded_string
        first_client_message = chain_to_bytes('n=', self.handshake._username, ',r=', encoded_string)

        expected_result = chain_to_bytes(
            struct.pack('<L', self.handshake.VERSION),
            self.handshake._json_encoder.encode({
                'protocol_version': self.handshake._protocol_version,
                'authentication_method': 'SCRAM-SHA-256',
                'authentication': chain_to_bytes('n,,', first_client_message).decode('ascii')
            }).encode('utf-8'),
            b'\0'
        )

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
        response = {'success': True, 'min_protocol_version': 0, 'max_protocol_version': 1}

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
        response = {'success': True, 'min_protocol_version': -1, 'max_protocol_version': -1}

        with pytest.raises(ReqlDriverError):
            result = self.handshake._read_response(json.dumps(response))

        assert self.handshake._next_state.called is False

    def test_prepare_auth_request(self):
        self.handshake._next_state = Mock()
        self.handshake._random_nonce = base64.encodebytes(b'random_nonce') if six.PY3 else base64.b64encode(b'random_nonce')
        self.handshake._first_client_message = chain_to_bytes('n=', self.handshake._username, ',r=', self.handshake._random_nonce)
        response = {'success': True, 'authentication': 's=cmFuZG9tX25vbmNl\n,i=2,r=cmFuZG9tX25vbmNl\n'}
        expected_result = b'{"authentication": "c=biws,r=cmFuZG9tX25vbmNl\\n,p=2Tpd60LM4Tkhe7VATTPj/lh4yunl07Sm4A+m3ukC774="}\x00'

        result = self.handshake._prepare_auth_request(json.dumps(response))

        assert isinstance(result, six.binary_type)
        assert result == expected_result
        assert self.handshake._next_state.called is True

    def test_prepare_auth_request_invalid_nonce(self):
        self.handshake._next_state = Mock()
        self.handshake._random_nonce = base64.encodebytes(b'invalid') if six.PY3 else base64.b64encode(b'invalid')
        response = {'success': True, 'authentication': 's=fake,i=2,r=cmFuZG9tX25vbmNl\n'}

        with pytest.raises(ReqlAuthError):
            result = self.handshake._prepare_auth_request(json.dumps(response))

        assert self.handshake._next_state.called is False

    def test_read_auth_response(self):
        self.handshake._next_state = Mock()
        self.handshake._server_signature = b'signature'
        response = {'success': True, 'authentication': 'v=c2lnbmF0dXJl\n'}

        result = self.handshake._read_auth_response(json.dumps(response))

        assert result is None
        assert self.handshake._next_state.called is True

    def test_read_auth_response_invalid_server_signature(self):
        self.handshake._next_state = Mock()
        self.handshake._server_signature = b'invalid-signature'
        response = {'success': True, 'authentication': 'v=c2lnbmF0dXJl\n'}

        with pytest.raises(ReqlAuthError):
            result = self.handshake._read_auth_response(json.dumps(response))

        assert self.handshake._next_state.called is False
