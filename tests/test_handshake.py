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

# pylint: disable=redefined-outer-name

import base64
import json
import struct
from unittest.mock import Mock, patch

import pytest

from rethinkdb.errors import InvalidHandshakeStateError, ReqlAuthError, ReqlDriverError
from rethinkdb.handshake import BaseHandshake, HandshakeState, HandshakeV1_0
from rethinkdb.ql2_pb2 import VersionDummy
from rethinkdb.utilities import chain_to_bytes


@pytest.fixture
def base_handshake():
    """
    Fixture returning a BaseHandshake object for testing.
    """

    return BaseHandshake(
        host="localhost",
        port=28015,
    )


@pytest.fixture
def handshake():
    """
    Fixture returning a Handshake object for testing.
    """

    return HandshakeV1_0(
        host="localhost",
        port=28015,
        username="admin",
        password="",
    )


def test_handshake_stage_stepping(base_handshake):
    """
    Test that the handshake states can be incremented.
    """

    assert base_handshake.state == HandshakeState.INITIAL_CONNECTION
    base_handshake.next_state()

    assert base_handshake.state == HandshakeState.INITIAL_RESPONSE

    # No more states, raise an error
    with pytest.raises(InvalidHandshakeStateError):
        base_handshake.next_state()


def test_handshake_v1_0_stage_stepping(handshake):
    """
    Test that the handshake states can be incremented.
    """

    assert handshake.state == HandshakeState.INITIAL_CONNECTION
    handshake.next_state()

    assert handshake.state == HandshakeState.INITIAL_RESPONSE
    handshake.next_state()

    assert handshake.state == HandshakeState.AUTH_REQUEST
    handshake.next_state()

    assert handshake.state == HandshakeState.AUTH_RESPONSE

    # No more states, raise an error
    with pytest.raises(InvalidHandshakeStateError):
        handshake.next_state()


def test_handshake_v1_0_initialization(handshake):
    """
    Test handshake object initialization works.
    """

    assert handshake.version == VersionDummy.Version.V1_0
    assert handshake.protocol == VersionDummy.Protocol.JSON
    assert handshake.protocol_version == 0


def test_handshake_v1_0_reset(handshake):
    """
    Test handshake object reset works.
    """

    assert handshake.state == HandshakeState.INITIAL_CONNECTION
    handshake.next_state()

    assert handshake.state == HandshakeState.INITIAL_RESPONSE
    handshake.reset()

    assert handshake.state == HandshakeState.INITIAL_CONNECTION


def test_handshake_v1_0_invalid_state(handshake):
    """
    Test handshake reaches an invalid state.
    """

    handshake.state = Mock()

    with pytest.raises(InvalidHandshakeStateError):
        handshake.next_message(None)


@patch("rethinkdb.handshake.base64")
def test_init_connection(mock_base64, handshake):
    """
    Test connection initialization works.
    """

    handshake.next_state = Mock()
    encoded_string = "test"
    mock_base64.b64encode.return_value = encoded_string

    first_client_message = chain_to_bytes("n=", "admin", ",r=", encoded_string)

    expected_result = chain_to_bytes(
        struct.pack("<L", handshake.version),
        handshake.json_encoder.encode(
            {
                "protocol_version": handshake.protocol_version,
                "authentication_method": "SCRAM-SHA-256",
                "authentication": chain_to_bytes("n,,", first_client_message).decode(
                    "ascii"
                ),
            }
        ).encode("utf-8"),
        "\0",
    )

    result = handshake.next_message(None)

    assert result == expected_result
    assert handshake.next_state.called is True
    assert handshake.state == HandshakeState.INITIAL_CONNECTION


def test_init_connection_unexpected_response(handshake):
    """
    Test unexpected response received from the server. This could by anything
    which is not valid.
    """

    handshake.next_state = Mock()

    with pytest.raises(ReqlDriverError):
        handshake.next_message(b"")

    assert handshake.next_state.called is False
    assert handshake.state == HandshakeState.INITIAL_CONNECTION


def test_read_response(handshake):
    """
    Test reading the initial database connection response.
    """

    handshake.next_state = Mock()
    handshake.state = HandshakeState.INITIAL_RESPONSE

    response = {
        "success": True,
        "min_protocol_version": 0,
        "max_protocol_version": 1,
    }

    result = handshake.next_message(bytes(json.dumps(response), "utf-8"))

    assert result == b""
    assert handshake.next_state.called is True


def test_decode_json_response_auth_error(handshake):
    """
    Test the unsuccessful json response returned by the server is an authentication error.
    """

    handshake.state = HandshakeState.INITIAL_RESPONSE

    expected_response = {
        "success": False,
        "error_code": 15,
        "error": "test error message",
    }

    with pytest.raises(ReqlAuthError):
        handshake.next_message(bytes(json.dumps(expected_response), "utf-8"))


def test_decode_json_response_driver_error(handshake):
    """
    Test the unsuccessful json response returned by the server is not an authentication error.
    """

    handshake.state = HandshakeState.INITIAL_RESPONSE

    expected_response = {
        "success": False,
        "error_code": 30,
        "error": "test error message",
    }

    with pytest.raises(ReqlDriverError):
        handshake.next_message(bytes(json.dumps(expected_response), "utf-8"))


def test_read_response_error_received(handshake):
    """
    Test the response returned by the database is an error.
    """

    handshake.next_state = Mock()
    handshake.state = HandshakeState.INITIAL_RESPONSE

    with pytest.raises(ValueError):
        handshake.next_message(bytes("ERROR", "utf-8"))

    assert handshake.next_state.called is False


def test_read_response_protocol_mismatch(handshake):
    """
    Test the response's min/max protocols has no match with the client's protocol.
    """

    handshake.next_state = Mock()
    handshake.state = HandshakeState.INITIAL_RESPONSE

    response = {
        "success": True,
        "min_protocol_version": -1,
        "max_protocol_version": -1,
    }

    with pytest.raises(ReqlDriverError):
        handshake.next_message(bytes(json.dumps(response), "utf-8"))

    assert handshake.next_state.called is False


def test_prepare_auth_request(handshake):
    """
    Test auth request is prepared.
    """

    handshake.next_state = Mock()
    handshake.state = HandshakeState.AUTH_REQUEST

    handshake._random_nonce = base64.encodebytes(b"random_nonce")
    handshake._first_client_message = chain_to_bytes(
        "n=", "admin", ",r=", handshake._random_nonce
    )

    response = {
        "success": True,
        "authentication": "s=cmFuZG9tX25vbmNl\n,i=2,r=cmFuZG9tX25vbmNl\n",
    }

    expected_result = b'{"authentication": "c=biws,r=cmFuZG9tX25vbmNl\\n,p=2Tpd60LM4Tkhe7VATTPj/lh4yunl07Sm4A+m3ukC774="}\x00'
    result = handshake.next_message(bytes(json.dumps(response), "utf-8"))

    assert result == expected_result
    assert handshake.next_state.called is True


def test_prepare_auth_request_invalid_nonce(handshake):
    """
    Test the returned nonce is invalid.
    """

    handshake.next_state = Mock()
    handshake.state = HandshakeState.AUTH_REQUEST
    handshake._random_nonce = base64.encodebytes(b"invalid")

    response = {
        "success": True,
        "authentication": "s=fake,i=2,r=cmFuZG9tX25vbmNl\n",
    }

    with pytest.raises(ReqlAuthError):
        handshake.next_message(bytes(json.dumps(response), "utf-8"))

    assert handshake.next_state.called is False


def test_read_auth_response(handshake):
    """
    Test parsing the authentication response from the server.
    """

    handshake.next_state = Mock()
    handshake.state = HandshakeState.AUTH_RESPONSE
    handshake._server_signature = bytes("signature".encode("utf-8"))
    response = {"success": True, "authentication": "v=c2lnbmF0dXJl\n"}

    result = handshake.next_message(bytes(json.dumps(response), "utf-8"))

    assert result is None
    assert handshake.next_state.called is False


def test_read_auth_response_invalid_server_signature(handshake):
    """
    Test the authentication response returned by the server is invalid.
    """

    handshake.next_state = Mock()
    handshake.state = HandshakeState.AUTH_RESPONSE
    handshake._server_signature = bytes("invalid-signature".encode("utf-8"))
    response = {"success": True, "authentication": "v=c2lnbmF0dXJl\n"}

    with pytest.raises(ReqlAuthError):
        handshake.next_message(bytes(json.dumps(response), "utf-8"))

    assert handshake.next_state.called is False
