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
#
# This file incorporates work covered by the following copyright:
# Copyright 2010-2016 RethinkDB, all rights reserved.

"""
RethinkDB client drivers are responsible for serializing queries, sending them to the server
using the Reql wire protocol, and receiving responses from the server and returning them to
the calling application.

This module contains the supported handshakes which can be used to establish a new connection.
"""

from abc import abstractmethod
import base64
from enum import Enum
import hashlib
import hmac
import json
from random import SystemRandom
import struct
from typing import Dict, Optional

from rethinkdb import ql2_pb2
from rethinkdb.errors import InvalidHandshakeStateError, ReqlAuthError, ReqlDriverError
from rethinkdb.utilities import chain_to_bytes


class HandshakeState(Enum):
    """
    Represents the state of the handshake.
    """

    INITIAL_CONNECTION = 0
    INITIAL_RESPONSE = 1
    AUTH_REQUEST = 2
    AUTH_RESPONSE = 3


class BaseHandshake:
    """
    :class:`BaseHandshake` is responsible for keeping the common functionality together, what
    handshake versions can reuse later.
    """

    STATE_TRANSITIONS: Dict[int, HandshakeState] = {
        0: HandshakeState.INITIAL_CONNECTION,
        1: HandshakeState.INITIAL_RESPONSE,
    }

    def __init__(self, host: str, port: int):
        super().__init__()
        self.host: str = host
        self.port: int = port
        self.state: HandshakeState = HandshakeState.INITIAL_CONNECTION

    @property
    @abstractmethod
    def version(self) -> int:
        """
        Return the version number of the handshake.
        """

    @property
    @abstractmethod
    def protocol(self) -> int:
        """
        Return the protocol of the handshake.
        """

    @property
    @abstractmethod
    def protocol_version(self) -> int:
        """
        Return the version of the protocol.
        """

    def is_valid_state(self, state: HandshakeState) -> bool:
        """
        Validate that the state is registered for the
        """

        return state in self.STATE_TRANSITIONS.values()

    def next_state(self) -> None:
        """
        Move to the next handshake state.
        """

        current_key = list(self.STATE_TRANSITIONS.values()).index(self.state)
        next_state_key = current_key + 1

        try:
            self.state = self.STATE_TRANSITIONS[next_state_key]
        except KeyError as exc:
            raise InvalidHandshakeStateError(
                f'No state assigned to "{next_state_key}"'
            ) from exc


class HandshakeV1_0(BaseHandshake):  # pylint: disable=invalid-name
    """
    The client sends the protocol version, authentication method, and authentication as a
    null-terminatedJSON response. RethinkDB currently supports only one authentication method,
    SCRAM-SHA-256, as specified in IETF RFC 7677 and RFC 5802. The RFC is followed with the
    exception of error handling (RethinkDB uses its own higher level error reporting rather than
    the e= field). RethinkDB does not support channel binding and clients should not request this.
    The value of "authentication" is the "client-first-message" specified in RFC 5802 (the channel
    binding flag, optional SASL authorization identity, username (n=), and random nonce (r=).

    More info: https://rethinkdb.com/docs/writing-drivers/
    """

    STATE_TRANSITIONS: Dict[int, HandshakeState] = {
        0: HandshakeState.INITIAL_CONNECTION,
        1: HandshakeState.INITIAL_RESPONSE,
        2: HandshakeState.AUTH_REQUEST,
        3: HandshakeState.AUTH_RESPONSE,
    }

    def __init__(self, host: str, port: int, username: str, password: str, **kwargs):
        super().__init__(host, port)

        self.__username = username.replace("=", "=3D").replace(",", "=2C")
        self.__password = password

        self._random_nonce = bytes()
        self._first_client_message = bytes()
        self._server_signature = bytes()

        self.json_encoder = kwargs.pop("json_encoder", json.JSONEncoder)()
        self.json_decoder = kwargs.pop("json_decoder", json.JSONDecoder)()

    @property
    def version(self) -> int:
        return ql2_pb2.VersionDummy.Version.V1_0

    @property
    def protocol(self) -> int:
        return ql2_pb2.VersionDummy.Protocol.JSON

    @property
    def protocol_version(self) -> int:
        return 0

    @staticmethod
    def __get_authentication_message(response: Dict[str, str]) -> Dict[bytes, bytes]:
        """
        Get the first client message and the authentication related data from the
        response provided by RethinkDB.
        """

        message: Dict[bytes, bytes] = {}

        for auth in response["authentication"].encode("ascii").split(b","):
            key, value = auth.split(b"=", 1)
            message[key] = value

        return message

    def __decode_json_response(self, response: str) -> Dict[str, str]:
        """
        Get decoded json response from response.

        :raises: ReqlDriverError | ReqlAuthError
        """

        json_response: Dict[str, str] = self.json_decoder.decode(response)

        if not json_response.get("success"):
            if 10 <= int(json_response["error_code"]) <= 20:
                raise ReqlAuthError(json_response["error"], self.host, self.port)

            raise ReqlDriverError(json_response["error"])

        return json_response

    def __initialize_connection(self) -> bytes:
        """
        Prepare initial connection message. We send the version as well as the initial
        JSON as an optimization.
        """

        self._random_nonce = base64.b64encode(
            bytes(bytearray(SystemRandom().getrandbits(8) for i in range(18)))
        )

        self._first_client_message = chain_to_bytes(
            "n=", self.__username, ",r=", self._random_nonce
        )

        initial_message: bytes = chain_to_bytes(
            struct.pack("<L", self.version),
            self.json_encoder.encode(
                {
                    "protocol_version": self.protocol_version,
                    "authentication_method": "SCRAM-SHA-256",
                    "authentication": chain_to_bytes(
                        "n,,", self._first_client_message
                    ).decode("ascii"),
                }
            ).encode("utf-8"),
            b"\0",
        )

        return initial_message

    def __read_response(self, response: str) -> None:
        """
        Read response of the server. Due to we've already sent the initial JSON, and only support
        a single protocol version at the moment thus we simply read the next response and return an
        empty string as a message.

        :raises: ReqlDriverError | ReqlAuthError
        """

        json_response: Dict[str, str] = self.__decode_json_response(response)
        min_protocol_version: int = int(json_response["min_protocol_version"])
        max_protocol_version: int = int(json_response["max_protocol_version"])

        if not min_protocol_version <= self.protocol_version <= max_protocol_version:
            raise ReqlDriverError(
                f"Unsupported protocol version {self.protocol_version}, expected between "
                f"{min_protocol_version} and {max_protocol_version}"
            )

    def __prepare_auth_request(self, response: str) -> bytes:
        """
        Put tohether the authentication request based on the response of the database.

        :raises: ReqlDriverError | ReqlAuthError
        """

        json_response: Dict[str, str] = self.__decode_json_response(response)
        first_client_message = json_response["authentication"].encode("ascii")
        authentication = self.__get_authentication_message(json_response)

        random_nonce: bytes = authentication[b"r"]

        if not random_nonce.startswith(self._random_nonce):
            raise ReqlAuthError("Invalid nonce from server", self.host, self.port)

        salted_password: bytes = hashlib.pbkdf2_hmac(
            "sha256",
            bytes(self.__password.encode("utf-8")),
            base64.standard_b64decode(authentication[b"s"]),
            int(authentication[b"i"]),
        )

        message_without_proof: bytes = chain_to_bytes("c=biws,r=", random_nonce)
        auth_message: bytes = b",".join(
            (self._first_client_message, first_client_message, message_without_proof)
        )

        self._server_signature = hmac.new(
            hmac.new(salted_password, b"Server Key", hashlib.sha256).digest(),
            auth_message,
            hashlib.sha256,
        ).digest()

        client_key: bytes = hmac.new(
            salted_password, b"Client Key", hashlib.sha256
        ).digest()

        client_signature: bytes = hmac.new(
            hashlib.sha256(client_key).digest(), auth_message, hashlib.sha256
        ).digest()

        client_proof: bytes = struct.pack(
            "32B",
            *(
                left ^ random_nonce
                for left, random_nonce in zip(
                    struct.unpack("32B", client_key),
                    struct.unpack("32B", client_signature),
                )
            ),
        )

        authentication_request: bytes = chain_to_bytes(
            self.json_encoder.encode(
                {
                    "authentication": chain_to_bytes(
                        message_without_proof,
                        ",p=",
                        base64.standard_b64encode(client_proof),
                    ).decode("ascii")
                }
            ),
            b"\0",
        )

        return authentication_request

    def __read_auth_response(self, response: str) -> None:
        """
        Read the authentication request's response sent by the database
        and validate the server signature which was returned.

        :raises: ReqlDriverError | ReqlAuthError
        """

        json_response: Dict[str, str] = self.__decode_json_response(response)
        authentication = self.__get_authentication_message(json_response)
        signature: bytes = base64.standard_b64decode(authentication[b"v"])

        if not hmac.compare_digest(signature, self._server_signature):
            raise ReqlAuthError("Invalid server signature", self.host, self.port)

    def reset(self):
        """
        Reset the handshake to its initial state.
        """

        self._random_nonce = None
        self._first_client_message = None
        self._server_signature = None
        self.state = HandshakeState.INITIAL_CONNECTION

    def next_message(self, raw_response: Optional[bytes]) -> Optional[bytes]:
        """
        Handle the next message to send or receive.
        """

        response: str = ""
        message: Optional[bytes] = None

        if raw_response is not None:
            response = raw_response.decode("utf-8")

        if not self.is_valid_state(self.state):
            raise InvalidHandshakeStateError("Unexpected handshake state")

        if self.state == HandshakeState.INITIAL_CONNECTION:
            if raw_response is not None:
                raise ReqlDriverError("Unexpected response")

            message = self.__initialize_connection()
            self.next_state()

        elif self.state == HandshakeState.INITIAL_RESPONSE:
            self.__read_response(response)
            self.next_state()
            # return empty bytes as the networking handler need to differenciate
            # between empty strings and None
            message = bytes()

        elif self.state == HandshakeState.AUTH_REQUEST:
            message = self.__prepare_auth_request(response)
            self.next_state()

        elif self.state == HandshakeState.AUTH_RESPONSE:
            # No next state, hence no state change required
            self.__read_auth_response(response)
            # Returning None as the message means the end of the handshake
            message = None

        return message
