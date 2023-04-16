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

import pytest

from rethinkdb.ast import ReqlQuery
from rethinkdb.encoder import ReqlDecoder, ReqlEncoder


class UnknownObj:
    """"""


def test_encode():
    """
    Test encoding of standard Python objects.
    """

    obj = {"strkey": "value", "intkey": 1, "dictkey": {"strkey": "value"}}

    encoder = ReqlEncoder()
    result = encoder.encode(obj)

    assert result == '{"strkey":"value","intkey":1,"dictkey":{"strkey":"value"}}'


def test_encode_Reql_query():
    """
    Test encoding ReqlQuery.
    """

    query = ReqlQuery(1, 2, optargs={"key": "val"})

    encoder = ReqlEncoder()
    result = encoder.encode(query)

    assert result == '[null,[1,2],{"optargs":{"key":"val"}}]'


def test_encode_unknown_object():
    """
    Test encoding objects which are unknown by the encoder.
    """

    encoder = ReqlEncoder()

    with pytest.raises(TypeError):
        encoder.encode(UnknownObj())


def test_decode():
    """
    Test decoding strings to standard Python objects.
    """

    string = '{"strkey":"value","intkey":1,"dictkey":{"strkey":"value"}}'

    decoder = ReqlDecoder()
    result = decoder.decode(string)

    assert result == {"strkey": "value", "intkey": 1, "dictkey": {"strkey": "value"}}


def test_decode_Reql_query():
    """
    Test decoding ReqlQuery.
    """

    query = '[null,[1,2],{"optargs":{"key":"val"}}]'

    decoder = ReqlDecoder()
    result = decoder.decode(query)

    assert result == ReqlQuery(1, 2, optargs={"key": "val"})


def test_decode_unknown_object():
    """
    Test encoding objects which are unknown by the encoder.
    """

    decoder = ReqlEncoder()

    with pytest.raises(TypeError):
        decoder.encode(UnknownObj())
