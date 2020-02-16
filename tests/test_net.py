import pytest
from mock import Mock, ANY
from rethinkdb.net import make_connection, DefaultConnection, DEFAULT_PORT


@pytest.mark.unit
class TestMakeConnection(object):
    def setup_method(self):
        self.reconnect = Mock()
        self.conn_type = Mock()
        self.conn_type.return_value.reconnect.return_value = self.reconnect

        self.host = "myhost"
        self.port = "1234"
        self.db = "mydb"
        self.auth_key = None
        self.user = "gabor"
        self.password = "strongpass"
        self.timeout = 20


    def test_make_connection(self):
        ssl = dict()
        _handshake_version = 10

        conn = make_connection(
            self.conn_type,
            host=self.host,
            port=self.port,
            db=self.db,
            auth_key=self.auth_key,
            user=self.user,
            password=self.password,
            timeout=self.timeout,
        )

        assert conn == self.reconnect
        self.conn_type.assert_called_once_with(
            self.host,
            self.port,
            self.db,
            self.auth_key,
            self.user,
            self.password,
            self.timeout,
            ssl,
            _handshake_version
        )


    def test_make_connection_db_url(self):
        url = "rethinkdb://gabor:strongpass@myhost:1234/mydb?auth_key=mykey&timeout=30"
        ssl = dict()
        _handshake_version = 10

        conn = make_connection(self.conn_type, url=url)

        assert conn == self.reconnect
        self.conn_type.assert_called_once_with(
            self.host,
            self.port,
            self.db,
            "mykey",
            self.user,
            self.password,
            30,
            ssl,
            _handshake_version
        )


    def test_make_connection_no_host(self):
        conn = make_connection(
            self.conn_type,
            port=self.port,
            db=self.db,
            auth_key=self.auth_key,
            user=self.user,
            password=self.password,
            timeout=self.timeout,
        )

        assert conn == self.reconnect
        self.conn_type.assert_called_once_with(
            "localhost",
            self.port,
            self.db,
            self.auth_key,
            self.user,
            self.password,
            self.timeout,
            ANY,
            ANY
        )


    def test_make_connection_no_port(self):
        conn = make_connection(
            self.conn_type,
            host=self.host,
            db=self.db,
            auth_key=self.auth_key,
            user=self.user,
            password=self.password,
            timeout=self.timeout,
        )

        assert conn == self.reconnect
        self.conn_type.assert_called_once_with(
            self.host,
            DEFAULT_PORT,
            self.db,
            self.auth_key,
            self.user,
            self.password,
            self.timeout,
            ANY,
            ANY
        )


    def test_make_connection_no_user(self):
        conn = make_connection(
            self.conn_type,
            host=self.host,
            port=self.port,
            db=self.db,
            auth_key=self.auth_key,
            password=self.password,
            timeout=self.timeout,
        )

        assert conn == self.reconnect
        self.conn_type.assert_called_once_with(
            self.host,
            self.port,
            self.db,
            self.auth_key,
            "admin",
            self.password,
            self.timeout,
            ANY,
            ANY
        )


    def test_make_connection_with_ssl(self):
        ssl = dict()

        conn = make_connection(
            self.conn_type,
            host=self.host,
            port=self.port,
            db=self.db,
            auth_key=self.auth_key,
            user=self.user,
            password=self.password,
            timeout=self.timeout,
            ssl=ssl,
        )

        assert conn == self.reconnect
        self.conn_type.assert_called_once_with(
            self.host,
            self.port,
            self.db,
            self.auth_key,
            self.user,
            self.password,
            self.timeout,
            ssl,
            ANY
        )


    def test_make_connection_different_handshake_version(self):
        conn = make_connection(
            self.conn_type,
            host=self.host,
            port=self.port,
            db=self.db,
            auth_key=self.auth_key,
            user=self.user,
            password=self.password,
            timeout=self.timeout,
            _handshake_version=20,
        )

        assert conn == self.reconnect
        self.conn_type.assert_called_once_with(
            self.host,
            self.port,
            self.db,
            self.auth_key,
            self.user,
            self.password,
            self.timeout,
            ANY,
            20
        )
