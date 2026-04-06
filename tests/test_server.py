from unittest.mock import MagicMock, patch

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import pytest

from mcp_psycopg2._server import (
    _active_connections,
    _active_cursors,
    begin_transaction,
    cancel_query,
    close_connection,
    close_cursor,
    commit_transaction,
    connect,
    copy_expert,
    copy_from,
    copy_to,
    create_cursor,
    create_large_object,
    create_named_cursor,
    execute_many,
    execute_query,
    fetch_all,
    fetch_many,
    fetch_one,
    get_backend_pid,
    get_connection_info,
    get_dsn_parameters,
    get_notices,
    get_server_version,
    list_connections,
    list_cursors,
    make_dsn,
    mogrify,
    parse_dsn,
    quote_identifier,
    read_large_object,
    register_composite,
    register_hstore,
    register_json,
    rollback_transaction,
    scroll_cursor,
    set_isolation_level,
    set_session,
    write_large_object,
)


@pytest.fixture(autouse=True)
def clear_state():
    """Clear active connections and cursors before each test."""
    _active_connections.clear()
    _active_cursors.clear()
    yield
    _active_connections.clear()
    _active_cursors.clear()


class TestConnect:
    """Tests for connect function."""

    def test_connect_success(self, mock_conn):
        with patch("psycopg2.connect", return_value=mock_conn):
            result = connect(
                dbname="test",
                user="postgres",
                password="secret",
                host="localhost",
                port=5432,
                connection_id="test_conn",
            )
            assert result == "test_conn"
            assert "test_conn" in _active_connections

    def test_connect_failure(self):
        with patch("psycopg2.connect") as mock_connect:
            mock_connect.side_effect = psycopg2.Error("Connection failed")
            with pytest.raises(Exception) as exc_info:
                connect()
            assert "Connection failed" in str(exc_info.value)


class TestCloseConnection:
    """Tests for close_connection function."""

    def test_close_connection_success(self, mock_conn, mock_cursor):
        _active_connections["test_conn"] = mock_conn
        _active_cursors["cursor_test_conn_1"] = mock_cursor
        _active_cursors["cursor_test_conn_2"] = mock_cursor
        result = close_connection("test_conn")
        assert result == "Connection closed"
        assert "test_conn" not in _active_connections
        assert "cursor_test_conn_1" not in _active_cursors
        assert "cursor_test_conn_2" not in _active_cursors

    def test_close_connection_not_found(self):
        with pytest.raises(Exception) as exc_info:
            close_connection("nonexistent")
        assert "not found" in str(exc_info.value)


class TestGetConnectionInfo:
    """Tests for get_connection_info function."""

    def test_get_connection_info(self, mock_conn, mock_info):
        mock_conn.info = mock_info
        _active_connections["test_conn"] = mock_conn
        result = get_connection_info("test_conn")
        assert result["dbname"] == "test"
        assert result["user"] == "postgres"
        assert result["host"] == "localhost"

    def test_get_connection_info_not_found(self):
        with pytest.raises(Exception) as exc_info:
            get_connection_info("nonexistent")
        assert "not found" in str(exc_info.value)


class TestTransaction:
    """Tests for transaction functions."""

    def test_begin_transaction(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        result = begin_transaction("test_conn")
        assert "started" in result.lower()

    def test_begin_transaction_not_found(self):
        with pytest.raises(Exception) as exc_info:
            begin_transaction("nonexistent")
        assert "not found" in str(exc_info.value)

    def test_commit_transaction(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        result = commit_transaction("test_conn")
        assert "committed" in result.lower()
        mock_conn.commit.assert_called_once()

    def test_commit_transaction_not_found(self):
        with pytest.raises(Exception) as exc_info:
            commit_transaction("nonexistent")
        assert "not found" in str(exc_info.value)

    def test_rollback_transaction(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        result = rollback_transaction("test_conn")
        assert "rolled back" in result.lower()
        mock_conn.rollback.assert_called_once()

    def test_rollback_transaction_not_found(self):
        with pytest.raises(Exception) as exc_info:
            rollback_transaction("nonexistent")
        assert "not found" in str(exc_info.value)


class TestSetIsolationLevel:
    """Tests for set_isolation_level function."""

    def test_set_isolation_level_autocommit(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        result = set_isolation_level("AUTOCOMMIT", "test_conn")
        assert "set" in result.lower()
        mock_conn.set_isolation_level.assert_called_once()

    def test_set_isolation_level_read_committed(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        result = set_isolation_level("READ_COMMITTED", "test_conn")
        assert "set" in result.lower()
        mock_conn.set_isolation_level.assert_called_once()

    def test_set_isolation_level_repeatable_read(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        result = set_isolation_level("REPEATABLE_READ", "test_conn")
        assert "set" in result.lower()
        mock_conn.set_isolation_level.assert_called_once()

    def test_set_isolation_level_serializable(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        result = set_isolation_level("SERIALIZABLE", "test_conn")
        assert "set" in result.lower()
        mock_conn.set_isolation_level.assert_called_once()

    def test_set_isolation_level_default(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        result = set_isolation_level("DEFAULT", "test_conn")
        assert "set" in result.lower()
        mock_conn.set_isolation_level.assert_called_once()

    def test_set_isolation_level_invalid(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        with pytest.raises(Exception) as exc_info:
            set_isolation_level("INVALID_LEVEL", "test_conn")
        assert "Invalid" in str(exc_info.value)

    def test_set_isolation_level_not_found(self):
        with pytest.raises(Exception) as exc_info:
            set_isolation_level("READ_COMMITTED", "nonexistent")
        assert "not found" in str(exc_info.value)


class TestCursor:
    """Tests for cursor functions."""

    def test_create_cursor(self, mock_conn, mock_cursor):
        _active_connections["test_conn"] = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        result = create_cursor(connection_id="test_conn")
        assert result.startswith("cursor_")
        assert len(_active_cursors) == 1

    def test_create_cursor_with_name(self, mock_conn, mock_cursor):
        _active_connections["test_conn"] = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        result = create_cursor(cursor_name="my_cursor", connection_id="test_conn")
        assert result.startswith("cursor_")

    def test_close_cursor(self, mock_cursor):
        cursor_id = "test_cursor"
        _active_cursors[cursor_id] = mock_cursor
        result = close_cursor(cursor_id)
        assert "closed" in result.lower()
        assert cursor_id not in _active_cursors

    def test_close_cursor_not_found(self):
        with pytest.raises(Exception) as exc_info:
            close_cursor("nonexistent")
        assert "not found" in str(exc_info.value)

    def test_execute_query_connection_not_found(self):
        with pytest.raises(Exception) as exc_info:
            execute_query("SELECT 1", connection_id="nonexistent")
        assert "not found" in str(exc_info.value)


class TestExecuteQuery:
    """Tests for execute_query function."""

    def test_execute_query_select(self, mock_conn, mock_cursor):
        _active_connections["test_conn"] = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_col = MagicMock()
        mock_col.name = "id"
        mock_col.type_code = 23
        mock_col.display_size = None
        mock_col.internal_size = 4
        mock_col.precision = None
        mock_col.scale = None
        mock_col.null_ok = None
        mock_cursor.description = [mock_col]
        mock_cursor.fetchall.return_value = [(1,), (2,)]

        result = execute_query("SELECT id FROM users", connection_id="test_conn")

        assert result["rowcount"] == 1
        assert len(result["rows"]) == 2
        assert result["columns"][0]["name"] == "id"

    def test_execute_query_insert(self, mock_conn, mock_cursor):
        _active_connections["test_conn"] = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.description = None
        mock_cursor.fetchall.return_value = []

        result = execute_query(
            "INSERT INTO users VALUES (1)", connection_id="test_conn"
        )

        assert "rowcount" in result
        assert result["columns"] == []


class TestExecuteMany:
    """Tests for execute_many function."""

    def test_execute_many(self, mock_conn, mock_cursor):
        _active_connections["test_conn"] = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        result = execute_many(
            "INSERT INTO users (name) VALUES (%s)",
            [["alice"], ["bob"]],
            connection_id="test_conn",
        )

        assert result["rowcount"] >= 0
        mock_cursor.executemany.assert_called_once()


class TestFetch:
    """Tests for fetch functions."""

    def test_fetch_one(self, mock_cursor):
        cursor_id = "test_cursor"
        _active_cursors[cursor_id] = mock_cursor
        mock_cursor.fetchone.return_value = (1, "john")

        result = fetch_one(cursor_id)

        assert result == [1, "john"]

    def test_fetch_one_empty(self, mock_cursor):
        cursor_id = "test_cursor"
        _active_cursors[cursor_id] = mock_cursor
        mock_cursor.fetchone.return_value = None

        result = fetch_one(cursor_id)

        assert result is None

    def test_fetch_one_not_found(self):
        with pytest.raises(Exception) as exc_info:
            fetch_one("nonexistent")
        assert "not found" in str(exc_info.value)

    def test_fetch_many(self, mock_cursor):
        cursor_id = "test_cursor"
        _active_cursors[cursor_id] = mock_cursor
        mock_cursor.fetchmany.return_value = [(1, "john"), (2, "jane")]

        result = fetch_many(cursor_id, size=2)

        assert len(result) == 2

    def test_fetch_many_not_found(self):
        with pytest.raises(Exception) as exc_info:
            fetch_many("nonexistent", 10)
        assert "not found" in str(exc_info.value)

    def test_fetch_all(self, mock_cursor):
        cursor_id = "test_cursor"
        _active_cursors[cursor_id] = mock_cursor
        mock_cursor.fetchall.return_value = [(1, "john"), (2, "jane")]

        result = fetch_all(cursor_id)

        assert len(result) == 2

    def test_fetch_all_not_found(self):
        with pytest.raises(Exception) as exc_info:
            fetch_all("nonexistent")
        assert "not found" in str(exc_info.value)


class TestQuoteIdentifier:
    """Tests for quote_identifier function."""

    def test_quote_identifier(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        with patch("psycopg2.extensions.quote_ident", return_value='"my_table"'):
            result = quote_identifier("my_table", "test_conn")
            assert '"' in result


class TestNamedCursor:
    """Tests for server-side named cursor functions."""

    def test_create_named_cursor(self, mock_conn, mock_cursor):
        _active_connections["test_conn"] = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        result = create_named_cursor(
            name="my_cursor",
            query="SELECT * FROM large_table",
            connection_id="test_conn",
        )

        assert "cursor_" in result

    def test_scroll_cursor(self, mock_cursor):
        cursor_id = "test_cursor"
        _active_cursors[cursor_id] = mock_cursor

        result = scroll_cursor(cursor_id, 10, "relative")

        assert "scrolled" in result.lower()


class TestCopy:
    """Tests for COPY functions."""

    def test_copy_from(self, mock_conn, mock_cursor):
        _active_connections["test_conn"] = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        result = copy_from(
            table="test_table",
            columns=["id", "name"],
            connection_id="test_conn",
        )

        assert "copied" in result.lower() or "table" in result.lower()

    def test_copy_to(self, mock_conn, mock_cursor):
        _active_connections["test_conn"] = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        result = copy_to(
            table="test_table",
            connection_id="test_conn",
        )

        assert isinstance(result, str)

    def test_copy_expert(self, mock_conn, mock_cursor):
        _active_connections["test_conn"] = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        result = copy_expert(
            sql="COPY test_table TO STDOUT",
            connection_id="test_conn",
        )

        assert isinstance(result, str)


class TestInfo:
    """Tests for info functions."""

    def test_get_server_version(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        mock_conn.info.server_version = 150005

        result = get_server_version("test_conn")

        assert result == 150005

    def test_get_backend_pid(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        mock_conn.info.backend_pid = 12345

        result = get_backend_pid("test_conn")

        assert result == 12345

    def test_get_dsn_parameters(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        mock_conn.info.dsn_parameters = {"dbname": "test", "user": "postgres"}

        result = get_dsn_parameters("test_conn")

        assert result["dbname"] == "test"

    def test_get_notices(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        mock_conn.notices = ["Notice 1", "Notice 2"]

        result = get_notices("test_conn")

        assert len(result) == 2


class TestDSN:
    """Tests for DSN functions."""

    def test_parse_dsn(self):
        with patch("psycopg2.extensions.parse_dsn") as mock_parse:
            mock_parse.return_value = {"dbname": "test", "user": "postgres"}
            result = parse_dsn("dbname=test user=postgres")
            assert result["dbname"] == "test"

    def test_make_dsn(self):
        with patch("psycopg2.extensions.make_dsn") as mock_make:
            mock_make.return_value = "dbname=test user=postgres"
            result = make_dsn(dbname="test", user="postgres")
            assert "test" in result


class TestTypeRegistration:
    """Tests for type registration functions."""

    def test_register_json(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        with patch("psycopg2.extras.register_json"):
            result = register_json("test_conn")
            assert "registered" in result.lower()

    def test_register_hstore(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        with patch("psycopg2.extras.register_hstore"):
            result = register_hstore("test_conn")
            assert "registered" in result.lower()

    def test_register_composite(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        mock_conn.cursor.return_value = MagicMock()
        with patch("psycopg2.extras.register_composite"):
            result = register_composite("my_type", "test_conn")
            assert "registered" in result.lower()


class TestLargeObject:
    """Tests for large object functions."""

    def test_create_large_object(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        mock_lobj = MagicMock()
        mock_lobj.oid = 12345
        mock_lobj.mode = "rw"
        mock_conn.lobject.return_value = mock_lobj

        result = create_large_object("test_conn")

        assert result["oid"] == 12345

    def test_read_large_object(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        mock_lobj = MagicMock()
        mock_lobj.read.return_value = "binary data"
        mock_conn.lobject.return_value = mock_lobj

        result = read_large_object(12345, connection_id="test_conn")

        assert result == "binary data"

    def test_write_large_object(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        mock_lobj = MagicMock()
        mock_lobj.oid = 12345
        mock_lobj.write.return_value = 5
        mock_conn.lobject.return_value = mock_lobj

        result = write_large_object("data", connection_id="test_conn")

        assert result["bytes_written"] == 5

    def test_create_large_object_not_found(self):
        with pytest.raises(Exception) as exc_info:
            create_large_object(connection_id="nonexistent")
        assert "not found" in str(exc_info.value)

    def test_read_large_object_not_found(self):
        with pytest.raises(Exception) as exc_info:
            read_large_object(12345, connection_id="nonexistent")
        assert "not found" in str(exc_info.value)

    def test_write_large_object_not_found(self):
        with pytest.raises(Exception) as exc_info:
            write_large_object("data", connection_id="nonexistent")
        assert "not found" in str(exc_info.value)

    def test_cancel_query_not_found(self):
        with pytest.raises(Exception) as exc_info:
            cancel_query("nonexistent")
        assert "not found" in str(exc_info.value)


class TestCancel:
    """Tests for cancel function."""

    def test_cancel_query(self, mock_conn):
        _active_connections["test_conn"] = mock_conn

        result = cancel_query("test_conn")

        assert "cancelled" in result.lower() or "cancel" in result.lower()
        mock_conn.cancel.assert_called_once()


class TestMogrify:
    """Tests for mogrify function."""

    def test_mogrify(self, mock_conn, mock_cursor):
        _active_connections["test_conn"] = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.mogrify.return_value = b"INSERT INTO t VALUES ('value')"

        result = mogrify("INSERT INTO t VALUES (%s)", ["value"], "test_conn")

        assert "value" in result.lower()

    def test_mogrify_not_found(self):
        with pytest.raises(Exception) as exc_info:
            mogrify("SELECT 1", connection_id="nonexistent")
        assert "not found" in str(exc_info.value)


class TestSetSession:
    """Tests for set_session function."""

    def test_set_session(self, mock_conn):
        _active_connections["test_conn"] = mock_conn

        result = set_session(readonly=True, connection_id="test_conn")

        assert "set" in result.lower()
        mock_conn.set_session.assert_called_once()

    def test_set_session_not_found(self):
        with pytest.raises(Exception) as exc_info:
            set_session(readonly=True, connection_id="nonexistent")
        assert "not found" in str(exc_info.value)


class TestList:
    """Tests for list functions."""

    def test_list_connections_empty(self):
        result = list_connections()
        assert result == []

    def test_list_connections_with_data(self, mock_conn):
        _active_connections["test_conn"] = mock_conn
        mock_conn.closed = 0

        result = list_connections()

        assert len(result) == 1
        assert result[0]["id"] == "test_conn"

    def test_list_cursors_empty(self):
        result = list_cursors()
        assert result == []

    def test_list_cursors_with_data(self, mock_cursor):
        cursor_id = "cursor_test_conn_1"
        _active_cursors[cursor_id] = mock_cursor

        result = list_cursors()

        assert len(result) == 1
