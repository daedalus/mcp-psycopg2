from unittest.mock import MagicMock, patch

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import pytest


@pytest.fixture
def mock_conn():
    """Create a mock connection."""
    with patch("psycopg2.connect") as mock_connect:
        conn = MagicMock(spec=psycopg2.extensions.connection)
        conn.closed = 0
        conn.info = MagicMock()
        conn.info.dbname = "test"
        conn.info.user = "postgres"
        conn.info.host = "localhost"
        conn.info.port = 5432
        conn.info.server_version = 150005
        conn.info.protocol_version = 3
        conn.info.backend_pid = 12345
        conn.info.status = 1
        conn.info.transaction_status = 0
        conn.info.dsn_parameters = {"dbname": "test", "user": "postgres"}
        mock_connect.return_value = conn
        yield conn


@pytest.fixture
def mock_cursor():
    """Create a mock cursor."""
    cursor = MagicMock(spec=psycopg2.extensions.cursor)
    cursor.description = None
    cursor.rowcount = 1
    cursor.query = b"SELECT 1"
    cursor.statusmessage = "SELECT 1"
    cursor.name = None
    cursor.closed = False
    return cursor


@pytest.fixture
def mock_info():
    """Create mock connection info."""
    info = MagicMock(spec=psycopg2.extensions.ConnectionInfo)
    info.dbname = "test"
    info.user = "postgres"
    info.host = "localhost"
    info.port = 5432
    info.server_version = 150005
    info.protocol_version = 3
    info.backend_pid = 12345
    info.status = 1
    info.transaction_status = 0
    info.dsn_parameters = {"dbname": "test", "user": "postgres"}
    return info
