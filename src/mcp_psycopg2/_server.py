from __future__ import annotations

import io
from typing import TYPE_CHECKING, Any

import fastmcp
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import psycopg2.sql

if TYPE_CHECKING:
    from psycopg2.extensions import connection, cursor

mcp = fastmcp.FastMCP("mcp-psycopg2")

_active_connections: dict[str, connection] = {}
_active_cursors: dict[str, cursor] = {}


@mcp.tool()
def connect(
    dbname: str = "test",
    user: str = "postgres",
    password: str = "password",
    host: str = "localhost",
    port: int = 5432,
    connection_id: str = "default",
) -> str:
    """Create a new PostgreSQL database connection.

    Args:
        dbname: Database name to connect to.
        user: User name for authentication.
        password: Password for authentication.
        host: Database host address.
        port: Connection port number (default 5432).
        connection_id: Unique identifier for this connection (default 'default').

    Returns:
        Connection ID if successful.

    Example:
        >>> connect(dbname="mydb", user="admin", password="secret", host="localhost")
        "default"
    """
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )
        _active_connections[connection_id] = conn
        return connection_id
    except psycopg2.Error as e:
        raise Exception(f"Connection failed: {e}")


@mcp.tool()
def close_connection(connection_id: str = "default") -> str:
    """Close an existing database connection.

    Args:
        connection_id: The ID of the connection to close.

    Returns:
        Confirmation message.

    Example:
        >>> close_connection("default")
        "Connection closed"
    """
    if connection_id in _active_connections:
        conn = _active_connections.pop(connection_id)
        conn.close()
        for cursor_id in list(_active_cursors.keys()):
            if cursor_id.startswith(connection_id):
                del _active_cursors[cursor_id]
        return "Connection closed"
    raise Exception(f"Connection {connection_id} not found")


@mcp.tool()
def get_connection_info(connection_id: str = "default") -> dict[str, Any]:
    """Get information about a database connection.

    Args:
        connection_id: The ID of the connection.

    Returns:
        Dictionary containing connection information.

    Example:
        >>> get_connection_info("default")
        {"dbname": "test", "user": "postgres", "host": "localhost", ...}
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    info = conn.info
    return {
        "dbname": info.dbname,
        "user": info.user,
        "host": info.host,
        "port": info.port,
        "server_version": info.server_version,
        "protocol_version": info.protocol_version,
        "backend_pid": info.backend_pid,
        "status": info.status,
        "transaction_status": info.transaction_status,
        "dsn_parameters": info.dsn_parameters,
    }


@mcp.tool()
def begin_transaction(connection_id: str = "default") -> str:
    """Begin a new transaction.

    Args:
        connection_id: The ID of the connection.

    Returns:
        Confirmation message.

    Example:
        >>> begin_transaction("default")
        "Transaction started"
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")
    return "Transaction started"


@mcp.tool()
def commit_transaction(connection_id: str = "default") -> str:
    """Commit the current transaction.

    Args:
        connection_id: The ID of the connection.

    Returns:
        Confirmation message.

    Example:
        >>> commit_transaction("default")
        "Transaction committed"
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")
    conn.commit()
    return "Transaction committed"


@mcp.tool()
def rollback_transaction(connection_id: str = "default") -> str:
    """Rollback the current transaction.

    Args:
        connection_id: The ID of the connection.

    Returns:
        Confirmation message.

    Example:
        >>> rollback_transaction("default")
        "Transaction rolled back"
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")
    conn.rollback()
    return "Transaction rolled back"


@mcp.tool()
def set_isolation_level(
    level: str,
    connection_id: str = "default",
) -> str:
    """Set transaction isolation level.

    Args:
        level: Isolation level (AUTOCOMMIT, READ_UNCOMMITTED, READ_COMMITTED,
               REPEATABLE_READ, SERIALIZABLE, DEFAULT).
        connection_id: The ID of the connection.

    Returns:
        Confirmation message.

    Example:
        >>> set_isolation_level("READ_COMMITTED", "default")
        "Isolation level set"
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    levels = {
        "AUTOCOMMIT": psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT,
        "READ_UNCOMMITTED": psycopg2.extensions.ISOLATION_LEVEL_READ_UNCOMMITTED,
        "READ_COMMITTED": psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED,
        "REPEATABLE_READ": psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ,
        "SERIALIZABLE": psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE,
        "DEFAULT": psycopg2.extensions.ISOLATION_LEVEL_DEFAULT,
    }

    level_upper = level.upper()
    if level_upper not in levels:
        raise Exception(f"Invalid isolation level: {level}")

    conn.set_isolation_level(levels[level_upper])
    return "Isolation level set"


@mcp.tool()
def create_cursor(
    cursor_name: str | None = None,
    connection_id: str = "default",
    cursor_id: str | None = None,
    scrollable: bool | None = None,
    withhold: bool = False,
) -> str:
    """Create a new database cursor.

    Args:
        cursor_name: Name for server-side cursor (optional).
        connection_id: The ID of the connection.
        cursor_id: Custom cursor ID (defaults to auto-generated).
        scrollable: Whether cursor can scroll backwards.
        withhold: Whether cursor persists after commit.

    Returns:
        Cursor ID.

    Example:
        >>> create_cursor(connection_id="default")
        "cursor_default_1"
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    cursor = conn.cursor(
        name=cursor_name,
        scrollable=scrollable,
        withhold=withhold,
    )

    if cursor_id is None:
        cursor_id = f"cursor_{connection_id}_{len(_active_cursors) + 1}"

    _active_cursors[cursor_id] = cursor
    return cursor_id


@mcp.tool()
def close_cursor(cursor_id: str) -> str:
    """Close a database cursor.

    Args:
        cursor_id: The ID of the cursor to close.

    Returns:
        Confirmation message.

    Example:
        >>> close_cursor("cursor_default_1")
        "Cursor closed"
    """
    if cursor_id in _active_cursors:
        cursor = _active_cursors.pop(cursor_id)
        cursor.close()
        return "Cursor closed"
    raise Exception(f"Cursor {cursor_id} not found")


@mcp.tool()
def execute_query(
    query: str,
    params: list[Any] | None = None,
    cursor_id: str | None = None,
    connection_id: str = "default",
) -> dict[str, Any]:
    """Execute a SQL query.

    Args:
        query: SQL query to execute.
        params: Query parameters (optional).
        cursor_id: Cursor ID to use (creates new if not provided).
        connection_id: Connection ID.

    Returns:
        Dictionary with query results and metadata.

    Example:
        >>> execute_query("SELECT * FROM users WHERE id = %s", [1])
        {"rows": [[1, "john"]], "rowcount": 1, "columns": [...]}
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    if cursor_id and cursor_id in _active_cursors:
        cursor = _active_cursors[cursor_id]
    else:
        cursor = conn.cursor()
        if cursor_id:
            _active_cursors[cursor_id] = cursor

    try:
        cursor.execute(query, params)

        columns = []
        if cursor.description:
            columns = [
                {
                    "name": col.name,
                    "type_code": col.type_code,
                    "display_size": col.display_size,
                    "internal_size": col.internal_size,
                    "precision": col.precision,
                    "scale": col.scale,
                    "null_ok": col.null_ok,
                }
                for col in cursor.description
            ]

        rows = cursor.fetchall() if cursor.description else []

        return {
            "rows": [list(row) for row in rows],
            "rowcount": cursor.rowcount,
            "columns": columns,
            "query": cursor.query.decode() if cursor.query else None,
            "statusmessage": cursor.statusmessage,
        }
    except psycopg2.Error as e:
        raise Exception(f"Query failed: {e}")


@mcp.tool()
def execute_many(
    query: str,
    params_list: list[list[Any]],
    cursor_id: str | None = None,
    connection_id: str = "default",
) -> dict[str, Any]:
    """Execute a SQL query with multiple parameter sets.

    Args:
        query: SQL query to execute.
        params_list: List of parameter sets.
        cursor_id: Cursor ID to use.
        connection_id: Connection ID.

    Returns:
        Dictionary with execution results.

    Example:
        >>> execute_many("INSERT INTO users (name) VALUES (%s)", [["alice"], ["bob"]])
        {"rowcount": 2}
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    if cursor_id and cursor_id in _active_cursors:
        cursor = _active_cursors[cursor_id]
    else:
        cursor = conn.cursor()
        if cursor_id:
            _active_cursors[cursor_id] = cursor

    try:
        cursor.executemany(query, params_list)
        return {
            "rowcount": cursor.rowcount,
            "statusmessage": cursor.statusmessage,
        }
    except psycopg2.Error as e:
        raise Exception(f"Query failed: {e}")


@mcp.tool()
def fetch_one(
    cursor_id: str,
) -> list[Any] | None:
    """Fetch one row from cursor.

    Args:
        cursor_id: Cursor ID.

    Returns:
        Row data as list, or None if no more rows.

    Example:
        >>> fetch_one("cursor_default_1")
        [1, "john"]
    """
    cursor = _active_cursors.get(cursor_id)
    if not cursor:
        raise Exception(f"Cursor {cursor_id} not found")

    row = cursor.fetchone()
    return list(row) if row else None


@mcp.tool()
def fetch_many(
    cursor_id: str,
    size: int = 1,
) -> list[list[Any]]:
    """Fetch multiple rows from cursor.

    Args:
        cursor_id: Cursor ID.
        size: Number of rows to fetch.

    Returns:
        List of rows.

    Example:
        >>> fetch_many("cursor_default_1", 10)
        [[1, "john"], [2, "jane"]]
    """
    cursor = _active_cursors.get(cursor_id)
    if not cursor:
        raise Exception(f"Cursor {cursor_id} not found")

    rows = cursor.fetchmany(size)
    return [list(row) for row in rows]


@mcp.tool()
def fetch_all(cursor_id: str) -> list[list[Any]]:
    """Fetch all remaining rows from cursor.

    Args:
        cursor_id: Cursor ID.

    Returns:
        List of all rows.

    Example:
        >>> fetch_all("cursor_default_1")
        [[1, "john"], [2, "jane"], [3, "bob"]]
    """
    cursor = _active_cursors.get(cursor_id)
    if not cursor:
        raise Exception(f"Cursor {cursor_id} not found")

    rows = cursor.fetchall()
    return [list(row) for row in rows]


@mcp.tool()
def quote_identifier(
    identifier: str,
    connection_id: str = "default",
) -> str:
    """Quote an SQL identifier.

    Args:
        identifier: The identifier to quote.
        connection_id: Connection ID.

    Returns:
        Quoted identifier.

    Example:
        >>> quote_identifier("my_table")
        '"my_table"'
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    return psycopg2.extensions.quote_ident(identifier, conn)  # type: ignore[no-any-return]


@mcp.tool()
def create_named_cursor(
    name: str,
    query: str | None = None,
    scrollable: bool = False,
    withhold: bool = False,
    connection_id: str = "default",
) -> str:
    """Create a server-side named cursor.

    Args:
        name: Name for the cursor.
        query: Initial query to execute (optional).
        scrollable: Whether cursor can scroll backwards.
        withhold: Whether cursor persists after commit.
        connection_id: Connection ID.

    Returns:
        Cursor ID.

    Example:
        >>> create_named_cursor("my_cursor", "SELECT * FROM large_table")
        "cursor_default_1"
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    cursor = conn.cursor(
        name=name,
        scrollable=scrollable,
        withhold=withhold,
    )

    cursor_id = f"cursor_{name}_{len(_active_cursors) + 1}"
    _active_cursors[cursor_id] = cursor

    if query:
        cursor.execute(query)

    return cursor_id


@mcp.tool()
def scroll_cursor(
    cursor_id: str,
    value: int,
    mode: str = "relative",
) -> str:
    """Scroll through cursor results.

    Args:
        cursor_id: Cursor ID.
        value: Offset or absolute position.
        mode: 'relative' or 'absolute'.

    Returns:
        Confirmation message.

    Example:
        >>> scroll_cursor("cursor_default_1", 10, "relative")
        "Cursor scrolled"
    """
    cursor = _active_cursors.get(cursor_id)
    if not cursor:
        raise Exception(f"Cursor {cursor_id} not found")

    cursor.scroll(value, mode=mode)
    return "Cursor scrolled"


@mcp.tool()
def copy_from(
    table: str,
    columns: list[str] | None = None,
    sep: str = "\t",
    null: str = "\\N",
    size: int = 8192,
    cursor_id: str | None = None,
    connection_id: str = "default",
) -> str:
    """Copy data from a file-like object to a table.

    Args:
        table: Target table name.
        columns: Column names (optional).
        sep: Column separator.
        null: NULL representation.
        size: Buffer size.
        cursor_id: Cursor ID.
        connection_id: Connection ID.

    Returns:
        Status message.

    Example:
        >>> copy_from("my_table", columns=["id", "name"])
        "Copied 100 rows"
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    if cursor_id and cursor_id in _active_cursors:
        cursor = _active_cursors[cursor_id]
    else:
        cursor = conn.cursor()
        if cursor_id:
            _active_cursors[cursor_id] = cursor

    file_obj = io.StringIO()
    cursor.copy_from(file_obj, table, sep=sep, null=null, columns=columns, size=size)
    return "Data copied to table"


@mcp.tool()
def copy_to(
    table: str,
    columns: list[str] | None = None,
    sep: str = "\t",
    null: str = "\\N",
    cursor_id: str | None = None,
    connection_id: str = "default",
) -> str:
    """Copy data from a table to a file-like object.

    Args:
        table: Source table name.
        columns: Column names (optional).
        sep: Column separator.
        null: NULL representation.
        cursor_id: Cursor ID.
        connection_id: Connection ID.

    Returns:
        Copied data as string.

    Example:
        >>> copy_to("my_table")
        "1\\tjohn\\n2\\tjane\\n"
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    if cursor_id and cursor_id in _active_cursors:
        cursor = _active_cursors[cursor_id]
    else:
        cursor = conn.cursor()
        if cursor_id:
            _active_cursors[cursor_id] = cursor

    file_obj = io.StringIO()
    cursor.copy_to(file_obj, table, sep=sep, null=null, columns=columns)
    return file_obj.getvalue()


@mcp.tool()
def copy_expert(
    sql: str,
    connection_id: str = "default",
) -> str:
    """Execute custom COPY statement.

    Args:
        sql: COPY SQL statement.
        connection_id: Connection ID.

    Returns:
        Status message.

    Example:
        >>> copy_expert("COPY my_table TO STDOUT WITH CSV HEADER")
        "data\\n1,john\\n2,jane\\n"
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    cursor = conn.cursor()
    file_obj = io.StringIO()
    cursor.copy_expert(sql, file_obj)
    return file_obj.getvalue()


@mcp.tool()
def get_server_version(connection_id: str = "default") -> int:
    """Get PostgreSQL server version.

    Args:
        connection_id: Connection ID.

    Returns:
        Server version as integer.

    Example:
        >>> get_server_version("default")
        150005
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    return conn.info.server_version  # type: ignore[no-any-return]


@mcp.tool()
def get_backend_pid(connection_id: str = "default") -> int:
    """Get backend process ID.

    Args:
        connection_id: Connection ID.

    Returns:
        Backend PID.

    Example:
        >>> get_backend_pid("default")
        12345
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    return conn.info.backend_pid  # type: ignore[no-any-return]


@mcp.tool()
def get_dsn_parameters(connection_id: str = "default") -> dict[str, str]:
    """Get connection parameters.

    Args:
        connection_id: Connection ID.

    Returns:
        Dictionary of DSN parameters.

    Example:
        >>> get_dsn_parameters("default")
        {"dbname": "test", "user": "postgres", "host": "localhost"}
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    return conn.info.dsn_parameters  # type: ignore[no-any-return]


@mcp.tool()
def get_notices(connection_id: str = "default") -> list[str]:
    """Get database notices.

    Args:
        connection_id: Connection ID.

    Returns:
        List of notice messages.

    Example:
        >>> get_notices("default")
        ["NOTICE: CREATE TABLE will create implicit sequence..."]
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    return list(conn.notices)


@mcp.tool()
def parse_dsn(dsn: str) -> dict[str, str]:
    """Parse a connection string.

    Args:
        dsn: Connection string to parse.

    Returns:
        Dictionary of connection parameters.

    Example:
        >>> parse_dsn("dbname=test user=postgres")
        {"dbname": "test", "user": "postgres"}
    """
    return psycopg2.extensions.parse_dsn(dsn)  # type: ignore[no-any-return]


@mcp.tool()
def make_dsn(
    dbname: str | None = None,
    user: str | None = None,
    password: str | None = None,
    host: str | None = None,
    port: int | None = None,
) -> str:
    """Create a connection string from arguments.

    Args:
        dbname: Database name.
        user: User name.
        password: Password.
        host: Host address.
        port: Port number.

    Returns:
        Connection string.

    Example:
        >>> make_dsn(dbname="test", user="postgres", host="localhost")
        "dbname=test user=postgres host=localhost"
    """
    return psycopg2.extensions.make_dsn(  # type: ignore[no-any-return]
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )


@mcp.tool()
def register_json(
    connection_id: str = "default",
    globally: bool = False,
) -> str:
    """Register JSON type adapter.

    Args:
        connection_id: Connection ID.
        globally: Register globally.

    Returns:
        Confirmation message.

    Example:
        >>> register_json("default")
        "JSON type registered"
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    psycopg2.extras.register_json(conn, globally=globally)
    return "JSON type registered"


@mcp.tool()
def register_hstore(
    connection_id: str = "default",
    globally: bool = False,
    unicode: bool = False,
) -> str:
    """Register hstore type adapter.

    Args:
        connection_id: Connection ID.
        globally: Register globally.
        unicode: Use unicode keys/values.

    Returns:
        Confirmation message.

    Example:
        >>> register_hstore("default")
        "Hstore type registered"
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    psycopg2.extras.register_hstore(conn, globally=globally, unicode=unicode)
    return "Hstore type registered"


@mcp.tool()
def register_composite(
    name: str,
    connection_id: str = "default",
    globally: bool = False,
) -> str:
    """Register composite type adapter.

    Args:
        name: Composite type name.
        connection_id: Connection ID.
        globally: Register globally.

    Returns:
        Confirmation message.

    Example:
        >>> register_composite("my_type", "default")
        "Composite type registered"
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    cursor = conn.cursor()
    psycopg2.extras.register_composite(name, cursor, globally=globally)
    return "Composite type registered"


@mcp.tool()
def create_large_object(
    connection_id: str = "default",
    oid: int = 0,
    mode: str = "rw",
) -> dict[str, Any]:
    """Create or open a large object.

    Args:
        connection_id: Connection ID.
        oid: Object OID (0 for new).
        mode: Access mode (r, w, rw, n, b, t).

    Returns:
        Dictionary with OID and mode.

    Example:
        >>> create_large_object("default")
        {"oid": 12345, "mode": "rw"}
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    lobj = conn.lobject(oid=oid, mode=mode)
    return {"oid": lobj.oid, "mode": lobj.mode}


@mcp.tool()
def read_large_object(
    oid: int,
    size: int = -1,
    connection_id: str = "default",
) -> str:
    """Read from a large object.

    Args:
        oid: Object OID.
        size: Bytes to read (-1 for all).
        connection_id: Connection ID.

    Returns:
        Data read from large object.

    Example:
        >>> read_large_object(12345, "default")
        "binary data..."
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    lobj = conn.lobject(oid=oid, mode="r")
    return lobj.read(size)  # type: ignore[no-any-return]


@mcp.tool()
def write_large_object(
    data: str,
    oid: int = 0,
    connection_id: str = "default",
) -> dict[str, Any]:
    """Write to a large object.

    Args:
        data: Data to write.
        oid: Object OID (0 for new).
        connection_id: Connection ID.

    Returns:
        Dictionary with OID and bytes written.

    Example:
        >>> write_large_object("data", 0, "default")
        {"oid": 12345, "bytes_written": 4}
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    lobj = conn.lobject(oid=oid, mode="w")
    written = lobj.write(data)
    return {"oid": lobj.oid, "bytes_written": written}


@mcp.tool()
def cancel_query(connection_id: str = "default") -> str:
    """Cancel the current database operation.

    Args:
        connection_id: Connection ID.

    Returns:
        Confirmation message.

    Example:
        >>> cancel_query("default")
        "Query cancelled"
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    conn.cancel()
    return "Query cancelled"


@mcp.tool()
def mogrify(
    query: str,
    params: list[Any] | None = None,
    connection_id: str = "default",
) -> str:
    """Return query string after parameter binding.

    Args:
        query: SQL query.
        params: Query parameters.
        connection_id: Connection ID.

    Returns:
        Mogrified query string.

    Example:
        >>> mogrify("INSERT INTO t VALUES (%s)", ["value"])
        "INSERT INTO t VALUES (E'value')"
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    cursor = conn.cursor()
    return cursor.mogrify(query, params).decode()  # type: ignore[no-any-return]


@mcp.tool()
def set_session(
    isolation_level: str | None = None,
    readonly: bool | None = None,
    deferrable: bool | None = None,
    autocommit: bool | None = None,
    connection_id: str = "default",
) -> str:
    """Set session parameters.

    Args:
        isolation_level: Isolation level.
        readonly: Read-only mode.
        deferrable: Deferrable mode.
        autocommit: Autocommit mode.
        connection_id: Connection ID.

    Returns:
        Confirmation message.

    Example:
        >>> set_session(readonly=True, connection_id="default")
        "Session parameters set"
    """
    conn = _active_connections.get(connection_id)
    if not conn:
        raise Exception(f"Connection {connection_id} not found")

    conn.set_session(
        isolation_level=isolation_level,
        readonly=readonly,
        deferrable=deferrable,
        autocommit=autocommit,
    )
    return "Session parameters set"


@mcp.tool()
def list_connections() -> list[dict[str, Any]]:
    """List all active connections.

    Returns:
        List of connection information.

    Example:
        >>> list_connections()
        [{"id": "default", "status": "OK"}]
    """
    result = []
    for conn_id, conn in _active_connections.items():
        result.append(
            {
                "id": conn_id,
                "closed": conn.closed,
                "status": conn.info.status,
            }
        )
    return result


@mcp.tool()
def list_cursors(connection_id: str | None = None) -> list[dict[str, Any]]:
    """List all active cursors.

    Args:
        connection_id: Filter by connection ID.

    Returns:
        List of cursor information.

    Example:
        >>> list_cursors()
        [{"id": "cursor_default_1", "name": None, "closed": False}]
    """
    result = []
    for cursor_id, cursor in _active_cursors.items():
        if connection_id and not cursor_id.startswith(f"cursor_{connection_id}"):
            continue
        result.append(
            {
                "id": cursor_id,
                "name": cursor.name,
                "closed": cursor.closed,
            }
        )
    return result


if __name__ == "__main__":
    mcp.run()
