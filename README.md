# mcp-psycopg2

MCP server exposing psycopg2 PostgreSQL database adapter functionality.

[![PyPI](https://img.shields.io/pypi/v/mcp-psycopg2.svg)](https://pypi.org/project/mcp-psycopg2/)
[![Python](https://img.shields.io/pypi/pyversions/mcp-psycopg2.svg)](https://pypi.org/project/mcp-psycopg2/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

mcp-name: io.github.daedalus/mcp-psycopg2

## Install

```bash
pip install mcp-psycopg2
```

## Usage

```python
from mcp_psycopg2 import mcp

# Run the MCP server
mcp.run()
```

## MCP Tools

The server exposes the following tools for interacting with PostgreSQL databases:

### Connection Management
- `connect` - Create a new database connection
- `close_connection` - Close an existing connection
- `get_connection_info` - Get connection details

### Transaction Management
- `begin_transaction` - Start a new transaction
- `commit_transaction` - Commit the current transaction
- `rollback_transaction` - Rollback the current transaction
- `set_isolation_level` - Set transaction isolation level

### Cursor Operations
- `create_cursor` - Create a new cursor
- `close_cursor` - Close a cursor
- `execute_query` - Execute a SQL query
- `execute_many` - Execute a query with multiple parameter sets
- `fetch_one` - Fetch one row
- `fetch_many` - Fetch multiple rows
- `fetch_all` - Fetch all remaining rows

### SQL Composition
- `quote_identifier` - Quote an SQL identifier
- `mogrify` - Return query string after parameter binding

### Type Registration
- `register_json` - Register JSON type adapter
- `register_hstore` - Register hstore type adapter
- `register_composite` - Register composite type adapter

### COPY Operations
- `copy_from` - Copy data from file to table
- `copy_to` - Copy data from table to file
- `copy_expert` - Execute custom COPY statement

### Server-Side Cursors
- `create_named_cursor` - Create a server-side named cursor
- `scroll_cursor` - Scroll through cursor results

### Large Objects
- `create_large_object` - Create or open a large object
- `read_large_object` - Read from a large object
- `write_large_object` - Write to a large object

### Information
- `get_server_version` - Get PostgreSQL server version
- `get_backend_pid` - Get backend process ID
- `get_dsn_parameters` - Get connection parameters
- `get_notices` - Get database notices
- `parse_dsn` - Parse a connection string
- `make_dsn` - Create a connection string

### Utility
- `cancel_query` - Cancel the current database operation
- `set_session` - Set session parameters
- `list_connections` - List all active connections
- `list_cursors` - List all active cursors

## Development

```bash
git clone https://github.com/daedalus/mcp-psycopg2.git
cd mcp-psycopg2
pip install -e ".[test]"

# run tests
pytest

# format
ruff format src/ tests/

# lint
ruff check src/ tests/

# type check
mypy src/
```
