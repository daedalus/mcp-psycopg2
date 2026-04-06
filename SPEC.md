# SPEC.md — mcp-psycopg2

## Purpose
An MCP server that exposes all the functionality of the psycopg2 2.9.11 PostgreSQL database adapter library, allowing LLMs to interact with PostgreSQL databases through a standardized Model Context Protocol interface.

## Scope

### What IS in scope
- All psycopg2 module-level functions (connect, parse_dsn, make_dsn, etc.)
- All connection class methods (commit, rollback, cursor, etc.)
- All cursor class methods (execute, executemany, fetchone, fetchmany, fetchall, etc.)
- psycopg2.extensions module (type casting, adapters, constants)
- psycopg2.sql module (SQL composition with Identifier, Literal, etc.)
- psycopg2.extras module (DictCursor, RealDictCursor, Json, Range, hstore, etc.)
- Transaction management (begin, commit, rollback)
- Server-side cursors
- COPY operations
- Two-phase commit support

### What is NOT in scope
- Direct libpq C API access
- Replication protocol (will be included as connection option)
- Async connections (will be handled via connection parameter)

## Public API / Interface

### Connection Management Tools
- `connect` - Create a new database connection
- `close_connection` - Close an existing connection
- `get_connection_info` - Get connection details

### Transaction Tools
- `begin_transaction` - Start a new transaction
- `commit_transaction` - Commit the current transaction
- `rollback_transaction` - Rollback the current transaction
- `set_isolation_level` - Set transaction isolation level

### Cursor Tools
- `create_cursor` - Create a new cursor
- `close_cursor` - Close a cursor
- `execute_query` - Execute a query
- `execute_many` - Execute a query with multiple parameter sets
- `fetch_one` - Fetch one row
- `fetch_many` - Fetch multiple rows
- `fetch_all` - Fetch all remaining rows

### SQL Composition Tools
- `quote_identifier` - Quote an SQL identifier
- `compose_sql` - Compose SQL using psycopg2.sql components

### Type Tools
- `register_json` - Register JSON type adapter
- `register_hstore` - Register hstore type adapter
- `register_composite` - Register composite type adapter
- `register_range` - Register range type adapter

### COPY Tools
- `copy_from` - Copy data from file to table
- `copy_to` - Copy data from table to file
- `copy_expert` - Execute custom COPY statement

### Server-Side Cursor Tools
- `create_named_cursor` - Create a server-side named cursor
- `scroll_cursor` - Scroll through cursor results

### Large Object Tools
- `create_large_object` - Create a large object
- `read_large_object` - Read from a large object
- `write_large_object` - Write to a large object

### Info Tools
- `get_server_version` - Get PostgreSQL server version
- `get_backend_pid` - Get backend process ID
- `get_dsn_parameters` - Get connection parameters
- `get_notices` - Get database notices

## Data Formats

### Connection Parameters
- `dbname` - Database name
- `user` - User name
- `password` - Password
- `host` - Host address
- `port` - Port number (default 5432)
- Additional libpq parameters supported

### Query Results
- Results returned as list of tuples (default)
- DictCursor: list of dictionaries
- RealDictCursor: list of dicts
- NamedTupleCursor: list of named tuples

### Error Handling
- All psycopg2 exceptions mapped to appropriate error codes
- Detailed error information via diag attribute

## Edge Cases
1. Connection failures - handle with proper error messages
2. Query timeouts - support cancellation
3. Large result sets - use server-side cursors
4. Binary data - handle with Binary adapter
5. NULL values - proper None handling
6. Transaction in error state - automatic rollback
7. Connection pooling - not handled (single connection per session)
8. Multiple result sets - not supported by PostgreSQL

## Performance & Constraints
- Connection parameters passed at connection time
- Query timeout supported via cancel()
- Server-side cursors for large datasets
- Memory-efficient iteration for large results
