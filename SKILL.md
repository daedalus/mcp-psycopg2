# MCP Psycopg2

MCP server exposing PostgreSQL database functionality via psycopg2.

## When to use this skill

Use this skill when you need to:
- Connect to PostgreSQL databases
- Execute queries
- Handle transactions
- Manage large objects
- COPY data

## Tools

**Connection:**
- `connect`, `close_connection`, `get_connection_info`

**Transaction:**
- `begin_transaction`, `commit_transaction`, `rollback_transaction`
- `set_isolation_level`

**Cursor:**
- `create_cursor`, `close_cursor`
- `execute_query`, `execute_many`
- `fetch_one`, `fetch_many`, `fetch_all`

**Type Registration:**
- `register_json`, `register_hstore`, `register_composite`

**COPY:**
- `copy_from`, `copy_to`, `copy_expert`

**Server-Side Cursors:**
- `create_named_cursor`, `scroll_cursor`

**Large Objects:**
- `create_large_object`, `read_large_object`, `write_large_object`

**Info:**
- `get_server_version`, `get_backend_pid`, `get_dsn_parameters`
- `parse_dsn`, `make_dsn`, `cancel_query`

## Install

```bash
pip install mcp-psycopg2
```