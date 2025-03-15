# Snowflake MCP Server

[![smithery badge](https://smithery.ai/badge/mcp_snowflake_server)](https://smithery.ai/server/mcp_snowflake_server) [![PyPI - Version](https://img.shields.io/pypi/dm/mcp-snowflake-server?color&logo=pypi&logoColor=white&label=PyPI%20downloads)](https://pypi.org/project/mcp-snowflake-server/)


## Overview
A Model Context Protocol (MCP) server implementation that provides database interaction with Snowflake. This server enables running SQL queries with tools and intereacting with a memo of data insights presented as a resource.

## Components

### Resources
The server exposes a single dynamic resource:
- `memo://insights`: A continuously updated data insights memo that aggregates discovered insights during analysis
  - Auto-updates as new insights are discovered via the append-insight tool

### Tools
The server offers six core tools:

#### Query Tools
- `read_query`
  - Execute SELECT queries to read data from the database
  - Input:
    - `query` (string): The SELECT SQL query to execute
  - Returns: Query results as array of objects

- `write_query` (with `--allow-write` flag)
  - Execute INSERT, UPDATE, or DELETE queries
  - Input:
    - `query` (string): The SQL modification query
  - Returns: `{ affected_rows: number }`

- `create_table` (with `--allow-write` flag)
  - Create new tables in the database
  - Input:
    - `query` (string): CREATE TABLE SQL statement
  - Returns: Confirmation of table creation

#### Schema Tools
- `list_databases`
  - Get a list of all databases in the Snowflake instance.
  - No input required
  - Returns: Array of database names.

- `list_schemas`
  - Get a list of all schemas in a specific database.
  - Input:
    - `database` (string): Name of the database.
  - Returns: Array of schema names.

- `list_tables`
  - Get a list of all tables in a specific database and schema.
  - Input:
    - `database` (string): Name of the database.
    - `schema` (string): Name of the schema.
  - Returns: Array of table metadata.

- `describe-table`
  - View column information for a specific table
  - Input:
    - `table_name` (string): Fully qualified name of table to describe (e.g., `database.schema.table`)
  - Returns: Array of column definitions with names and types

#### Analysis Tools
- `append_insight`
  - Add new data insights to the memo resource
  - Input:
    - `insight` (string): data insight discovered from analysis
  - Returns: Confirmation of insight addition
  - Triggers update of memo://insights resource


## Usage Locally with Claude Desktop

An full-version guide can be found [here](https://modelcontextprotocol.io/quickstart/user). Below is a quick guide to get started:


1. Install [Claude AI Desktop App](https://claude.ai/download)

2. Install `uv` by:
```
curl -LsSf https://astral.sh/uv/install.sh | sh
```

3. Create a `.env` file using the following template under this dir
```
SNOWFLAKE_USER="XXX@EMAIL.COM"
SNOWFLAKE_ACCOUNT="XXX"
SNOWFLAKE_ROLE="XXX"
SNOWFLAKE_DATABASE="XXX" # This doesn't affect the MCP's access scope
SNOWFLAKE_SCHEMA="XXX"   # This doesn't affect the MCP's access scope
SNOWFLAKE_WAREHOUSE="XXX"
SNOWFLAKE_AUTHENTICATOR="externalbrowser"
```
4. Test locally using 
```
uv --directory /absolute/path/to/mcp_snowflake_server run mcp_snowflake_server
```

5. Add the server to your `claude_desktop_config.json`
```python
"mcpServers": {
  "snowflake_local": {
      "command": "/absolute/path/to/uv", # obtained by using `which uv`
      "args": [
          "--directory",
          "/absolute/path/to/mcp_snowflake_server",
          "run",
          "mcp_snowflake_server",
          # Optionally: "--allow_write" (but not recommended)
          # Optionally: "--log_dir", "/absolute/path/to/logs"
          # Optionally: "--log_level", "DEBUG"/"INFO"/"WARNING"/"ERROR"/"CRITICAL"
          # Optionally: "--exclude_tools", "{tool name}", ["{other tool name}"]
      ]
  }
}
```
