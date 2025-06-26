# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Development Setup
```bash
# Install uv package manager
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Type check
uv run pyright

# Build package
uv build
```

### Running the Server
```bash
# Run with default settings (read-only)
uv run mcp_snowflake_server

# Run with write operations enabled
uv run mcp_snowflake_server --allow-write

# Run with debug logging
uv run mcp_snowflake_server --log-level DEBUG
```

### Environment Configuration
Create a `.env` file with Snowflake credentials:
```
SNOWFLAKE_USER="xxx@your_email.com"
SNOWFLAKE_ACCOUNT="xxx"
SNOWFLAKE_ROLE="xxx"
SNOWFLAKE_DATABASE="xxx"
SNOWFLAKE_SCHEMA="xxx"
SNOWFLAKE_WAREHOUSE="xxx"
SNOWFLAKE_PASSWORD="xxx"
```

## Troubleshooting

### SSL/OpenSSL Compatibility Issues

If you encounter errors like `AttributeError: module 'lib' has no attribute 'X509_V_FLAG_NOTIFY_POLICY'`, this is due to incompatibility between pyOpenSSL and cryptography versions. The project dependencies now include version constraints to prevent this:

- `cryptography<43` - Prevents newer versions that are incompatible with Snowflake connector
- `pyOpenSSL>=24.0.0` - Ensures compatibility with the cryptography version

These constraints are automatically installed with the package dependencies.

## Architecture

This is an MCP (Model Context Protocol) server that provides tools for interacting with Snowflake databases. The architecture follows a clean separation of concerns:

### Core Components

1. **Entry Point** (`src/mcp_snowflake_server/__init__.py`): Handles CLI argument parsing and environment variable loading for Snowflake connection parameters.

2. **MCP Server** (`src/mcp_snowflake_server/server.py`): Implements the MCP protocol with tool handlers for database operations. Key responsibilities:
   - Tool registration and handling
   - Resource management (insights memo, table contexts)
   - Request routing to database client

3. **Database Client** (`src/mcp_snowflake_server/db_client.py`): Wraps Snowflake's Snowpark Session API with:
   - Connection pooling using threading locks
   - Safe query execution with error handling
   - Schema introspection methods
   - Runtime configuration filtering (exclusion patterns)

4. **Write Detector** (`src/mcp_snowflake_server/write_detector.py`): SQL parser that identifies write operations to enforce read-only mode when `--allow-write` is not specified.

### Key Design Patterns

- **Safety by Default**: Write operations are disabled unless explicitly enabled
- **Resource Filtering**: `runtime_config.json` defines exclusion patterns to hide sensitive databases/schemas/tables
- **Lazy Connection**: Database connection is established only when first needed
- **Thread-Safe Operations**: Uses locks to ensure thread-safe database access

### MCP Tools Exposed

- **Query Tools**: `read_query`, `write_query` (conditional), `create_table` (conditional)
- **Schema Tools**: `list_databases`, `list_schemas`, `list_tables`, `describe_table`
- **Analysis Tools**: `append_insight` for tracking data insights
- **Resources**: Memo resource for insights, optional table context resources

### Important Implementation Notes

- The server uses Snowflake's Snowpark Python API rather than raw SQL connections
- All database operations go through the centralized `SnowflakeDBClient` class
- SQL write detection is performed using the `sqlparse` library
- The project requires Python 3.10-3.12 due to Snowflake connector compatibility