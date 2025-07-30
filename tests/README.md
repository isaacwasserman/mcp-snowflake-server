# MCP Snowflake Server Test Suite

This directory contains comprehensive unit and integration tests for the MCP Snowflake Server.

## Test Structure

```
tests/
├── __init__.py                 # Test package initialization
├── conftest.py                 # Pytest configuration and shared fixtures
├── test_write_detector.py      # Unit tests for SQLWriteDetector
├── test_db_client.py           # Unit tests for SnowflakeDB client
├── test_server_handlers.py     # Unit tests for server tool handlers
├── test_server_integration.py  # Integration tests for main server
├── run_tests.py                # Test runner script
├── pytest.ini                  # Pytest configuration
└── README.md                   # This file
```

## Test Categories

### Unit Tests
- **SQLWriteDetector Tests** (`test_write_detector.py`): Tests SQL query analysis and write operation detection
- **SnowflakeDB Tests** (`test_db_client.py`): Tests database client functionality, connection management, and query execution
- **Server Handler Tests** (`test_server_handlers.py`): Tests individual tool handlers and utility functions

### Integration Tests
- **Server Integration Tests** (`test_server_integration.py`): Tests complete server functionality, configuration loading, and tool integration

## Running Tests

### Prerequisites

Install test dependencies:
```bash
uv sync --dev
```

### Basic Test Execution

Run all tests:
```bash
pytest
```

Run with coverage:
```bash
pytest --cov=src/mcp_snowflake_server --cov-report=html
```

Run tests with additional debugging:
```bash
pytest -v -s --tb=long
```

Run a specific test:
```bash
pytest tests/test_write_detector.py::TestSQLWriteDetector::test_analyze_select_query
```

Run tests in parallel (requires pytest-xdist):
```bash
pytest -n auto
```

### Test Markers

TODO: We should organize tests using markers:

- `@pytest.mark.unit`: Unit tests
- `@pytest.mark.integration`: Integration tests
- `@pytest.mark.slow`: Slow-running tests
- `@pytest.mark.asyncio`: Async tests

Then, we can run specific test types:
```bash
pytest -m unit          # Run only unit tests
pytest -m integration   # Run only integration tests
pytest -m "not slow"    # Skip slow tests
```
