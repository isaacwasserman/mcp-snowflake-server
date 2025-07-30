"""Pytest configuration and fixtures for MCP Snowflake Server tests"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock

from mcp import ServerCapabilities
from mcp.server import Server
from src.mcp_snowflake_server.db_client import SnowflakeDB
from src.mcp_snowflake_server.write_detector import SQLWriteDetector


@pytest.fixture
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_connection_config():
    """Mock connection configuration for testing"""
    return {
        "account": "test_account",
        "user": "test_user",
        "password": "test_password",
        "warehouse": "test_warehouse",
        "database": "test_database",
        "schema": "test_schema"
    }


@pytest.fixture
def mock_snowflake_session():
    """Mock Snowflake session for testing"""
    session = Mock()
    mock_df = Mock()
    mock_df.to_dict.return_value = []
    session.sql.return_value.to_pandas.return_value = mock_df
    return session


@pytest.fixture
def mock_db_client(mock_connection_config, mock_snowflake_session):
    """Mock SnowflakeDB client for testing"""
    db = SnowflakeDB(mock_connection_config)
    db.session = mock_snowflake_session
    db.auth_time = 1000000000  # Set to avoid re-authentication
    db.init_task = None  # No pending initialization task
    # Mock the execute_query method to return proper async mock
    db.execute_query = AsyncMock(return_value=([], "test-data-id"))
    return db


@pytest.fixture
def write_detector():
    """SQLWriteDetector instance for testing"""
    return SQLWriteDetector()


@pytest.fixture
def mock_server():
    """Mock MCP server for testing"""
    server = Mock(spec=Server)
    server.request_context = Mock()
    server.request_context.session = Mock()
    server.request_context.session.send_resource_updated = AsyncMock()
    return server


@pytest.fixture
def sample_query_results():
    """Sample query results for testing"""
    return [
        {"DATABASE_NAME": "TEST_DB1"},
        {"DATABASE_NAME": "TEST_DB2"},
        {"DATABASE_NAME": "EXCLUDED_DB"}
    ]


@pytest.fixture
def sample_table_results():
    """Sample table query results for testing"""
    return [
        {
            "TABLE_CATALOG": "TEST_DB",
            "TABLE_SCHEMA": "PUBLIC",
            "TABLE_NAME": "USERS",
            "COMMENT": "User information table"
        },
        {
            "TABLE_CATALOG": "TEST_DB",
            "TABLE_SCHEMA": "PUBLIC",
            "TABLE_NAME": "ORDERS",
            "COMMENT": "Order tracking table"
        }
    ]


@pytest.fixture
def sample_column_results():
    """Sample column description results for testing"""
    return [
        {
            "COLUMN_NAME": "ID",
            "COLUMN_DEFAULT": None,
            "IS_NULLABLE": "NO",
            "DATA_TYPE": "NUMBER",
            "COMMENT": "Primary key"
        },
        {
            "COLUMN_NAME": "NAME",
            "COLUMN_DEFAULT": None,
            "IS_NULLABLE": "YES",
            "DATA_TYPE": "VARCHAR",
            "COMMENT": "User name"
        }
    ]


@pytest.fixture
def exclusion_config():
    """Sample exclusion configuration for testing"""
    return {
        "databases": ["excluded"],
        "schemas": ["temp"],
        "tables": ["staging"]
    }


def create_mock_db_with_session(connection_config):
    """Helper function to create a properly mocked SnowflakeDB instance"""
    # Create the database instance
    db = SnowflakeDB(connection_config)

    # Create mock session
    mock_session = Mock()
    mock_df = Mock()
    mock_df.to_dict.return_value = []
    mock_session.sql.return_value.to_pandas.return_value = mock_df

    # Set the mock session directly and prevent initialization
    db.session = mock_session
    db.auth_time = 1000000000  # Set to avoid re-authentication
    db._init_database = AsyncMock()  # Mock the initialization method

    return db, mock_session


def setup_server_integration_mocks(mock_snowflake_db_class, mock_stdio_server):
    """Helper function to set up common server integration test mocks"""
    # Setup database mock
    mock_db_instance = Mock()
    mock_snowflake_db_class.return_value = mock_db_instance

    # Setup stdio server mock
    mock_read_stream = Mock()
    mock_write_stream = Mock()
    mock_stdio_server.return_value.__aenter__.return_value = (mock_read_stream, mock_write_stream)

    # Setup server mock with capabilities
    mock_server_instance = Mock()
    mock_server_instance.run = AsyncMock()
    mock_server_instance.get_capabilities.return_value = ServerCapabilities(
        logging=None, prompts=None, resources=None, tools=None
    )

    return mock_db_instance, mock_server_instance
