"""Integration tests for the main server functionality"""

import pytest
import json
import logging
from unittest.mock import mock_open, Mock, AsyncMock, patch

import mcp.types as types
from pydantic import AnyUrl
from src.mcp_snowflake_server.server import main, prefetch_tables

from tests.conftest import setup_server_integration_mocks, mock_connection_config


class TestServerIntegration:
    """Integration test cases for the main server functionality"""

    @pytest.mark.asyncio
    async def test_prefetch_tables_success(self, mock_db_client):
        """Test successful table prefetching"""
        # Mock table results
        table_results = [
            {"TABLE_NAME": "USERS", "COMMENT": "User data"},
            {"TABLE_NAME": "ORDERS", "COMMENT": "Order data"}
        ]

        # Mock column results
        column_results = [
            {"TABLE_NAME": "USERS", "COLUMN_NAME": "ID", "DATA_TYPE": "NUMBER", "COMMENT": "Primary key"},
            {"TABLE_NAME": "USERS", "COLUMN_NAME": "NAME", "DATA_TYPE": "VARCHAR", "COMMENT": "User name"},
            {"TABLE_NAME": "ORDERS", "COLUMN_NAME": "ID", "DATA_TYPE": "NUMBER", "COMMENT": "Order ID"},
            {"TABLE_NAME": "ORDERS", "COLUMN_NAME": "USER_ID", "DATA_TYPE": "NUMBER", "COMMENT": "Foreign key"}
        ]

        # Setup mock to return different results for different queries
        def mock_execute_query(query):
            if "information_schema.tables" in query.lower():
                return table_results, "table-data-id"
            elif "information_schema.columns" in query.lower():
                return column_results, "column-data-id"
            return [], "default-id"

        mock_db_client.execute_query.side_effect = mock_execute_query

        credentials = {"database": "TEST_DB", "schema": "PUBLIC"}
        result = await prefetch_tables(mock_db_client, credentials)

        expected = {"TABLE_NAME": "USERS", "COMMENT": "User data",
                    "COLUMNS": {'ID': {'COLUMN_NAME': 'ID', 'COMMENT': 'Primary key', 'DATA_TYPE': 'NUMBER'},
                                'NAME': {'COLUMN_NAME': 'NAME', 'COMMENT': 'User name', 'DATA_TYPE': 'VARCHAR'}}}
        # Verify users table
        assert result["USERS"] == expected

    @pytest.mark.asyncio
    async def test_prefetch_tables_error(self, mock_db_client, caplog):
        """Test table prefetching with database error"""
        mock_db_client.execute_query.side_effect = Exception("Database connection failed")

        credentials = {"database": "TEST_DB", "schema": "PUBLIC"}

        with caplog.at_level(logging.ERROR):
            result = await prefetch_tables(mock_db_client, credentials)

        # Should return error message as string
        assert isinstance(result, str)
        assert "Error prefetching table descriptions" in result
        assert "Database connection failed" in result

        # Check error was logged
        assert any("Error prefetching table descriptions" in record.message for record in caplog.records)

    @patch('src.mcp_snowflake_server.server.mcp.server.stdio.stdio_server')
    @patch('src.mcp_snowflake_server.server.SnowflakeDB')
    @pytest.mark.asyncio
    async def test_main_function_basic_setup(self, mock_snowflake_db_class, mock_stdio_server, mock_connection_config):
        """Test main function basic setup and configuration"""
        mock_db_instance, mock_server_instance = setup_server_integration_mocks(
            mock_snowflake_db_class, mock_stdio_server
        )

        with patch('src.mcp_snowflake_server.server.Server') as mock_server_class:
            mock_server_class.return_value = mock_server_instance

            # Call main function
            await main(
                allow_write=False,
                connection_args=mock_connection_config,
                log_level="DEBUG",
                exclude_tools=["write_query"],
                prefetch=False
            )

            # Verify database initialization
            mock_snowflake_db_class.assert_called_once_with(mock_connection_config)
            mock_db_instance.start_init_connection.assert_called_once()

            # Verify server was created and run
            mock_server_class.assert_called_once_with("snowflake-manager")
            mock_server_instance.run.assert_called_once()

    @patch('src.mcp_snowflake_server.server.mcp.server.stdio.stdio_server')
    @patch('src.mcp_snowflake_server.server.SnowflakeDB')
    @pytest.mark.asyncio
    async def test_main_function_with_prefetch(self, mock_snowflake_db_class, mock_stdio_server, mock_connection_config):
        """Test main function with table prefetching enabled"""
        mock_db_instance, mock_server_instance = setup_server_integration_mocks(
            mock_snowflake_db_class, mock_stdio_server
        )

        # Mock prefetch_tables function
        with patch('src.mcp_snowflake_server.server.prefetch_tables') as mock_prefetch:
            mock_prefetch.return_value = {"USERS": {"TABLE_NAME": "USERS", "COLUMNS": {}}}

            with patch('src.mcp_snowflake_server.server.Server') as mock_server_class:
                mock_server_class.return_value = mock_server_instance

                await main(
                    allow_write=True,
                    connection_args=mock_connection_config,
                    prefetch=True
                )

                # Verify prefetch was called
                mock_prefetch.assert_called_once_with(mock_db_instance, mock_connection_config)

    @patch('src.mcp_snowflake_server.server.mcp.server.stdio.stdio_server')
    @patch('src.mcp_snowflake_server.server.SnowflakeDB')
    @pytest.mark.asyncio
    async def test_main_function_with_config_file(self, mock_snowflake_db_class, mock_stdio_server):
        """Test main function with configuration file loading"""
        mock_db_instance, mock_server_instance = setup_server_integration_mocks(
            mock_snowflake_db_class, mock_stdio_server
        )

        # Mock config file content
        config_content = {
            "exclude_patterns": {
                "databases": ["temp"],
                "schemas": ["staging"],
                "tables": ["test"]
            }
        }

        with patch('builtins.open', mock_open_with_content(json.dumps(config_content))):
            with patch('src.mcp_snowflake_server.server.Server') as mock_server_class:
                mock_server_class.return_value = mock_server_instance

                connection_args = {"database": "test_db"}

                await main(
                    connection_args=connection_args,
                    config_file="test_config.json"
                )

                # Verify server was created (config loading is internal)
                mock_server_class.assert_called_once_with("snowflake-manager")

    @patch('src.mcp_snowflake_server.server.mcp.server.stdio.stdio_server')
    @patch('src.mcp_snowflake_server.server.SnowflakeDB')
    @pytest.mark.asyncio
    async def test_main_function_stdio_error(self, mock_snowflake_db_class, mock_stdio_server):
        """Test main function handling stdio server errors"""
        # Setup mocks
        mock_db_instance = Mock()
        mock_snowflake_db_class.return_value = mock_db_instance

        # Make stdio_server raise an exception
        mock_stdio_server.side_effect = Exception("Failed to start stdio server")

        connection_args = {"database": "test_db"}

        with pytest.raises(Exception, match="Failed to start stdio server"):
            await main(connection_args=connection_args)


    @patch('src.mcp_snowflake_server.server.mcp.server.stdio.stdio_server')
    @patch('src.mcp_snowflake_server.server.SnowflakeDB')
    @pytest.mark.asyncio
    async def test_main_function_tool_filtering(self, mock_snowflake_db_class, mock_stdio_server):
        """Test main function tool filtering based on allow_write and exclude_tools"""
        mock_db_instance, mock_server_instance = setup_server_integration_mocks(
            mock_snowflake_db_class, mock_stdio_server
        )

        # Capture the list_tools handler
        list_tools_handler = None

        def capture_list_tools():
            def decorator(func):
                nonlocal list_tools_handler
                list_tools_handler = func
                return func

            return decorator

        mock_server_instance.list_tools = capture_list_tools

        with patch('src.mcp_snowflake_server.server.Server') as mock_server_class:
            mock_server_class.return_value = mock_server_instance

            connection_args = {"database": "test_db"}

            await main(
                allow_write=False,  # Should exclude write tools
                connection_args=connection_args,
                exclude_tools=["describe_table"]  # Should exclude this tool
            )

            # Test the list_tools handler
            tool_names = {}
            if list_tools_handler:
                tools = await list_tools_handler()
                tool_names = {tool.name for tool in tools}

            # Should include read-only tools
            assert tool_names == {'list_databases', 'list_schemas', 'list_tables', 'read_query', 'append_insight'}


class TestServerResourceHandlers:
    """Test cases for server resource handlers"""

    @patch('src.mcp_snowflake_server.server.mcp.server.stdio.stdio_server')
    @patch('src.mcp_snowflake_server.server.SnowflakeDB')
    @pytest.mark.asyncio
    async def test_resource_handlers_setup(self, mock_snowflake_db_class, mock_stdio_server):
        """Test that resource handlers are properly set up"""
        mock_db_instance, mock_server_instance = setup_server_integration_mocks(
            mock_snowflake_db_class, mock_stdio_server
        )
        mock_db_instance.get_memo.return_value = "Test memo content"

        # Capture resource handlers
        list_resources_handler = None
        read_resource_handler = None

        def capture_list_resources():
            def decorator(func):
                nonlocal list_resources_handler
                list_resources_handler = func
                return func

            return decorator

        def capture_read_resource():
            def decorator(func):
                nonlocal read_resource_handler
                read_resource_handler = func
                return func

            return decorator

        mock_server_instance.list_resources = capture_list_resources
        mock_server_instance.read_resource = capture_read_resource

        with patch('src.mcp_snowflake_server.server.Server') as mock_server_class:
            mock_server_class.return_value = mock_server_instance

            connection_args = {"database": "test_db"}

            await main(connection_args=connection_args, prefetch=False)

            # Test list_resources handler
            if list_resources_handler:
                resources = await list_resources_handler()
                assert len(resources) >= 1

                # Should include memo resource
                memo_resource = next((r for r in resources if str(r.uri) == "memo://insights"), None)
                assert memo_resource is not None
                assert memo_resource.name == "Data Insights Memo"
                assert memo_resource.mimeType == "text/plain"

            # Test read_resource handler
            if read_resource_handler:
                # Test memo resource
                memo_content = await read_resource_handler(AnyUrl("memo://insights"))
                assert memo_content == "Test memo content"

                # Test unknown resource
                with pytest.raises(ValueError, match="Unknown resource"):
                    await read_resource_handler(AnyUrl("unknown://resource"))


def mock_open_with_content(content):
    """Helper function to create a mock open that returns specific content"""
    return mock_open(read_data=content)


class TestServerToolHandlerIntegration:
    """Integration tests for server tool handler registration"""

    @patch('src.mcp_snowflake_server.server.mcp.server.stdio.stdio_server')
    @patch('src.mcp_snowflake_server.server.SnowflakeDB')
    @pytest.mark.asyncio
    async def test_call_tool_handler_integration(self, mock_snowflake_db_class, mock_stdio_server):
        """Test that call_tool handler is properly integrated"""
        mock_db_instance, mock_server_instance = setup_server_integration_mocks(
            mock_snowflake_db_class, mock_stdio_server
        )
        mock_db_instance.execute_query = AsyncMock(return_value=([{"DATABASE_NAME": "TEST_DB"}], "test-id"))

        # Capture call_tool handler
        call_tool_handler = None

        def capture_call_tool():
            def decorator(func):
                nonlocal call_tool_handler
                call_tool_handler = func
                return func

            return decorator

        mock_server_instance.call_tool = capture_call_tool

        with patch('src.mcp_snowflake_server.server.Server') as mock_server_class:
            mock_server_class.return_value = mock_server_instance

            connection_args = {"database": "test_db"}

            await main(connection_args=connection_args)

            # Test call_tool handler
            if call_tool_handler:
                # Test valid tool call
                result = await call_tool_handler("list_databases", {})
                assert len(result) == 2  # YAML content + embedded resource
                assert isinstance(result[0], types.TextContent)
                assert isinstance(result[1], types.EmbeddedResource)

                # Test unknown tool - the error decorator catches exceptions and returns error text
                result = await call_tool_handler("unknown_tool", {})
                assert len(result) == 1
                assert isinstance(result[0], types.TextContent)
                assert "Error: Unknown tool: unknown_tool" in result[0].text

    @patch('src.mcp_snowflake_server.server.mcp.server.stdio.stdio_server')
    @patch('src.mcp_snowflake_server.server.SnowflakeDB')
    @pytest.mark.asyncio
    async def test_exclusion_config_integration(self, mock_snowflake_db_class, mock_stdio_server):
        """Test that exclusion configuration is properly integrated"""
        mock_db_instance, mock_server_instance = setup_server_integration_mocks(
            mock_snowflake_db_class, mock_stdio_server
        )
        mock_db_instance.execute_query = AsyncMock(
            return_value=(
                [{"DATABASE_NAME": "PROD_DB"}, {"DATABASE_NAME": "TEMP_DB"}],
                "test-id",
            )
        )

        # Capture call_tool handler
        call_tool_handler = None

        def capture_call_tool():
            def decorator(func):
                nonlocal call_tool_handler
                call_tool_handler = func
                return func

            return decorator

        mock_server_instance.call_tool = capture_call_tool

        with patch('src.mcp_snowflake_server.server.Server') as mock_server_class:
            mock_server_class.return_value = mock_server_instance

            connection_args = {"database": "test_db"}
            exclude_patterns = {"databases": ["temp"]}

            await main(
                connection_args=connection_args,
                exclude_patterns=exclude_patterns
            )

            # Test that exclusion patterns are applied
            if call_tool_handler:
                result = await call_tool_handler("list_databases", {})

                # Parse the embedded resource to check filtering
                resource_content = json.loads(result[1].resource.text)
                database_names = [item["DATABASE_NAME"] for item in resource_content["data"]]

                assert "PROD_DB" in database_names
                assert "TEMP_DB" not in database_names  # Should be excluded
