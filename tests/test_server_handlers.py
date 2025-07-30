"""Unit tests for server tool handlers"""

from datetime import date, datetime
import pytest
import json
import mcp.types as types
from pydantic import AnyUrl

from src.mcp_snowflake_server.server import (
    handle_list_databases,
    handle_list_schemas,
    handle_list_tables,
    handle_describe_table,
    handle_read_query,
    handle_append_insight,
    handle_write_query,
    handle_create_table,
    data_to_yaml,
    data_json_serializer,
    handle_tool_errors
)


class TestServerHandlers:
    """Test cases for server tool handlers"""

    @pytest.mark.asyncio
    async def test_handle_list_databases_success(self, mock_db_client, sample_query_results):
        """Test successful database listing"""
        mock_db_client.execute_query.return_value = (sample_query_results, "test-data-id")

        result = await handle_list_databases({}, mock_db_client)

        assert len(result) == 2
        assert isinstance(result[0], types.TextContent)
        assert isinstance(result[1], types.EmbeddedResource)

        # Check YAML content
        yaml_content = result[0].text
        assert "TEST_DB1" in yaml_content
        assert "TEST_DB2" in yaml_content

        # Check embedded resource
        resource = result[1].resource
        assert str(resource.uri) == "data://test-data-id"
        assert resource.mimeType == "application/json"

    @pytest.mark.asyncio
    async def test_handle_list_databases_with_exclusions(self, mock_db_client, sample_query_results, exclusion_config):
        """Test database listing with exclusion patterns"""
        mock_db_client.execute_query.return_value = (sample_query_results, "test-data-id")

        result = await handle_list_databases({}, mock_db_client, exclusion_config=exclusion_config)

        # Parse the JSON from embedded resource to check filtering
        resource_content = json.loads(result[1].resource.text)
        database_names = [item["DATABASE_NAME"] for item in resource_content["data"]]

        # Should exclude "EXCLUDED_DB" based on exclusion pattern
        assert "TEST_DB1" in database_names
        assert "TEST_DB2" in database_names
        assert "EXCLUDED_DB" not in database_names

    @pytest.mark.asyncio
    async def test_handle_list_schemas_success(self, mock_db_client, sample_query_results):
        """Test successful schema listing"""
        mock_db_client.execute_query.return_value = (sample_query_results, "test-data-id")
        arguments = {"database": "TEST_DB"}

        result = await handle_list_schemas(arguments, mock_db_client)

        assert len(result) == 2
        assert isinstance(result[0], types.TextContent)
        assert isinstance(result[1], types.EmbeddedResource)

        # Check that database name is included in output
        resource_content = json.loads(result[1].resource.text)
        assert resource_content["database"] == "TEST_DB"

    @pytest.mark.asyncio
    async def test_handle_list_schemas_missing_database(self, mock_db_client):
        """Test schema listing with missing database parameter"""
        with pytest.raises(ValueError, match="Missing required 'database' parameter"):
            await handle_list_schemas({}, mock_db_client)

        with pytest.raises(ValueError, match="Missing required 'database' parameter"):
            await handle_list_schemas(None, mock_db_client)

    @pytest.mark.asyncio
    async def test_handle_list_tables_success(self, mock_db_client, sample_table_results):
        """Test successful table listing"""
        mock_db_client.execute_query.return_value = (sample_table_results, "test-data-id")
        arguments = {"database": "TEST_DB", "schema": "PUBLIC"}

        result = await handle_list_tables(arguments, mock_db_client)

        assert len(result) == 2
        assert isinstance(result[0], types.TextContent)
        assert isinstance(result[1], types.EmbeddedResource)

        # Check that database and schema are included in output
        resource_content = json.loads(result[1].resource.text)
        assert resource_content["database"] == "TEST_DB"
        assert resource_content["schema"] == "PUBLIC"

    @pytest.mark.asyncio
    async def test_handle_list_tables_missing_parameters(self, mock_db_client):
        """Test table listing with missing parameters"""
        with pytest.raises(ValueError, match="Missing required 'database' and 'schema' parameters"):
            await handle_list_tables({}, mock_db_client)

        with pytest.raises(ValueError, match="Missing required 'database' and 'schema' parameters"):
            await handle_list_tables({"database": "TEST_DB"}, mock_db_client)

    @pytest.mark.asyncio
    async def test_handle_describe_table_success(self, mock_db_client, sample_column_results):
        """Test successful table description"""
        mock_db_client.execute_query.return_value = (sample_column_results, "test-data-id")
        arguments = {"table_name": "TEST_DB.PUBLIC.USERS"}

        result = await handle_describe_table(arguments, mock_db_client)

        assert len(result) == 2
        assert isinstance(result[0], types.TextContent)
        assert isinstance(result[1], types.EmbeddedResource)

        # Check that table components are parsed correctly
        resource_content = json.loads(result[1].resource.text)
        assert resource_content["database"] == "TEST_DB"
        assert resource_content["schema"] == "PUBLIC"
        assert resource_content["table"] == "USERS"

    @pytest.mark.asyncio
    async def test_handle_describe_table_invalid_format(self, mock_db_client):
        """Test table description with invalid table name format"""
        with pytest.raises(ValueError, match="Missing table_name argument"):
            await handle_describe_table({}, mock_db_client)

        with pytest.raises(ValueError, match="Table name must be fully qualified"):
            await handle_describe_table({"table_name": "invalid_format"}, mock_db_client)

    @pytest.mark.asyncio
    async def test_handle_read_query_success(self, mock_db_client, write_detector):
        """Test successful read query execution"""
        query_results = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
        mock_db_client.execute_query.return_value = (query_results, "test-data-id")
        arguments = {"query": "SELECT * FROM users"}

        result = await handle_read_query(arguments, mock_db_client, write_detector)

        assert len(result) == 2
        assert isinstance(result[0], types.TextContent)
        assert isinstance(result[1], types.EmbeddedResource)

    @pytest.mark.asyncio
    async def test_handle_read_query_with_write_operation(self, mock_db_client, write_detector):
        """Test read query handler rejecting write operations"""
        arguments = {"query": "INSERT INTO users VALUES (1, 'John')"}

        with pytest.raises(ValueError, match="Calls to read_query should not contain write operations"):
            await handle_read_query(arguments, mock_db_client, write_detector)

    @pytest.mark.asyncio
    async def test_handle_read_query_missing_query(self, mock_db_client, write_detector):
        """Test read query handler with missing query parameter"""
        with pytest.raises(ValueError, match="Missing query argument"):
            await handle_read_query({}, mock_db_client, write_detector)

    @pytest.mark.asyncio
    async def test_handle_append_insight_success(self, mock_db_client, mock_server):
        """Test successful insight appending"""
        arguments = {"insight": "Users table has grown by 20%"}

        result = await handle_append_insight(arguments, mock_db_client, None, None, mock_server)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].text == "Insight added to memo"

        # Verify insight was added to database
        assert "Users table has grown by 20%" in mock_db_client.insights

        # Verify resource update notification was sent
        mock_server.request_context.session.send_resource_updated.assert_called_once_with(
            AnyUrl("memo://insights")
        )

    @pytest.mark.asyncio
    async def test_handle_append_insight_missing_insight(self, mock_db_client, mock_server):
        """Test insight appending with missing insight parameter"""
        with pytest.raises(ValueError, match="Missing insight argument"):
            await handle_append_insight({}, mock_db_client, None, None, mock_server)

    @pytest.mark.asyncio
    async def test_handle_write_query_success(self, mock_db_client):
        """Test successful write query execution"""
        mock_db_client.execute_query.return_value = ("Query executed successfully", "test-data-id")
        arguments = {"query": "INSERT INTO users VALUES (1, 'John')"}
        allow_write = True

        result = await handle_write_query(arguments, mock_db_client, None, allow_write, None)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Query executed successfully" in result[0].text

    @pytest.mark.asyncio
    async def test_handle_write_query_not_allowed(self, mock_db_client):
        """Test write query when write operations are not allowed"""
        arguments = {"query": "INSERT INTO users VALUES (1, 'John')"}
        allow_write = False

        with pytest.raises(ValueError, match="Write operations are not allowed"):
            await handle_write_query(arguments, mock_db_client, None, allow_write, None)

    @pytest.mark.asyncio
    async def test_handle_write_query_select_not_allowed(self, mock_db_client):
        """Test write query handler rejecting SELECT queries"""
        arguments = {"query": "SELECT * FROM users"}
        allow_write = True

        with pytest.raises(ValueError, match="SELECT queries are not allowed for write_query"):
            await handle_write_query(arguments, mock_db_client, None, allow_write, None)

    @pytest.mark.asyncio
    async def test_handle_create_table_success(self, mock_db_client):
        """Test successful table creation"""
        mock_db_client.execute_query.return_value = ("Table created", "test-data-id")
        arguments = {"query": "CREATE TABLE test_table (id INT, name VARCHAR(100))"}
        allow_write = True

        result = await handle_create_table(arguments, mock_db_client, None, allow_write, None)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Table created successfully" in result[0].text
        assert "test-data-id" in result[0].text

    @pytest.mark.asyncio
    async def test_handle_create_table_not_allowed(self, mock_db_client):
        """Test table creation when write operations are not allowed"""
        arguments = {"query": "CREATE TABLE test_table (id INT)"}
        allow_write = False

        with pytest.raises(ValueError, match="Write operations are not allowed"):
            await handle_create_table(arguments, mock_db_client, None, allow_write, None)

    @pytest.mark.asyncio
    async def test_handle_create_table_invalid_query(self, mock_db_client):
        """Test table creation with invalid query"""
        arguments = {"query": "DROP TABLE test_table"}
        allow_write = True

        with pytest.raises(ValueError, match="Only CREATE TABLE statements are allowed"):
            await handle_create_table(arguments, mock_db_client, None, allow_write, None)


class TestUtilityFunctions:
    """Test cases for utility functions"""

    def test_data_to_yaml(self):
        """Test YAML conversion function"""
        data = {"key": "value", "number": 42, "list": [1, 2, 3]}
        yaml_output = data_to_yaml(data)

        assert "key: value" in yaml_output
        assert "number: 42" in yaml_output
        assert "list:" in yaml_output

    def test_data_json_serializer_with_date(self):
        """Test JSON serializer with date objects"""
        test_date = date(2023, 1, 1)
        test_datetime = datetime(2023, 1, 1, 12, 0, 0)

        assert data_json_serializer(test_date) == "2023-01-01"
        assert data_json_serializer(test_datetime) == "2023-01-01T12:00:00"
        assert data_json_serializer("regular_string") == "regular_string"
        assert data_json_serializer(42) == 42

    @pytest.mark.asyncio
    async def test_handle_tool_errors_decorator(self):
        """Test the error handling decorator"""

        @handle_tool_errors
        async def failing_function():
            raise Exception("Test error")

        @handle_tool_errors
        async def successful_function():
            return [types.TextContent(type="text", text="Success")]

        # Test error case
        error_result = await failing_function()
        assert len(error_result) == 1
        assert isinstance(error_result[0], types.TextContent)
        assert "Error: Test error" in error_result[0].text

        # Test success case
        success_result = await successful_function()
        assert len(success_result) == 1
        assert success_result[0].text == "Success"


class TestExclusionFiltering:
    """Test cases for exclusion pattern filtering"""

    @pytest.mark.asyncio
    async def test_database_exclusion_filtering(self, mock_db_client):
        """Test database exclusion filtering"""
        sample_data = [
            {"DATABASE_NAME": "PROD_DB"},
            {"DATABASE_NAME": "TEST_DB"},
            {"DATABASE_NAME": "TEMP_DATABASE"},
            {"DATABASE_NAME": "STAGING_DB"}
        ]
        mock_db_client.execute_query.return_value = (sample_data, "test-id")

        exclusion_config = {"databases": ["temp", "staging"]}

        result = await handle_list_databases({}, mock_db_client, exclusion_config=exclusion_config)
        resource_content = json.loads(result[1].resource.text)
        database_names = [item["DATABASE_NAME"] for item in resource_content["data"]]

        assert "PROD_DB" in database_names
        assert "TEST_DB" in database_names
        assert "TEMP_DATABASE" not in database_names  # Contains "temp"
        assert "STAGING_DB" not in database_names  # Contains "staging"

    @pytest.mark.asyncio
    async def test_schema_exclusion_filtering(self, mock_db_client):
        """Test schema exclusion filtering"""
        sample_data = [
            {"SCHEMA_NAME": "PUBLIC"},
            {"SCHEMA_NAME": "PRIVATE"},
            {"SCHEMA_NAME": "TEMP_SCHEMA"},
            {"SCHEMA_NAME": "INFORMATION_SCHEMA"}
        ]
        mock_db_client.execute_query.return_value = (sample_data, "test-id")

        exclusion_config = {"schemas": ["temp", "information"]}
        arguments = {"database": "TEST_DB"}

        result = await handle_list_schemas(arguments, mock_db_client, exclusion_config=exclusion_config)
        resource_content = json.loads(result[1].resource.text)
        schema_names = [item["SCHEMA_NAME"] for item in resource_content["data"]]

        assert "PUBLIC" in schema_names
        assert "PRIVATE" in schema_names
        assert "TEMP_SCHEMA" not in schema_names  # Contains "temp"
        assert "INFORMATION_SCHEMA" not in schema_names  # Contains "information"

    @pytest.mark.asyncio
    async def test_table_exclusion_filtering(self, mock_db_client):
        """Test table exclusion filtering"""
        sample_data = [
            {"TABLE_NAME": "USERS", "TABLE_CATALOG": "DB", "TABLE_SCHEMA": "PUBLIC", "COMMENT": ""},
            {"TABLE_NAME": "ORDERS", "TABLE_CATALOG": "DB", "TABLE_SCHEMA": "PUBLIC", "COMMENT": ""},
            {"TABLE_NAME": "TEMP_TABLE", "TABLE_CATALOG": "DB", "TABLE_SCHEMA": "PUBLIC", "COMMENT": ""},
            {"TABLE_NAME": "STAGING_DATA", "TABLE_CATALOG": "DB", "TABLE_SCHEMA": "PUBLIC", "COMMENT": ""}
        ]
        mock_db_client.execute_query.return_value = (sample_data, "test-id")

        exclusion_config = {"tables": ["temp", "staging"]}
        arguments = {"database": "TEST_DB", "schema": "PUBLIC"}

        result = await handle_list_tables(arguments, mock_db_client, exclusion_config=exclusion_config)
        resource_content = json.loads(result[1].resource.text)
        table_names = [item["TABLE_NAME"] for item in resource_content["data"]]

        assert "USERS" in table_names
        assert "ORDERS" in table_names
        assert "TEMP_TABLE" not in table_names  # Contains "temp"
        assert "STAGING_DATA" not in table_names  # Contains "staging"
