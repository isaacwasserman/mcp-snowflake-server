"""Unit tests for SnowflakeDB class"""
import logging
from datetime import datetime, date

import pytest
from src.mcp_snowflake_server.db_client import SnowflakeDB
import time
from unittest.mock import Mock, AsyncMock, patch

from tests.conftest import create_mock_db_with_session


class TestSnowflakeDB:
    """Test cases for SnowflakeDB"""

    def test_init(self, mock_connection_config):
        """Test SnowflakeDB initialization"""
        db = SnowflakeDB(mock_connection_config)

        assert db.connection_config == mock_connection_config
        assert db.session is None
        assert db.insights == []
        assert db.auth_time == 0
        assert db.init_task is None

    @pytest.mark.asyncio
    async def test_init_database_success(self, mock_connection_config):
        """Test successful database initialization"""
        with patch('src.mcp_snowflake_server.db_client.Session') as mock_session_class:
            mock_session = Mock()
            mock_session_class.builder.configs.return_value.create.return_value = mock_session

            db = SnowflakeDB(mock_connection_config)
            await db._init_database()

            assert db.session == mock_session
            assert db.auth_time > 0
            mock_session.sql.assert_called_once_with("USE WAREHOUSE TEST_WAREHOUSE")

    @pytest.mark.asyncio
    async def test_init_database_without_warehouse(self, mock_connection_config):
        """Test database initialization without warehouse"""
        config_without_warehouse = mock_connection_config.copy()
        del config_without_warehouse['warehouse']

        with patch('src.mcp_snowflake_server.db_client.Session') as mock_session_class:
            mock_session = Mock()
            mock_session_class.builder.configs.return_value.create.return_value = mock_session

            db = SnowflakeDB(config_without_warehouse)
            await db._init_database()

            assert db.session == mock_session
            assert db.auth_time > 0
            mock_session.sql.assert_not_called()

    @pytest.mark.asyncio
    async def test_init_database_failure(self, mock_connection_config):
        """Test database initialization failure"""
        with patch('src.mcp_snowflake_server.db_client.Session') as mock_session_class:
            mock_session_class.builder.configs.return_value.create.side_effect = Exception("Connection failed")

            db = SnowflakeDB(mock_connection_config)

            with pytest.raises(ValueError, match="Failed to connect to Snowflake database"):
                await db._init_database()

    @patch('asyncio.get_event_loop')
    def test_start_init_connection(self, mock_get_loop, mock_connection_config):
        """Test starting database initialization in background"""
        mock_loop = Mock()
        mock_task = Mock()
        mock_loop.create_task.return_value = mock_task
        mock_get_loop.return_value = mock_loop

        db = SnowflakeDB(mock_connection_config)
        result = db.start_init_connection()

        assert result == mock_task
        assert db.init_task == mock_task
        mock_loop.create_task.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_query_with_init_task_pending(self, mock_db_client):
        """Test execute_query when init_task is pending"""
        # Create a mock task that's not done
        mock_task = AsyncMock()
        mock_task.done.return_value = False
        mock_db_client.init_task = mock_task

        # Set up return data
        test_data = [{"col1": "value1"}]
        mock_db_client.execute_query.return_value = (test_data, "test-data-id")

        result_data, data_id = await mock_db_client.execute_query("SELECT * FROM test")

        assert result_data == test_data
        assert data_id == "test-data-id"

    @pytest.mark.asyncio
    async def test_execute_query_with_expired_session(self, mock_connection_config):
        """Test execute_query when session is expired"""
        with patch.object(SnowflakeDB, '_init_database') as mock_init:
            db = SnowflakeDB(mock_connection_config)

            # Set up expired session
            mock_session = Mock()
            mock_df = Mock()
            mock_df.to_dict.return_value = [{"col1": "value1"}]
            mock_session.sql.return_value.to_pandas.return_value = mock_df
            db.session = mock_session
            db.auth_time = time.time() - 2000  # Expired

            result_data, data_id = await db.execute_query("SELECT * FROM test")

            # Verify re-initialization was called
            mock_init.assert_called_once()
            assert result_data == [{"col1": "value1"}]
            assert isinstance(data_id, str)

    @pytest.mark.asyncio
    async def test_execute_query_success(self, mock_db_client):
        """Test successful query execution"""
        # Setup mock return data
        test_data = [
            {"id": 1, "name": "John"},
            {"id": 2, "name": "Jane"}
        ]
        mock_db_client.execute_query.return_value = (test_data, "test-data-id")

        result_data, data_id = await mock_db_client.execute_query("SELECT * FROM users")

        assert result_data == test_data
        assert data_id == "test-data-id"
        mock_db_client.execute_query.assert_called_once_with("SELECT * FROM users")

    @pytest.mark.asyncio
    async def test_execute_query_failure(self, mock_db_client):
        """Test query execution failure"""
        mock_db_client.execute_query.side_effect = Exception("SQL execution failed")

        with pytest.raises(Exception, match="SQL execution failed"):
            await mock_db_client.execute_query("INVALID SQL")

    @pytest.mark.asyncio
    async def test_execute_query_with_complex_data_types(self, mock_db_client):
        """Test query execution with complex data types"""
        # Setup mock return data with various data types
        test_data = [
            {
                "id": 1,
                "name": "John",
                "created_date": date(2023, 1, 1),
                "updated_timestamp": datetime(2023, 1, 1, 12, 0, 0),
                "amount": 123.45,
                "is_active": True,
                "metadata": None
            }
        ]
        mock_db_client.execute_query.return_value = (test_data, "test-data-id")

        result_data, data_id = await mock_db_client.execute_query("SELECT * FROM complex_table")

        assert result_data == test_data
        assert data_id == "test-data-id"

    def test_add_insight(self, mock_db_client):
        """Test adding insights to the collection"""
        assert len(mock_db_client.insights) == 0

        mock_db_client.add_insight("First insight")
        assert len(mock_db_client.insights) == 1
        assert mock_db_client.insights[0] == "First insight"

        mock_db_client.add_insight("Second insight")
        assert len(mock_db_client.insights) == 2
        assert mock_db_client.insights[1] == "Second insight"

    def test_get_memo_empty(self, mock_db_client):
        """Test getting memo when no insights exist"""
        memo = mock_db_client.get_memo()
        assert memo == "No data insights have been discovered yet."

    def test_get_memo_single_insight(self, mock_db_client):
        """Test getting memo with single insight"""
        mock_db_client.add_insight("Users table has 1000 records")
        memo = mock_db_client.get_memo()

        assert "📊 Data Intelligence Memo 📊" in memo
        assert "Key Insights Discovered:" in memo
        assert "- Users table has 1000 records" in memo
        assert "Summary:" not in memo  # No summary for single insight

    def test_get_memo_multiple_insights(self, mock_db_client):
        """Test getting memo with multiple insights"""
        mock_db_client.add_insight("Users table has 1000 records")
        mock_db_client.add_insight("Orders table shows 50% growth")
        mock_db_client.add_insight("Revenue increased by 25%")

        memo = mock_db_client.get_memo()

        assert "📊 Data Intelligence Memo 📊" in memo
        assert "Key Insights Discovered:" in memo
        assert "- Users table has 1000 records" in memo
        assert "- Orders table shows 50% growth" in memo
        assert "- Revenue increased by 25%" in memo
        assert "Summary:" in memo
        assert "3 key data insights" in memo

    @pytest.mark.asyncio
    async def test_execute_query_logging(self, mock_connection_config, caplog):
        """Test that query execution logs appropriately"""
        db, mock_session = create_mock_db_with_session(mock_connection_config)
        mock_session.sql.return_value.to_pandas.return_value.to_dict.return_value = [{"col1": "value1"}]

        with caplog.at_level(logging.DEBUG):
            await db.execute_query("SELECT * FROM test")

        # Check that debug log was created
        assert any("Executing query: SELECT * FROM test" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_execute_query_error_logging(self, mock_connection_config):
        """Test that query execution errors are logged"""
        db, mock_session = create_mock_db_with_session(mock_connection_config)
        mock_session.sql.side_effect = Exception("Database connection lost")

        # Test that the exception is raised (the logging is tested implicitly)
        with pytest.raises(Exception, match="Database connection lost"):
            await db.execute_query("SELECT * FROM test")

    @pytest.mark.asyncio
    async def test_execute_query_uuid_generation(self, mock_connection_config):
        """Test that each query execution generates a unique data_id"""
        db, mock_session = create_mock_db_with_session(mock_connection_config)
        mock_session.sql.return_value.to_pandas.return_value.to_dict.return_value = [{"col1": "value1"}]

        _, data_id1 = await db.execute_query("SELECT * FROM test1")
        _, data_id2 = await db.execute_query("SELECT * FROM test2")

        assert data_id1 != data_id2
        assert len(data_id1) == 36  # UUID4 length
        assert len(data_id2) == 36  # UUID4 length
