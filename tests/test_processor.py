"""Unit tests for Processor class"""

import pytest
from unittest.mock import Mock, AsyncMock
from src.mcp_snowflake_server.processor import Processor


class TestProcessor:
    """Test cases for Processor class"""

    def test_init(self, mock_db_client):
        """Test Processor initialization"""
        processor = Processor(mock_db_client)
        assert processor.db == mock_db_client

    @pytest.mark.asyncio
    async def test_retrieve_table_metadata(self, mock_db_client):
        """Test _retrieve_table_metadata method"""
        # Setup mock return data
        expected_tables = [
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'USERS',
                'TABLE_TYPE': 'BASE TABLE',
                'TABLE_OWNER': 'ADMIN',
                'TABLE_COMMENT': 'User information table'
            },
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'ORDERS',
                'TABLE_TYPE': 'BASE TABLE',
                'TABLE_OWNER': 'ADMIN',
                'TABLE_COMMENT': None
            }
        ]
        mock_db_client.execute_query.return_value = (expected_tables, "test-data-id")

        processor = Processor(mock_db_client)
        result = await processor._retrieve_table_metadata("TEST_DB")

        assert result == expected_tables
        mock_db_client.execute_query.assert_called_once()
        
        # Verify the SQL query structure
        call_args = mock_db_client.execute_query.call_args[0][0]
        assert "TEST_DB.INFORMATION_SCHEMA.TABLES" in call_args
        assert "TABLE_SCHEMA != 'INFORMATION_SCHEMA'" in call_args

    @pytest.mark.asyncio
    async def test_retrieve_column_metadata(self, mock_db_client):
        """Test _retrieve_column_metadata method"""
        # Setup mock return data
        expected_columns = [
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'USERS',
                'COLUMN_NAME': 'ID',
                'DATA_TYPE': 'NUMBER',
                'COLUMN_COMMENT': 'Primary key',
                'ORDINAL_POSITION': 1
            },
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'USERS',
                'COLUMN_NAME': 'NAME',
                'DATA_TYPE': 'VARCHAR',
                'COLUMN_COMMENT': 'User name',
                'ORDINAL_POSITION': 2
            }
        ]
        mock_db_client.execute_query.return_value = (expected_columns, "test-data-id")

        processor = Processor(mock_db_client)
        result = await processor._retrieve_column_metadata("TEST_DB")

        assert result == expected_columns
        mock_db_client.execute_query.assert_called_once()
        
        # Verify the SQL query structure
        call_args = mock_db_client.execute_query.call_args[0][0]
        assert "TEST_DB.INFORMATION_SCHEMA.COLUMNS" in call_args
        assert "ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION" in call_args

    @pytest.mark.asyncio
    async def test_retrieve_constraints(self, mock_db_client):
        """Test _retrieve_constraints method"""
        # Setup mock return data
        expected_constraints = [
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'USERS',
                'CONSTRAINT_NAME': 'PK_USERS',
                'CONSTRAINT_TYPE': 'PRIMARY KEY',
                'IS_ENFORCED': 'YES',
                'IS_DEFERRABLE': 'NO',
                'CONSTRAINT_COMMENT': 'Primary key constraint'
            }
        ]
        mock_db_client.execute_query.return_value = (expected_constraints, "test-data-id")

        processor = Processor(mock_db_client)
        result = await processor._retrieve_constraints("TEST_DB")

        assert result == expected_constraints
        mock_db_client.execute_query.assert_called_once()
        
        # Verify the SQL query structure
        call_args = mock_db_client.execute_query.call_args[0][0]
        assert "TEST_DB.INFORMATION_SCHEMA.TABLE_CONSTRAINTS" in call_args
        assert "ORDER BY TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME" in call_args


class TestProcessorStaticMethods:
    """Test cases for Processor static methods"""

    def test_process_tables_empty_input(self):
        """Test _process_tables with empty input"""
        schemas = {}
        tables = []
        
        Processor._process_tables(tables, schemas)
        
        assert schemas == {}

    def test_process_tables_single_table(self):
        """Test _process_tables with single table"""
        schemas = {}
        tables = [
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'USERS',
                'TABLE_TYPE': 'BASE TABLE',
                'TABLE_OWNER': 'ADMIN',
                'TABLE_COMMENT': 'User information table'
            }
        ]
        
        Processor._process_tables(tables, schemas)
        
        assert 'PUBLIC' in schemas
        assert 'tables' in schemas['PUBLIC']
        assert 'USERS' in schemas['PUBLIC']['tables']
        
        user_table = schemas['PUBLIC']['tables']['USERS']
        assert user_table['type'] == 'BASE TABLE'
        assert user_table['owner'] == 'ADMIN'
        assert user_table['comment'] == 'User information table'
        assert user_table['columns'] == []

    def test_process_tables_multiple_tables_multiple_schemas(self):
        """Test _process_tables with multiple tables across multiple schemas"""
        schemas = {}
        tables = [
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'USERS',
                'TABLE_TYPE': 'BASE TABLE',
                'TABLE_OWNER': 'ADMIN',
                'TABLE_COMMENT': 'User information table'
            },
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'ORDERS',
                'TABLE_TYPE': 'BASE TABLE',
                'TABLE_OWNER': 'ADMIN',
                'TABLE_COMMENT': None
            },
            {
                'TABLE_SCHEMA': 'ANALYTICS',
                'TABLE_NAME': 'REPORTS',
                'TABLE_TYPE': 'VIEW',
                'TABLE_OWNER': 'ANALYST',
                'TABLE_COMMENT': 'Analytics reports'
            }
        ]
        
        Processor._process_tables(tables, schemas)
        
        # Check PUBLIC schema
        assert 'PUBLIC' in schemas
        assert 'USERS' in schemas['PUBLIC']['tables']
        assert 'ORDERS' in schemas['PUBLIC']['tables']
        
        # Check ANALYTICS schema
        assert 'ANALYTICS' in schemas
        assert 'REPORTS' in schemas['ANALYTICS']['tables']
        
        # Verify table without comment
        orders_table = schemas['PUBLIC']['tables']['ORDERS']
        assert 'comment' not in orders_table
        
        # Verify view type
        reports_table = schemas['ANALYTICS']['tables']['REPORTS']
        assert reports_table['type'] == 'VIEW'

    def test_process_tables_existing_schema(self):
        """Test _process_tables with existing schema"""
        schemas = {
            'PUBLIC': {
                'tables': {
                    'EXISTING_TABLE': {
                        'type': 'BASE TABLE',
                        'owner': 'USER',
                        'columns': []
                    }
                }
            }
        }
        tables = [
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'NEW_TABLE',
                'TABLE_TYPE': 'VIEW',
                'TABLE_OWNER': 'ADMIN',
                'TABLE_COMMENT': 'New table'
            }
        ]
        
        Processor._process_tables(tables, schemas)
        
        # Verify existing table is preserved
        assert 'EXISTING_TABLE' in schemas['PUBLIC']['tables']
        # Verify new table is added
        assert 'NEW_TABLE' in schemas['PUBLIC']['tables']
        assert schemas['PUBLIC']['tables']['NEW_TABLE']['type'] == 'VIEW'

    def test_process_columns_empty_input(self):
        """Test _process_columns with empty input"""
        schema = {
            'PUBLIC': {
                'tables': {
                    'USERS': {
                        'type': 'BASE TABLE',
                        'owner': 'ADMIN',
                        'columns': []
                    }
                }
            }
        }
        columns = []
        
        Processor._process_columns(columns, schema)
        
        # Schema should remain unchanged
        assert schema['PUBLIC']['tables']['USERS']['columns'] == []

    def test_process_columns_single_column(self):
        """Test _process_columns with single column"""
        schema = {
            'PUBLIC': {
                'tables': {
                    'USERS': {
                        'type': 'BASE TABLE',
                        'owner': 'ADMIN',
                        'columns': []
                    }
                }
            }
        }
        columns = [
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'USERS',
                'COLUMN_NAME': 'ID',
                'DATA_TYPE': 'NUMBER',
                'COLUMN_COMMENT': 'Primary key'
            }
        ]
        
        Processor._process_columns(columns, schema)
        
        user_columns = schema['PUBLIC']['tables']['USERS']['columns']
        assert len(user_columns) == 1
        assert user_columns[0]['name'] == 'ID'
        assert user_columns[0]['type'] == 'NUMBER'
        assert user_columns[0]['comment'] == 'Primary key'

    def test_process_columns_multiple_columns(self):
        """Test _process_columns with multiple columns"""
        schema = {
            'PUBLIC': {
                'tables': {
                    'USERS': {
                        'type': 'BASE TABLE',
                        'owner': 'ADMIN',
                        'columns': []
                    }
                }
            }
        }
        columns = [
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'USERS',
                'COLUMN_NAME': 'ID',
                'DATA_TYPE': 'NUMBER',
                'COLUMN_COMMENT': 'Primary key'
            },
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'USERS',
                'COLUMN_NAME': 'NAME',
                'DATA_TYPE': 'VARCHAR',
                'COLUMN_COMMENT': None
            }
        ]
        
        Processor._process_columns(columns, schema)
        
        user_columns = schema['PUBLIC']['tables']['USERS']['columns']
        assert len(user_columns) == 2
        
        # Check first column
        assert user_columns[0]['name'] == 'ID'
        assert user_columns[0]['type'] == 'NUMBER'
        assert user_columns[0]['comment'] == 'Primary key'
        
        # Check second column (no comment)
        assert user_columns[1]['name'] == 'NAME'
        assert user_columns[1]['type'] == 'VARCHAR'
        assert 'comment' not in user_columns[1]

    def test_process_constraints_empty_input(self):
        """Test _process_constraints with empty input"""
        schema = {
            'PUBLIC': {
                'tables': {
                    'USERS': {
                        'type': 'BASE TABLE',
                        'owner': 'ADMIN',
                        'columns': []
                    }
                }
            }
        }
        constraints = []
        
        Processor._process_constraints(constraints, schema)
        
        # Schema should remain unchanged, no constraints key should be added
        assert 'constraints' not in schema['PUBLIC']['tables']['USERS']

    def test_process_constraints_single_constraint(self):
        """Test _process_constraints with single constraint"""
        schema = {
            'PUBLIC': {
                'tables': {
                    'USERS': {
                        'type': 'BASE TABLE',
                        'owner': 'ADMIN',
                        'columns': []
                    }
                }
            }
        }
        constraints = [
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'USERS',
                'CONSTRAINT_NAME': 'PK_USERS',
                'CONSTRAINT_TYPE': 'PRIMARY KEY',
                'CONSTRAINT_COMMENT': 'Primary key constraint',
                'IS_ENFORCED': 'YES',
                'IS_DEFERRABLE': 'NO'
            }
        ]
        
        Processor._process_constraints(constraints, schema)
        
        user_constraints = schema['PUBLIC']['tables']['USERS']['constraints']
        assert len(user_constraints) == 1
        assert user_constraints[0]['name'] == 'PK_USERS'
        assert user_constraints[0]['type'] == 'PRIMARY KEY'
        assert user_constraints[0]['comment'] == 'Primary key constraint'
        assert user_constraints[0]['is_enforced'] == 'YES'
        assert user_constraints[0]['is_deferrable'] == 'NO'

    def test_process_constraints_multiple_constraints(self):
        """Test _process_constraints with multiple constraints"""
        schema = {
            'PUBLIC': {
                'tables': {
                    'USERS': {
                        'type': 'BASE TABLE',
                        'owner': 'ADMIN',
                        'columns': []
                    }
                }
            }
        }
        constraints = [
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'USERS',
                'CONSTRAINT_NAME': 'PK_USERS',
                'CONSTRAINT_TYPE': 'PRIMARY KEY',
                'CONSTRAINT_COMMENT': 'Primary key constraint',
                'IS_ENFORCED': 'YES',
                'IS_DEFERRABLE': 'NO'
            },
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'USERS',
                'CONSTRAINT_NAME': 'UK_USERS_EMAIL',
                'CONSTRAINT_TYPE': 'UNIQUE',
                'CONSTRAINT_COMMENT': None,
                'IS_ENFORCED': None,
                'IS_DEFERRABLE': None
            }
        ]
        
        Processor._process_constraints(constraints, schema)
        
        user_constraints = schema['PUBLIC']['tables']['USERS']['constraints']
        assert len(user_constraints) == 2
        
        # Check first constraint
        assert user_constraints[0]['name'] == 'PK_USERS'
        assert user_constraints[0]['type'] == 'PRIMARY KEY'
        assert user_constraints[0]['comment'] == 'Primary key constraint'
        
        # Check second constraint (with None values)
        assert user_constraints[1]['name'] == 'UK_USERS_EMAIL'
        assert user_constraints[1]['type'] == 'UNIQUE'
        assert 'comment' not in user_constraints[1]
        assert 'is_enforced' not in user_constraints[1]
        assert 'is_deferrable' not in user_constraints[1]

    def test_process_constraints_existing_constraints(self):
        """Test _process_constraints with existing constraints"""
        schema = {
            'PUBLIC': {
                'tables': {
                    'USERS': {
                        'type': 'BASE TABLE',
                        'owner': 'ADMIN',
                        'columns': [],
                        'constraints': [
                            {
                                'name': 'EXISTING_CONSTRAINT',
                                'type': 'CHECK'
                            }
                        ]
                    }
                }
            }
        }
        constraints = [
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'USERS',
                'CONSTRAINT_NAME': 'PK_USERS',
                'CONSTRAINT_TYPE': 'PRIMARY KEY',
                'CONSTRAINT_COMMENT': 'Primary key constraint',
                'IS_ENFORCED': 'YES',
                'IS_DEFERRABLE': 'NO'
            }
        ]
        
        Processor._process_constraints(constraints, schema)
        
        user_constraints = schema['PUBLIC']['tables']['USERS']['constraints']
        assert len(user_constraints) == 2
        
        # Verify existing constraint is preserved
        assert user_constraints[0]['name'] == 'EXISTING_CONSTRAINT'
        # Verify new constraint is added
        assert user_constraints[1]['name'] == 'PK_USERS'

    @pytest.mark.asyncio
    async def test_process_database_structure_complete_flow(self, mock_db_client):
        """Test complete process_database_structure method"""
        # Setup mock data for all three queries
        mock_tables = [
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'USERS',
                'TABLE_TYPE': 'BASE TABLE',
                'TABLE_OWNER': 'ADMIN',
                'TABLE_COMMENT': 'User information table'
            }
        ]
        
        mock_columns = [
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'USERS',
                'COLUMN_NAME': 'ID',
                'DATA_TYPE': 'NUMBER',
                'COLUMN_COMMENT': 'Primary key',
                'ORDINAL_POSITION': 1
            }
        ]
        
        mock_constraints = [
            {
                'TABLE_SCHEMA': 'PUBLIC',
                'TABLE_NAME': 'USERS',
                'CONSTRAINT_NAME': 'PK_USERS',
                'CONSTRAINT_TYPE': 'PRIMARY KEY',
                'CONSTRAINT_COMMENT': 'Primary key constraint',
                'IS_ENFORCED': 'YES',
                'IS_DEFERRABLE': 'NO'
            }
        ]
        
        # Setup mock to return different data for each call
        mock_db_client.execute_query.side_effect = [
            (mock_tables, "tables-data-id"),
            (mock_columns, "columns-data-id"),
            (mock_constraints, "constraints-data-id")
        ]
        
        database_info = {
            'kind': 'STANDARD',
            'owner': 'ADMIN',
            'comment': 'Test database'
        }
        
        processor = Processor(mock_db_client)
        result = await processor.process_database_structure("TEST_DB", database_info)
        
        # Verify structure
        assert 'metadata' in result
        assert 'schemas' in result
        
        # Verify metadata
        metadata = result['metadata']
        assert metadata['kind'] == 'STANDARD'
        assert metadata['owner'] == 'ADMIN'
        assert metadata['comment'] == 'Test database'
        
        # Verify schema structure
        schemas = result['schemas']
        assert 'PUBLIC' in schemas
        assert 'USERS' in schemas['PUBLIC']['tables']
        
        # Verify table structure
        users_table = schemas['PUBLIC']['tables']['USERS']
        assert users_table['type'] == 'BASE TABLE'
        assert users_table['owner'] == 'ADMIN'
        assert users_table['comment'] == 'User information table'
        
        # Verify columns
        assert len(users_table['columns']) == 1
        assert users_table['columns'][0]['name'] == 'ID'
        assert users_table['columns'][0]['type'] == 'NUMBER'
        
        # Verify constraints
        assert len(users_table['constraints']) == 1
        assert users_table['constraints'][0]['name'] == 'PK_USERS'
        assert users_table['constraints'][0]['type'] == 'PRIMARY KEY'
        
        # Verify all three queries were called
        assert mock_db_client.execute_query.call_count == 3

    @pytest.mark.asyncio
    async def test_process_database_structure_default_metadata(self, mock_db_client):
        """Test process_database_structure with default metadata values"""
        # Setup minimal mock data
        mock_db_client.execute_query.side_effect = [
            ([], "tables-data-id"),
            ([], "columns-data-id"),
            ([], "constraints-data-id")
        ]
        
        # Empty database info
        database_info = {}
        
        processor = Processor(mock_db_client)
        result = await processor.process_database_structure("TEST_DB", database_info)
        
        # Verify default metadata values
        metadata = result['metadata']
        assert metadata['kind'] == 'IMPORTED DATABASE'
        assert metadata['owner'] == ''
        assert metadata['comment'] == ''