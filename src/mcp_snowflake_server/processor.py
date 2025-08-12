import logging

logger = logging.getLogger('mcp_snowflake_dataops_server')
logger.setLevel(logging.ERROR)


class Processor:

    def __init__(self, db):
        self.db = db

    async def _retrieve_table_metadata(self, db_name):
        tables_query = f"""
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                TABLE_TYPE,
                TABLE_OWNER,
                COMMENT as TABLE_COMMENT
            FROM {db_name}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'
        """
        logger.debug(f"Executing tables query: {tables_query}")
        tables, _ = await self.db.execute_query(tables_query)
        logger.info(f"Retrieved {len(tables)} columns")
        return tables

    async def _retrieve_column_metadata(self, db_name):
        columns_query = f"""
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                COLUMN_NAME,
                DATA_TYPE,
                COMMENT as COLUMN_COMMENT,
                ORDINAL_POSITION
            FROM {db_name}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        """
        logger.debug(f"Executing columns query: {columns_query}")
        columns, _ = await self.db.execute_query(columns_query)
        logger.info(f"Retrieved {len(columns)} columns")
        return columns

    async def _retrieve_constraints(self, db_name):
        constraints_query = f"""
            SELECT
                TABLE_SCHEMA,
                TABLE_NAME,
                CONSTRAINT_NAME,
                CONSTRAINT_TYPE,
                ENFORCED as IS_ENFORCED,
                INITIALLY_DEFERRED as IS_DEFERRABLE,
                COMMENT as CONSTRAINT_COMMENT
            FROM {db_name}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'
            ORDER BY TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME
        """
        logger.debug(f"Executing constraints query for {db_name}: {constraints_query}")
        constraints, _ = await self.db.execute_query(constraints_query)
        logger.info(f"Retrieved {len(constraints)} constraints from {db_name}")
        return constraints

    @staticmethod
    def _process_tables(tables, schemas):
        for table in tables:
            schema_name = table['TABLE_SCHEMA']
            table_name = table['TABLE_NAME']

            # Initialize schema if needed
            if schema_name not in schemas:
                schemas[schema_name] = {"tables": {}}

            # Create table entry
            table_info = {
                "type": table['TABLE_TYPE'],
                "owner": table['TABLE_OWNER'],
                "columns": []
            }

            if table['TABLE_COMMENT']:
                table_info["comment"] = table['TABLE_COMMENT']

            schemas[schema_name]["tables"][table_name] = table_info

    @staticmethod
    def _process_columns(columns, schema):
        for col in columns:
            schema_name = col['TABLE_SCHEMA']
            table_name = col['TABLE_NAME']

            col_info = {
                "name": col['COLUMN_NAME'],
                "type": col['DATA_TYPE']
            }
            if col['COLUMN_COMMENT']:
                col_info["comment"] = col['COLUMN_COMMENT']

            schema[schema_name]["tables"][table_name]["columns"].append(col_info)

    @staticmethod
    def _process_constraints(constraints, schema):
        for constraint in constraints:
            schema_name = constraint['TABLE_SCHEMA']
            table_name = constraint['TABLE_NAME']

            if not schema[schema_name]["tables"][table_name].get("constraints"):
                schema[schema_name]["tables"][table_name]["constraints"] = []

            constraint_info = {
                "name": constraint['CONSTRAINT_NAME'],
                "type": constraint['CONSTRAINT_TYPE']
            }
            if constraint['CONSTRAINT_COMMENT']:
                constraint_info["comment"] = constraint['CONSTRAINT_COMMENT']
            if constraint['IS_ENFORCED'] is not None:
                constraint_info["is_enforced"] = constraint['IS_ENFORCED']
            if constraint['IS_DEFERRABLE'] is not None:
                constraint_info["is_deferrable"] = constraint['IS_DEFERRABLE']

            schema[schema_name]["tables"][table_name]["constraints"].append(constraint_info)

    async def process_database_structure(self, db_name, database_info):
        """Process a database using its Information Schema"""
        logger.info(f"Starting to process database: {db_name}")

        # Initialize database structure
        schema = {}
        database_structure = {
            "metadata": {
                "kind": database_info.get('kind', 'IMPORTED DATABASE'),
                "owner": database_info.get('owner', ''),
                "comment": database_info.get('comment', ''),
            },
            "schemas": schema
        }

        # Get tables metadata and process it
        self._process_tables(await self._retrieve_table_metadata(db_name), schema)

        # Get columns data and process it
        self._process_columns(await self._retrieve_column_metadata(db_name), schema)

        # Get constraints data and process it
        self._process_constraints(await self._retrieve_constraints(db_name), schema)

        return database_structure
