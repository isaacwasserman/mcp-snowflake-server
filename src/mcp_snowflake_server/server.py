import importlib.metadata
import json
import logging
import os
import time
import uuid
from functools import wraps
from typing import Any, Callable
import datetime
from decimal import Decimal

import mcp.server.stdio
import mcp.types as types
import yaml
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from pydantic import AnyUrl, BaseModel
from snowflake.snowpark import Session

from .write_detector import SQLWriteDetector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("mcp_snowflake_server")


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime, date, and Timestamp objects"""

    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        elif hasattr(obj, "isoformat"):
            # Handle Snowflake Timestamp and other objects with isoformat method
            return obj.isoformat()
        elif hasattr(obj, "strftime"):
            # Handle other date-like objects
            try:
                return obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            except Exception:
                return str(obj)
        return super().default(obj)


def data_to_yaml(data: Any) -> str:
    return yaml.dump(data, indent=2, sort_keys=False)


class SnowflakeDB:
    AUTH_EXPIRATION_TIME = 1800

    def __init__(self, connection_config: dict):
        self.connection_config = connection_config
        self.session = None
        self.insights: list[str] = []
        self.auth_time = 0

    def _init_database(self):
        """Initialize connection to the Snowflake database"""
        try:
            # Create session without setting specific database and schema
            self.session = Session.builder.configs(self.connection_config).create()

            # Set initial warehouse if provided, but don't set database or schema
            if "warehouse" in self.connection_config:
                self.session.sql(f"USE WAREHOUSE {self.connection_config['warehouse'].upper()}")

            self.auth_time = time.time()
        except Exception as e:
            raise ValueError(f"Failed to connect to Snowflake database: {e}")

    def execute_query(self, query: str) -> tuple[list[dict[str, Any]], str]:
        """Execute a SQL query and return results as a list of dictionaries"""
        if not self.session or time.time() - self.auth_time > self.AUTH_EXPIRATION_TIME:
            self._init_database()

        logger.debug(f"Executing query: {query}")
        try:
            result = self.session.sql(query).to_pandas()

            # Convert all timestamp/date columns to ISO format strings for JSON serialization
            for col in result.columns:
                if result[col].dtype == "datetime64[ns]":
                    result[col] = result[col].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                elif result[col].dtype == "object":
                    # Handle various timestamp/date types that might be in object columns
                    def convert_timestamp(x):
                        if x is None:
                            return None
                        elif isinstance(x, (datetime.date, datetime.datetime)):
                            return x.isoformat()
                        elif hasattr(x, "isoformat"):  # Handle Snowflake Timestamp objects
                            return x.isoformat()
                        elif hasattr(x, "strftime"):  # Handle other date-like objects
                            try:
                                return x.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                            except:
                                return str(x)
                        else:
                            return x

                    result[col] = result[col].apply(convert_timestamp)

            result_rows = result.to_dict(orient="records")

            # Deep conversion pass to ensure all nested timestamps are converted
            result_rows = self._convert_timestamps_deep(result_rows)

            data_id = str(uuid.uuid4())

            return result_rows, data_id

        except Exception as e:
            logger.error(f'Database error executing "{query}": {e}')
            raise

    def _convert_timestamps_deep(self, data: Any) -> Any:
        """Recursively convert all timestamp objects to ISO format strings"""
        if isinstance(data, list):
            return [self._convert_timestamps_deep(item) for item in data]
        elif isinstance(data, dict):
            return {key: self._convert_timestamps_deep(value) for key, value in data.items()}
        elif isinstance(data, (datetime.datetime, datetime.date)):
            return data.isoformat()
        elif hasattr(data, "isoformat"):
            # Handle Snowflake Timestamp and other timestamp-like objects
            try:
                return data.isoformat()
            except Exception:
                return str(data)
        elif hasattr(data, "strftime"):
            # Handle other date-like objects
            try:
                return data.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            except Exception:
                return str(data)
        else:
            return data

    def add_insight(self, insight: str) -> None:
        """Add a new insight to the collection"""
        self.insights.append(insight)

    def get_memo(self) -> str:
        """Generate a formatted memo from collected insights"""
        if not self.insights:
            return "No data insights have been discovered yet."

        memo = "📊 Data Intelligence Memo 📊\n\n"
        memo += "Key Insights Discovered:\n\n"
        memo += "\n".join(f"- {insight}" for insight in self.insights)

        if len(self.insights) > 1:
            memo += f"\n\nSummary:\nAnalysis has revealed {len(self.insights)} key data insights that suggest opportunities for strategic optimization and growth."

        return memo


def handle_tool_errors(func: Callable) -> Callable:
    """Decorator to standardize tool error handling"""

    @wraps(func)
    async def wrapper(*args, **kwargs) -> list[types.TextContent]:
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            return [types.TextContent(type="text", text=f"Error: {str(e)}")]

    return wrapper


class Tool(BaseModel):
    name: str
    description: str
    input_schema: dict[str, Any]
    handler: Callable[
        [str, dict[str, Any] | None],
        list[types.TextContent | types.ImageContent | types.EmbeddedResource],
    ]
    tags: list[str] = []


def normalize_identifier_part(part: str) -> tuple[str, str]:
    """Normalize an identifier part and determine the value to use for metadata lookups."""

    stripped = part.strip()
    if stripped.startswith('"') and stripped.endswith('"') and len(stripped) >= 2:
        # Remove surrounding quotes and unescape any embedded quotes
        unquoted = stripped[1:-1].replace('""', '"')
        return unquoted, unquoted
    return stripped, stripped.upper()


def parse_table_identifier(table_spec: str) -> tuple[list[str], list[str]]:
    """Split a fully qualified table name into parts and metadata-safe variants."""

    parts = [part for part in table_spec.split(".") if part.strip()]
    if len(parts) != 3:
        raise ValueError("Table name must be fully qualified as 'database.schema.table'")

    normalized_parts: list[str] = []
    metadata_parts: list[str] = []
    for part in parts:
        normalized, metadata = normalize_identifier_part(part)
        normalized_parts.append(normalized)
        metadata_parts.append(metadata)

    return normalized_parts, metadata_parts


def escape_literal(value: str) -> str:
    """Escape string literals for inclusion in SQL queries."""

    return value.replace("'", "''")


def normalize_where_clause(where_clause: str | None) -> str:
    """Normalize an optional SQL WHERE clause fragment."""

    if where_clause is None:
        return ""
    if not isinstance(where_clause, str):
        raise ValueError("where_clause must be a string")

    normalized = where_clause.strip()
    if not normalized:
        return ""

    if normalized.upper() == "WHERE":
        raise ValueError("where_clause must include a condition")
    if normalized.upper().startswith("WHERE "):
        normalized = normalized[6:].strip()

    if ";" in normalized:
        raise ValueError("where_clause must not contain semicolons")

    return normalized


def fetch_table_columns(db: SnowflakeDB, metadata_parts: list[str]) -> list[str]:
    """Fetch ordered column names for a given table using information_schema."""

    database, schema, table = metadata_parts
    query = f"""
        SELECT COLUMN_NAME
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{escape_literal(schema)}'
          AND TABLE_NAME = '{escape_literal(table)}'
        ORDER BY ORDINAL_POSITION
    """
    data, _ = db.execute_query(query)
    return [row["COLUMN_NAME"] for row in data]


# Tool handlers
async def handle_list_databases(arguments, db, *_, exclusion_config=None):
    query = "SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES"
    data, data_id = db.execute_query(query)

    # Filter out excluded databases
    if exclusion_config and "databases" in exclusion_config and exclusion_config["databases"]:
        filtered_data = []
        for item in data:
            db_name = item.get("DATABASE_NAME", "")
            exclude = False
            for pattern in exclusion_config["databases"]:
                if pattern.lower() in db_name.lower():
                    exclude = True
                    break
            if not exclude:
                filtered_data.append(item)
        data = filtered_data

    output = {
        "type": "data",
        "data_id": data_id,
        "data": data,
    }
    yaml_output = data_to_yaml(output)
    json_output = json.dumps(output, cls=DateTimeEncoder)
    return [
        types.TextContent(type="text", text=yaml_output),
        types.EmbeddedResource(
            type="resource",
            resource=types.TextResourceContents(
                uri=f"data://{data_id}", text=json_output, mimeType="application/json"
            ),
        ),
    ]


async def handle_list_schemas(arguments, db, *_, exclusion_config=None):
    if not arguments or "database" not in arguments:
        raise ValueError("Missing required 'database' parameter")

    database = arguments["database"]
    query = f"SELECT SCHEMA_NAME FROM {database.upper()}.INFORMATION_SCHEMA.SCHEMATA"
    data, data_id = db.execute_query(query)

    # Filter out excluded schemas
    if exclusion_config and "schemas" in exclusion_config and exclusion_config["schemas"]:
        filtered_data = []
        for item in data:
            schema_name = item.get("SCHEMA_NAME", "")
            exclude = False
            for pattern in exclusion_config["schemas"]:
                if pattern.lower() in schema_name.lower():
                    exclude = True
                    break
            if not exclude:
                filtered_data.append(item)
        data = filtered_data

    output = {
        "type": "data",
        "data_id": data_id,
        "database": database,
        "data": data,
    }
    yaml_output = data_to_yaml(output)
    json_output = json.dumps(output, cls=DateTimeEncoder)
    return [
        types.TextContent(type="text", text=yaml_output),
        types.EmbeddedResource(
            type="resource",
            resource=types.TextResourceContents(
                uri=f"data://{data_id}", text=json_output, mimeType="application/json"
            ),
        ),
    ]


async def handle_list_tables(arguments, db, *_, exclusion_config=None):
    if not arguments or "database" not in arguments or "schema" not in arguments:
        raise ValueError("Missing required 'database' and 'schema' parameters")

    database = arguments["database"]
    schema = arguments["schema"]

    query = f"""
        SELECT table_catalog, table_schema, table_name, comment 
        FROM {database.upper()}.information_schema.tables 
        WHERE table_schema = '{schema.upper()}'
    """
    data, data_id = db.execute_query(query)

    # Filter out excluded tables
    if exclusion_config and "tables" in exclusion_config and exclusion_config["tables"]:
        filtered_data = []
        for item in data:
            table_name = item.get("TABLE_NAME", "")
            exclude = False
            for pattern in exclusion_config["tables"]:
                if pattern.lower() in table_name.lower():
                    exclude = True
                    break
            if not exclude:
                filtered_data.append(item)
        data = filtered_data

    output = {
        "type": "data",
        "data_id": data_id,
        "database": database,
        "schema": schema,
        "data": data,
    }
    yaml_output = data_to_yaml(output)
    json_output = json.dumps(output, cls=DateTimeEncoder)
    return [
        types.TextContent(type="text", text=yaml_output),
        types.EmbeddedResource(
            type="resource",
            resource=types.TextResourceContents(
                uri=f"data://{data_id}", text=json_output, mimeType="application/json"
            ),
        ),
    ]


async def handle_describe_table(arguments, db, *_):
    if not arguments or "table_name" not in arguments:
        raise ValueError("Missing table_name argument")

    table_spec = arguments["table_name"]
    split_identifier = table_spec.split(".")

    # Parse the fully qualified table name
    if len(split_identifier) < 3:
        raise ValueError("Table name must be fully qualified as 'database.schema.table'")

    database_name = split_identifier[0].upper()
    schema_name = split_identifier[1].upper()
    table_name = split_identifier[2].upper()

    query = f"""
        SELECT column_name, column_default, is_nullable, data_type, comment 
        FROM {database_name}.information_schema.columns 
        WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
    """
    data, data_id = db.execute_query(query)

    output = {
        "type": "data",
        "data_id": data_id,
        "database": database_name,
        "schema": schema_name,
        "table": table_name,
        "data": data,
    }
    yaml_output = data_to_yaml(output)
    json_output = json.dumps(output, cls=DateTimeEncoder)
    return [
        types.TextContent(type="text", text=yaml_output),
        types.EmbeddedResource(
            type="resource",
            resource=types.TextResourceContents(
                uri=f"data://{data_id}", text=json_output, mimeType="application/json"
            ),
        ),
    ]


def estimate_row_size(row: dict) -> int:
    """Estimate the size of a row in characters for context window calculation."""
    total_size = 0
    for key, value in row.items():
        # Add key size
        total_size += len(str(key))
        # Add value size (estimate JSON serialization overhead)
        if value is None:
            total_size += 4  # "null"
        elif isinstance(value, (str, int, float, bool)):
            total_size += len(str(value))
        else:
            # For complex objects, estimate JSON serialization size
            total_size += len(str(value)) * 1.2  # Add 20% overhead for JSON formatting
    return total_size


def calculate_dynamic_limit(sample_rows: list[dict], max_context_chars: int = 15000) -> int:
    """Calculate dynamic row limit based on estimated row sizes."""
    if not sample_rows:
        return 30  # Default fallback

    # Calculate average row size from sample
    total_size = sum(estimate_row_size(row) for row in sample_rows)
    avg_row_size = total_size / len(sample_rows)

    # Calculate how many rows can fit in the context limit
    # Reserve some space for metadata and formatting
    available_chars = max_context_chars * 0.8  # Use 80% of limit for data
    dynamic_limit = max(1, int(available_chars / avg_row_size))

    # Apply reasonable bounds
    return min(max(dynamic_limit, 1), 100)  # Between 1 and 100 rows


async def handle_compare_models(arguments, db, *_):
    if not arguments:
        raise ValueError("Missing required arguments: base_model and comparing_model")

    if "base_model" not in arguments or "comparing_model" not in arguments:
        raise ValueError("Missing required arguments: base_model and comparing_model")

    base_model = arguments["base_model"]
    comparing_model = arguments["comparing_model"]
    column_source = arguments.get("column_source", "base").lower()
    where_clause = normalize_where_clause(arguments.get("where_clause"))

    if column_source not in {"base", "comparing"}:
        raise ValueError("column_source must be either 'base' or 'comparing'")

    preview_limit_arg = arguments.get("preview_limit", 30)
    try:
        preview_limit = int(preview_limit_arg)
    except (TypeError, ValueError):
        raise ValueError("preview_limit must be an integer")

    if preview_limit <= 0:
        raise ValueError("preview_limit must be greater than 0")

    # Get max context size from arguments (default 50KB)
    max_context_chars = arguments.get("max_context_chars", 50000)
    try:
        max_context_chars = int(max_context_chars)
    except (TypeError, ValueError):
        raise ValueError("max_context_chars must be an integer")

    except_columns_arg = arguments.get("except_columns", [])
    if isinstance(except_columns_arg, str):
        except_columns = [except_columns_arg]
    else:
        except_columns = list(except_columns_arg)

    # Get column_list parameter - if provided, use it directly
    column_list_arg = arguments.get("column_list", [])
    if isinstance(column_list_arg, str):
        column_list = [column_list_arg]
    else:
        column_list = list(column_list_arg)
    column_list_provided = bool(column_list)

    base_parts, base_metadata_parts = parse_table_identifier(base_model)
    compare_parts, compare_metadata_parts = parse_table_identifier(comparing_model)

    # Always fetch column lists for validation
    base_columns = fetch_table_columns(db, base_metadata_parts)
    compare_columns = fetch_table_columns(db, compare_metadata_parts)

    if column_list:
        # Use provided column list directly
        selected_columns = column_list
        logger.info(f"Using provided column list: {selected_columns}")
    else:
        # Use existing logic to determine columns
        source_columns = base_columns if column_source == "base" else compare_columns
        exceptions = {col.upper() for col in except_columns}
        selected_columns = [col for col in source_columns if col.upper() not in exceptions]

        if not selected_columns:
            raise ValueError("No columns available for comparison after applying exceptions")

    base_column_set = {col.upper() for col in base_columns}
    compare_column_set = {col.upper() for col in compare_columns}

    missing_in_base = [col for col in selected_columns if col.upper() not in base_column_set]
    missing_in_compare = [col for col in selected_columns if col.upper() not in compare_column_set]

    if missing_in_base:
        raise ValueError(
            "Selected columns are not present in the base model: " + ", ".join(missing_in_base)
        )
    if missing_in_compare:
        raise ValueError(
            "Selected columns are not present in the comparing model: "
            + ", ".join(missing_in_compare)
        )

    column_sql = ", ".join(selected_columns)

    base_fqn = ".".join(part.upper() for part in base_parts)
    compare_fqn = ".".join(part.upper() for part in compare_parts)
    where_sql = f"\n    WHERE {where_clause}" if where_clause else ""

    base_cte = f"""
WITH base_model AS (
    SELECT {column_sql}
    FROM {base_fqn}{where_sql}
),
compare_model AS (
    SELECT {column_sql}
    FROM {compare_fqn}{where_sql}
)"""

    # Sample some rows to estimate size and calculate dynamic limit
    sample_query = f"""
{base_cte},
sample_rows AS (
    SELECT {column_sql}
    FROM base_model
    LIMIT 5
)
SELECT * FROM sample_rows
"""

    try:
        sample_data, _ = db.execute_query(sample_query)
        dynamic_limit = calculate_dynamic_limit(sample_data, max_context_chars)
        # Use the smaller of user-specified limit or dynamic limit
        effective_limit = min(preview_limit, dynamic_limit)
        logger.info(
            f"Row size estimation: using {effective_limit} rows (user limit: {preview_limit}, dynamic limit: {dynamic_limit})"
        )
    except Exception as e:
        logger.warning(f"Failed to estimate row size, using user-specified limit: {e}")
        effective_limit = preview_limit

    stats_query = f"""
{base_cte},
base_counts AS (SELECT COUNT(*) AS base_row_count FROM base_model),
compare_counts AS (SELECT COUNT(*) AS compare_row_count FROM compare_model),
base_only AS (
    SELECT {column_sql}
    FROM base_model
    MINUS
    SELECT {column_sql}
    FROM compare_model
),
compare_only AS (
    SELECT {column_sql}
    FROM compare_model
    MINUS
    SELECT {column_sql}
    FROM base_model
),
base_only_count AS (SELECT COUNT(*) AS base_only_count FROM base_only),
compare_only_count AS (SELECT COUNT(*) AS compare_only_count FROM compare_only),
matching AS (
    SELECT COUNT(*) AS matching_row_count
    FROM (
        SELECT * FROM base_model
        INTERSECT
        SELECT * FROM compare_model
    )
)
SELECT
    base_counts.base_row_count,
    compare_counts.compare_row_count,
    matching.matching_row_count,
    base_only_count.base_only_count,
    compare_only_count.compare_only_count,
    base_only_count.base_only_count + compare_only_count.compare_only_count AS differing_row_count
FROM base_counts, compare_counts, matching, base_only_count, compare_only_count
"""

    diff_query = f"""
{base_cte},
diff_compare_only AS (
    SELECT 'in_comparing_not_base' AS difference_type, {column_sql}
    FROM compare_model
    MINUS
    SELECT 'in_comparing_not_base' AS difference_type, {column_sql}
    FROM base_model
),
diff_base_only AS (
    SELECT 'in_base_not_comparing' AS difference_type, {column_sql}
    FROM base_model
    MINUS
    SELECT 'in_base_not_comparing' AS difference_type, {column_sql}
    FROM compare_model
)
SELECT *
FROM diff_compare_only
UNION ALL
SELECT *
FROM diff_base_only
ORDER BY 1, 2
LIMIT {effective_limit}
"""

    stats_data, stats_data_id = db.execute_query(stats_query)
    differences_data, differences_data_id = db.execute_query(diff_query)

    stats_row = stats_data[0] if stats_data else {}

    summary_output = {
        "type": "model_comparison_summary",
        "base_model": base_model,
        "comparing_model": comparing_model,
        "where_clause": where_clause or None,
        "column_source": column_source if not column_list_provided else None,
        "columns_compared": selected_columns,
        "column_list_provided": column_list_provided,
        "except_columns": except_columns if not column_list_provided else [],
        "stats_data_id": stats_data_id,
        "statistics": stats_row,
        "differences_data_id": differences_data_id,
        "differences_preview_count": len(differences_data),
        "differences_preview_limit": effective_limit,
        "user_specified_limit": preview_limit,
        "max_context_chars": max_context_chars,
    }

    summary_yaml = data_to_yaml(summary_output)
    summary_json = json.dumps(summary_output, cls=DateTimeEncoder)
    differences_output = {
        "type": "model_comparison_differences",
        "data_id": differences_data_id,
        "base_model": base_model,
        "comparing_model": comparing_model,
        "where_clause": where_clause or None,
        "difference_rows": differences_data,
        "preview_limit": effective_limit,
        "user_specified_limit": preview_limit,
        "preview_count": len(differences_data),
    }
    differences_json = json.dumps(differences_output, cls=DateTimeEncoder)

    return [
        types.TextContent(type="text", text=summary_yaml),
        types.EmbeddedResource(
            type="resource",
            resource=types.TextResourceContents(
                uri=f"data://{stats_data_id}",
                text=summary_json,
                mimeType="application/json",
            ),
        ),
        types.EmbeddedResource(
            type="resource",
            resource=types.TextResourceContents(
                uri=f"data://{differences_data_id}",
                text=differences_json,
                mimeType="application/json",
            ),
        ),
    ]


async def handle_read_query(arguments, db, write_detector, *_):
    if not arguments or "query" not in arguments:
        raise ValueError("Missing query argument")

    if write_detector.analyze_query(arguments["query"])["contains_write"]:
        raise ValueError("Calls to read_query should not contain write operations")

    data, data_id = db.execute_query(arguments["query"])
    output = {
        "type": "data",
        "data_id": data_id,
        "data": data,
    }
    yaml_output = data_to_yaml(output)
    json_output = json.dumps(output, cls=DateTimeEncoder)
    return [
        types.TextContent(type="text", text=yaml_output),
        types.EmbeddedResource(
            type="resource",
            resource=types.TextResourceContents(
                uri=f"data://{data_id}", text=json_output, mimeType="application/json"
            ),
        ),
    ]


async def handle_append_insight(arguments, db, _, __, server):
    if not arguments or "insight" not in arguments:
        raise ValueError("Missing insight argument")

    db.add_insight(arguments["insight"])
    await server.request_context.session.send_resource_updated(AnyUrl("memo://insights"))
    return [types.TextContent(type="text", text="Insight added to memo")]


async def handle_write_query(arguments, db, _, allow_write, __):
    if not allow_write:
        raise ValueError("Write operations are not allowed for this data connection")
    if arguments["query"].strip().upper().startswith("SELECT"):
        raise ValueError("SELECT queries are not allowed for write_query")

    results, data_id = db.execute_query(arguments["query"])
    return [types.TextContent(type="text", text=str(results))]


async def handle_create_table(arguments, db, _, allow_write, __):
    if not allow_write:
        raise ValueError("Write operations are not allowed for this data connection")
    if not arguments["query"].strip().upper().startswith("CREATE TABLE"):
        raise ValueError("Only CREATE TABLE statements are allowed")

    results, data_id = db.execute_query(arguments["query"])
    return [types.TextContent(type="text", text=f"Table created successfully. data_id = {data_id}")]


async def main(
    allow_write: bool = False,
    connection_args: dict = None,
    log_dir: str = None,
    log_level: str = "INFO",
    exclude_tools: list[str] = [],
    config_file: str = "runtime_config.json",
    exclude_patterns: dict = None,
):
    # Setup logging
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        logger.handlers.append(
            logging.FileHandler(os.path.join(log_dir, "mcp_snowflake_server.log"))
        )
    if log_level:
        logger.setLevel(log_level)

    logger.info("Starting Snowflake MCP Server")
    logger.info("Allow write operations: %s", allow_write)
    logger.info("Excluded tools: %s", exclude_tools)

    # Load configuration from file if provided
    config = {}
    #
    if config_file:
        try:
            with open(config_file, "r") as f:
                config = json.load(f)
                logger.info(f"Loaded configuration from {config_file}")
        except Exception as e:
            logger.error(f"Error loading configuration file: {e}")

    # Merge exclude_patterns from parameters with config file
    exclusion_config = config.get("exclude_patterns", {})
    if exclude_patterns:
        # Merge patterns from parameters with those from config file
        for key, patterns in exclude_patterns.items():
            if key in exclusion_config:
                exclusion_config[key].extend(patterns)
            else:
                exclusion_config[key] = patterns

    # Set default patterns if none are specified
    if not exclusion_config:
        exclusion_config = {"databases": [], "schemas": [], "tables": []}

    # Ensure all keys exist in the exclusion config
    for key in ["databases", "schemas", "tables"]:
        if key not in exclusion_config:
            exclusion_config[key] = []

    logger.info(f"Exclusion patterns: {exclusion_config}")

    db = SnowflakeDB(connection_args)
    server = Server("snowflake-manager")
    write_detector = SQLWriteDetector()

    tables_info = {}
    tables_brief = ""

    all_tools = [
        Tool(
            name="list_databases",
            description="List all available databases in Snowflake",
            input_schema={
                "type": "object",
                "properties": {},
            },
            handler=handle_list_databases,
        ),
        Tool(
            name="list_schemas",
            description="List all schemas in a database",
            input_schema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name to list schemas from",
                    },
                },
                "required": ["database"],
            },
            handler=handle_list_schemas,
        ),
        Tool(
            name="list_tables",
            description="List all tables in a specific database and schema",
            input_schema={
                "type": "object",
                "properties": {
                    "database": {"type": "string", "description": "Database name"},
                    "schema": {"type": "string", "description": "Schema name"},
                },
                "required": ["database", "schema"],
            },
            handler=handle_list_tables,
        ),
        Tool(
            name="describe_table",
            description="Get the schema information for a specific table",
            input_schema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Fully qualified table name in the format 'database.schema.table'",
                    },
                },
                "required": ["table_name"],
            },
            handler=handle_describe_table,
        ),
        Tool(
            name="compare_models",
            description="Compare two models or tables and report summary statistics and differences",
            input_schema={
                "type": "object",
                "properties": {
                    "base_model": {
                        "type": "string",
                        "description": "Fully qualified table name for the base model (database.schema.table)",
                    },
                    "comparing_model": {
                        "type": "string",
                        "description": "Fully qualified table name for the comparing model (database.schema.table)",
                    },
                    "where_clause": {
                        "type": "string",
                        "description": "Optional SQL WHERE condition applied to both models before comparison. May include or omit the WHERE keyword. Semicolons are not allowed.",
                    },
                    "column_source": {
                        "type": "string",
                        "description": "Which model's columns to use for comparison (base or comparing)",
                        "enum": ["base", "comparing"],
                        "default": "base",
                    },
                    "preview_limit": {
                        "type": "integer",
                        "description": "Maximum number of differing rows to include in the preview (may be reduced based on row size)",
                        "default": 30,
                        "minimum": 1,
                    },
                    "max_context_chars": {
                        "type": "integer",
                        "description": "Maximum number of characters to use for context window (default: 15000)",
                        "default": 15000,
                        "minimum": 1000,
                    },
                    "column_list": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of specific column names to compare. If provided, only these columns will be compared (ignores column_source and except_columns)",
                        "default": [],
                    },
                    "except_columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of column names to exclude from the comparison (ignored if column_list is provided)",
                        "default": [],
                    },
                },
                "required": ["base_model", "comparing_model"],
            },
            handler=handle_compare_models,
        ),
        Tool(
            name="read_query",
            description="Execute a SELECT query.",
            input_schema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "SELECT SQL query to execute"}
                },
                "required": ["query"],
            },
            handler=handle_read_query,
        ),
        Tool(
            name="append_insight",
            description="Add a data insight to the memo",
            input_schema={
                "type": "object",
                "properties": {
                    "insight": {
                        "type": "string",
                        "description": "Data insight discovered from analysis",
                    }
                },
                "required": ["insight"],
            },
            handler=handle_append_insight,
            tags=["resource_based"],
        ),
        Tool(
            name="write_query",
            description="Execute an INSERT, UPDATE, or DELETE query on the Snowflake database",
            input_schema={
                "type": "object",
                "properties": {"query": {"type": "string", "description": "SQL query to execute"}},
                "required": ["query"],
            },
            handler=handle_write_query,
            tags=["write"],
        ),
        Tool(
            name="create_table",
            description="Create a new table in the Snowflake database",
            input_schema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "CREATE TABLE SQL statement"}
                },
                "required": ["query"],
            },
            handler=handle_create_table,
            tags=["write"],
        ),
    ]

    exclude_tags = []
    if not allow_write:
        exclude_tags.append("write")
    allowed_tools = [
        tool
        for tool in all_tools
        if tool.name not in exclude_tools and not any(tag in exclude_tags for tag in tool.tags)
    ]

    logger.info("Allowed tools: %s", [tool.name for tool in allowed_tools])

    # Register handlers
    @server.list_resources()
    async def handle_list_resources() -> list[types.Resource]:
        resources = [
            types.Resource(
                uri=AnyUrl("memo://insights"),
                name="Data Insights Memo",
                description="A living document of discovered data insights",
                mimeType="text/plain",
            )
        ]
        table_brief_resources = [
            types.Resource(
                uri=AnyUrl(f"context://table/{table_name}"),
                name=f"{table_name} table",
                description=f"Description of the {table_name} table",
                mimeType="text/plain",
            )
            for table_name in tables_info.keys()
        ]
        resources += table_brief_resources
        return resources

    @server.read_resource()
    async def handle_read_resource(uri: AnyUrl) -> str:
        if str(uri) == "memo://insights":
            return db.get_memo()
        elif str(uri).startswith("context://table"):
            table_name = str(uri).split("/")[-1]
            if table_name in tables_info:
                return data_to_yaml(tables_info[table_name])
            else:
                raise ValueError(f"Unknown table: {table_name}")
        else:
            raise ValueError(f"Unknown resource: {uri}")

    @server.list_prompts()
    async def handle_list_prompts() -> list[types.Prompt]:
        return []

    @server.get_prompt()
    async def handle_get_prompt(
        name: str, arguments: dict[str, str] | None
    ) -> types.GetPromptResult:
        raise ValueError(f"Unknown prompt: {name}")

    @server.call_tool()
    @handle_tool_errors
    async def handle_call_tool(
        name: str, arguments: dict[str, Any] | None
    ) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        if name in exclude_tools:
            return [
                types.TextContent(
                    type="text", text=f"Tool {name} is excluded from this data connection"
                )
            ]

        handler = next((tool.handler for tool in allowed_tools if tool.name == name), None)
        if not handler:
            raise ValueError(f"Unknown tool: {name}")

        # Pass exclusion_config to the handler if it's a listing function
        if name in ["list_databases", "list_schemas", "list_tables"]:
            return await handler(
                arguments,
                db,
                write_detector,
                allow_write,
                server,
                exclusion_config=exclusion_config,
            )
        else:
            return await handler(arguments, db, write_detector, allow_write, server)

    @server.list_tools()
    async def handle_list_tools() -> list[types.Tool]:
        logger.info("Listing tools")
        logger.error(f"Allowed tools: {allowed_tools}")
        tools = [
            types.Tool(
                name=tool.name,
                description=tool.description,
                inputSchema=tool.input_schema,
            )
            for tool in allowed_tools
        ]
        return tools

    # Start server
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        logger.info("Server running with stdio transport")
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="snowflake",
                server_version=importlib.metadata.version("mcp_snowflake_server"),
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )
