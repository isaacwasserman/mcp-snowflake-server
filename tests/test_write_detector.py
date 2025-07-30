"""Unit tests for SQLWriteDetector class"""
import sqlparse


class TestSQLWriteDetector:
    """Test cases for SQLWriteDetector"""

    def test_init(self, write_detector):
        """Test SQLWriteDetector initialization"""
        assert isinstance(write_detector.dml_write_keywords, set)
        assert isinstance(write_detector.ddl_keywords, set)
        assert isinstance(write_detector.dcl_keywords, set)
        assert isinstance(write_detector.write_keywords, set)

        # Check that write_keywords contains all categories
        assert write_detector.dml_write_keywords.issubset(write_detector.write_keywords)
        assert write_detector.ddl_keywords.issubset(write_detector.write_keywords)
        assert write_detector.dcl_keywords.issubset(write_detector.write_keywords)

    def test_analyze_select_query(self, write_detector):
        """Test analysis of SELECT queries (read-only)"""
        queries = [
            "SELECT * FROM users",
            "SELECT name, email FROM users WHERE id = 1",
            "SELECT COUNT(*) FROM orders",
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"
        ]

        for query in queries:
            result = write_detector.analyze_query(query)
            assert result["contains_write"] is False
            assert len(result["write_operations"]) == 0
            assert result["has_cte_write"] is False

    def test_analyze_insert_query(self, write_detector):
        """Test analysis of INSERT queries"""
        queries = [
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
            "INSERT INTO orders SELECT * FROM temp_orders",
            "insert into products values (1, 'Product A')"  # lowercase
        ]

        for query in queries:
            result = write_detector.analyze_query(query)
            assert result["contains_write"] is True
            assert "INSERT" in result["write_operations"]
            assert result["has_cte_write"] is False

    def test_analyze_update_query(self, write_detector):
        """Test analysis of UPDATE queries"""
        queries = [
            "UPDATE users SET email = 'new@example.com' WHERE id = 1",
            "UPDATE orders SET status = 'shipped' WHERE order_date < '2023-01-01'",
            "update products set price = price * 1.1"  # lowercase
        ]

        for query in queries:
            result = write_detector.analyze_query(query)
            assert result["contains_write"] is True
            assert "UPDATE" in result["write_operations"]
            assert result["has_cte_write"] is False

    def test_analyze_delete_query(self, write_detector):
        """Test analysis of DELETE queries"""
        queries = [
            "DELETE FROM users WHERE id = 1",
            "DELETE FROM orders WHERE status = 'cancelled'",
            "delete from temp_table"  # lowercase
        ]

        for query in queries:
            result = write_detector.analyze_query(query)
            assert result["contains_write"] is True
            assert "DELETE" in result["write_operations"]
            assert result["has_cte_write"] is False

    def test_analyze_ddl_queries(self, write_detector):
        """Test analysis of DDL queries"""
        queries_op_tuples = [
            ("CREATE", "CREATE TABLE users (id INT, name VARCHAR(100))"),
            ("ALTER", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
            ("DROP", "DROP TABLE temp_table"),
            ("TRUNCATE", "TRUNCATE TABLE logs"),
            ("CREATE", "create view user_summary as select * from users")
        ]

        for expected_op, query in queries_op_tuples:
            result = write_detector.analyze_query(query)
            assert result["contains_write"] is True
            assert expected_op in result["write_operations"]
            assert result["has_cte_write"] is False

    def test_analyze_dcl_queries(self, write_detector):
        """Test analysis of DCL queries"""
        queries_op_tuples = [
            ("GRANT", "GRANT SELECT ON users TO role_name"),
            ("REVOKE", "REVOKE INSERT ON orders FROM user_name"),
            ("GRANT", "grant all privileges on database test_db to role admin")
        ]

        for expected_op, query in queries_op_tuples:
            result = write_detector.analyze_query(query)
            assert result["contains_write"] is True
            assert expected_op in result["write_operations"]
            assert result["has_cte_write"] is False

    def test_analyze_merge_upsert_queries(self, write_detector):
        """Test analysis of MERGE and UPSERT queries"""
        queries_op_tuples = [
            ("MERGE", "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET name = source.name"),
            ("UPSERT", "UPSERT INTO users VALUES (1, 'John')"),
            ("MERGE", "merge into products using staging_products on products.id = staging_products.id")
        ]

        for expected_op, query in queries_op_tuples:
            result = write_detector.analyze_query(query)
            assert result["contains_write"] is True
            assert expected_op in result["write_operations"]
            assert result["has_cte_write"] is False

    def test_analyze_cte_with_select(self, write_detector):
        """Test analysis of CTE with SELECT (read-only)"""
        query = """
        WITH user_stats AS (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            GROUP BY user_id
        )
        SELECT u.name, us.order_count
        FROM users u
        JOIN user_stats us ON u.id = us.user_id
        """

        result = write_detector.analyze_query(query)
        assert result["contains_write"] is False
        assert len(result["write_operations"]) == 0
        assert result["has_cte_write"] is False

    def test_analyze_cte_with_write(self, write_detector):
        """Test analysis of CTE with write operations"""
        query = """
        WITH updated_users AS (
            UPDATE users SET last_login = CURRENT_TIMESTAMP
            WHERE id IN (SELECT user_id FROM recent_activity)
            RETURNING *
        )
        SELECT * FROM updated_users
        """

        result = write_detector.analyze_query(query)
        assert result["contains_write"] is True
        assert result["has_cte_write"] is True
        assert "CTE_WRITE" in result["write_operations"]

    def test_analyze_complex_query_with_multiple_operations(self, write_detector):
        """Test analysis of complex queries with multiple write operations"""
        query = """
        INSERT INTO audit_log (action, table_name, timestamp)
        VALUES ('UPDATE', 'users', CURRENT_TIMESTAMP);
        
        UPDATE users SET status = 'active' WHERE last_login > '2023-01-01';
        """

        result = write_detector.analyze_query(query)
        assert result["contains_write"] is True
        assert "INSERT" in result["write_operations"]
        assert "UPDATE" in result["write_operations"]
        assert result["has_cte_write"] is False

    def test_analyze_empty_query(self, write_detector):
        """Test analysis of empty or whitespace-only queries"""
        queries = ["", "   ", "\n\t  \n"]

        for query in queries:
            result = write_detector.analyze_query(query)
            assert result["contains_write"] is False
            assert len(result["write_operations"]) == 0
            assert result["has_cte_write"] is False

    def test_analyze_comment_only_query(self, write_detector):
        """Test analysis of queries with only comments"""
        queries = [
            "-- This is a comment",
            "/* Multi-line comment */",
            "-- SELECT * FROM users (commented out)"
        ]

        for query in queries:
            result = write_detector.analyze_query(query)
            assert result["contains_write"] is False
            assert len(result["write_operations"]) == 0
            assert result["has_cte_write"] is False

    def test_has_cte_method(self, write_detector):
        """Test the _has_cte method"""
        # Query with CTE
        query_with_cte = "WITH cte AS (SELECT * FROM users) SELECT * FROM cte"
        parsed_with_cte = sqlparse.parse(query_with_cte)[0]
        assert write_detector._has_cte(parsed_with_cte) is True

        # Query without CTE
        query_without_cte = "SELECT * FROM users"
        parsed_without_cte = sqlparse.parse(query_without_cte)[0]
        assert write_detector._has_cte(parsed_without_cte) is False

    def test_find_write_operations_method(self, write_detector):
        """Test the _find_write_operations method"""
        # Test with INSERT query
        insert_query = "INSERT INTO users VALUES (1, 'John')"
        parsed_insert = sqlparse.parse(insert_query)[0]
        operations = write_detector._find_write_operations(parsed_insert)
        assert "INSERT" in operations

        # Test with SELECT query
        select_query = "SELECT * FROM users"
        parsed_select = sqlparse.parse(select_query)[0]
        operations = write_detector._find_write_operations(parsed_select)
        assert len(operations) == 0

    def test_analyze_cte_method(self, write_detector):
        """Test the _analyze_cte method"""
        # CTE with write operation
        cte_write_query = """
        WITH updated AS (
            UPDATE users SET status = 'active'
            RETURNING *
        )
        SELECT * FROM updated
        """
        parsed_cte_write = sqlparse.parse(cte_write_query)[0]
        assert write_detector._analyze_cte(parsed_cte_write) is True

        # CTE with only SELECT
        cte_select_query = """
        WITH user_stats AS (
            SELECT user_id, COUNT(*) as count
            FROM orders
            GROUP BY user_id
        )
        SELECT * FROM user_stats
        """
        parsed_cte_select = sqlparse.parse(cte_select_query)[0]
        assert write_detector._analyze_cte(parsed_cte_select) is False
