"""
Unit tests for SQL CTE (WITH clause) support.
"""

import pytest
from sparkless.sql import SparkSession


class TestSQLCTE:
    """Test cases for Common Table Expressions (CTEs)."""

    def test_simple_cte(self):
        """Test basic CTE with single CTE definition."""
        spark = SparkSession("TestApp")
        try:
            # Create test data
            data = [{"id": 1, "value": 50}, {"id": 2, "value": 75}, {"id": 3, "value": 100}]
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView("source_table")

            # Execute CTE query
            result = spark.sql(
                """
                WITH filtered_data AS (
                    SELECT id, value FROM source_table WHERE value > 75
                )
                SELECT * FROM filtered_data
                """
            )

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["id"] == 3
            assert rows[0]["value"] == 100
        finally:
            spark.stop()

    def test_multiple_ctes(self):
        """Test CTE with multiple CTE definitions."""
        spark = SparkSession("TestApp")
        try:
            # Create test data
            data = [{"id": 1, "value": 50}, {"id": 2, "value": 75}, {"id": 3, "value": 100}]
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView("source_table")

            # Execute CTE query with multiple CTEs
            result = spark.sql(
                """
                WITH filtered_data AS (
                    SELECT id, value FROM source_table WHERE value > 50
                ),
                aggregated_data AS (
                    SELECT COUNT(*) as count FROM filtered_data
                )
                SELECT * FROM aggregated_data
                """
            )

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["count"] == 2
        finally:
            spark.stop()

    def test_chained_ctes(self):
        """Test CTE that references another CTE."""
        spark = SparkSession("TestApp")
        try:
            # Create test data
            data = [{"id": 1, "value": 50}, {"id": 2, "value": 75}, {"id": 3, "value": 100}]
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView("source_table")

            # Execute CTE query with chained CTEs
            result = spark.sql(
                """
                WITH filtered_data AS (
                    SELECT id, value FROM source_table WHERE value > 50
                ),
                doubled_data AS (
                    SELECT id, value * 2 as doubled_value FROM filtered_data
                )
                SELECT * FROM doubled_data
                """
            )

            rows = result.collect()
            assert len(rows) == 2
            # Check that values are doubled
            values = {row["doubled_value"] for row in rows}
            assert 150 in values  # 75 * 2
            assert 200 in values  # 100 * 2
        finally:
            spark.stop()

    def test_cte_with_aggregation(self):
        """Test CTE with aggregation."""
        spark = SparkSession("TestApp")
        try:
            # Create test data
            data = [{"id": 1, "value": 50}, {"id": 2, "value": 75}, {"id": 3, "value": 100}]
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView("source_table")

            # Execute CTE query with aggregation
            result = spark.sql(
                """
                WITH aggregated_data AS (
                    SELECT AVG(value) as avg_value FROM source_table
                )
                SELECT * FROM aggregated_data
                """
            )

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["avg_value"] == 75.0
        finally:
            spark.stop()

    def test_cte_with_filtering(self):
        """Test CTE with filtering in main query."""
        spark = SparkSession("TestApp")
        try:
            # Create test data
            data = [{"id": 1, "value": 50}, {"id": 2, "value": 75}, {"id": 3, "value": 100}]
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView("source_table")

            # Execute CTE query with filtering in main query
            result = spark.sql(
                """
                WITH filtered_data AS (
                    SELECT id, value FROM source_table WHERE value > 50
                )
                SELECT * FROM filtered_data WHERE value > 80
                """
            )

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["value"] == 100
        finally:
            spark.stop()

    def test_cte_case_insensitive(self):
        """Test that WITH keyword is case-insensitive."""
        spark = SparkSession("TestApp")
        try:
            # Create test data
            data = [{"id": 1, "value": 50}]
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView("source_table")

            # Execute CTE query with lowercase WITH
            result = spark.sql(
                """
                with filtered_data AS (
                    SELECT id, value FROM source_table
                )
                SELECT * FROM filtered_data
                """
            )

            rows = result.collect()
            assert len(rows) == 1
        finally:
            spark.stop()

    def test_cte_with_column_selection(self):
        """Test CTE with specific column selection."""
        spark = SparkSession("TestApp")
        try:
            # Create test data
            data = [{"id": 1, "value": 50, "name": "A"}, {"id": 2, "value": 75, "name": "B"}]
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView("source_table")

            # Execute CTE query with column selection
            result = spark.sql(
                """
                WITH filtered_data AS (
                    SELECT id, value FROM source_table WHERE value > 50
                )
                SELECT id FROM filtered_data
                """
            )

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["id"] == 2
            # Verify only id column is present
            assert "value" not in rows[0].asDict() or "value" not in dir(rows[0])
        finally:
            spark.stop()

    def test_cte_whitespace_handling(self):
        """Test CTE with various whitespace patterns."""
        spark = SparkSession("TestApp")
        try:
            # Create test data
            data = [{"id": 1, "value": 50}]
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView("source_table")

            # Execute CTE query with extra whitespace
            result = spark.sql(
                """
                WITH   filtered_data   AS   (
                    SELECT id, value FROM source_table
                )
                SELECT * FROM filtered_data
                """
            )

            rows = result.collect()
            assert len(rows) == 1
        finally:
            spark.stop()

    def test_cte_parser_detection(self):
        """Test that parser correctly detects WITH queries."""
        spark = SparkSession("TestApp")
        try:
            # Create test data
            data = [{"id": 1, "value": 50}]
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView("source_table")

            # Verify parser detects WITH
            parser = spark._sql_executor.parser
            ast = parser.parse(
                """
                WITH filtered_data AS (
                    SELECT id, value FROM source_table
                )
                SELECT * FROM filtered_data
                """
            )

            assert ast.query_type == "WITH"
            assert "ctes" in ast.components
            assert "main_query" in ast.components
            assert len(ast.components["ctes"]) == 1
            assert ast.components["ctes"][0]["name"] == "filtered_data"
        finally:
            spark.stop()

    def test_cte_with_order_by(self):
        """Test CTE with ORDER BY in main query."""
        spark = SparkSession("TestApp")
        try:
            # Create test data
            data = [{"id": 1, "value": 50}, {"id": 2, "value": 75}, {"id": 3, "value": 100}]
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView("source_table")

            # Execute CTE query with ORDER BY
            result = spark.sql(
                """
                WITH filtered_data AS (
                    SELECT id, value FROM source_table WHERE value > 50
                )
                SELECT * FROM filtered_data ORDER BY value DESC
                """
            )

            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["value"] == 100
            assert rows[1]["value"] == 75
        finally:
            spark.stop()

    def test_cte_with_limit(self):
        """Test CTE with LIMIT in main query."""
        spark = SparkSession("TestApp")
        try:
            # Create test data
            data = [{"id": 1, "value": 50}, {"id": 2, "value": 75}, {"id": 3, "value": 100}]
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView("source_table")

            # Execute CTE query with LIMIT
            result = spark.sql(
                """
                WITH filtered_data AS (
                    SELECT id, value FROM source_table WHERE value > 50
                )
                SELECT * FROM filtered_data LIMIT 1
                """
            )

            rows = result.collect()
            assert len(rows) == 1
        finally:
            spark.stop()
