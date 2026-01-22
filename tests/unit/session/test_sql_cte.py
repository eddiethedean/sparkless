"""Tests for CTE (WITH clause) support in SQL executor."""

from typing import TYPE_CHECKING, cast

import pytest

if TYPE_CHECKING:
    from sparkless.dataframe import DataFrame


class TestCTEBasic:
    """Test basic CTE (WITH clause) functionality."""

    def test_simple_cte(self, spark) -> None:
        """Test simple CTE execution."""
        # Create source data
        rows = [
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
            {"id": 3, "value": 50},
        ]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("source_table")

        # Execute query with CTE
        result = spark.sql("""
            WITH filtered_data AS (
                SELECT id, value FROM source_table WHERE value > 75
            )
            SELECT * FROM filtered_data
        """)

        collected = result.collect()
        assert len(collected) == 2
        values = [row["value"] for row in collected]
        assert all(v > 75 for v in values)

    def test_cte_with_column_selection(self, spark) -> None:
        """Test CTE selecting specific columns."""
        rows = [{"id": 1, "amount": 100, "name": "test"}]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("amounts_table")

        result = spark.sql("""
            WITH selected AS (
                SELECT id, amount FROM amounts_table
            )
            SELECT * FROM selected
        """)

        collected = result.collect()
        assert len(collected) == 1
        assert collected[0]["id"] == 1
        assert collected[0]["amount"] == 100


class TestCTEMultiple:
    """Test multiple CTEs in single query."""

    def test_multiple_ctes(self, spark) -> None:
        """Test multiple CTEs in single query."""
        rows = [
            {"id": 1, "amount": 100, "category": "A"},
            {"id": 2, "amount": 50, "category": "B"},
            {"id": 3, "amount": 150, "category": "A"},
        ]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("transactions")

        result = spark.sql("""
            WITH high_value AS (
                SELECT id, amount, category FROM transactions WHERE amount > 75
            ),
            category_a AS (
                SELECT id, amount FROM high_value WHERE category = 'A'
            )
            SELECT * FROM category_a
        """)

        collected = result.collect()
        # high_value has id=1 (100) and id=3 (150), category_a filters to category A
        assert len(collected) == 2
        amounts = sorted([row["amount"] for row in collected])
        assert amounts == [100, 150]

    def test_cte_chain(self, spark) -> None:
        """Test CTE that references another CTE."""
        rows = [
            {"id": 1, "value": 10, "status": "active"},
            {"id": 2, "value": 20, "status": "inactive"},
            {"id": 3, "value": 30, "status": "active"},
        ]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("base_data")

        result = spark.sql("""
            WITH step1 AS (
                SELECT id, value, status FROM base_data WHERE status = 'active'
            ),
            step2 AS (
                SELECT id, value FROM step1 WHERE value > 15
            )
            SELECT * FROM step2
        """)

        collected = result.collect()
        # step1 has id=1 (10, active) and id=3 (30, active)
        # step2 filters value > 15, so only id=3 remains
        assert len(collected) == 1
        assert collected[0]["id"] == 3
        assert collected[0]["value"] == 30


class TestCTEWithFiltering:
    """Test CTEs with WHERE clauses."""

    def test_cte_with_where_on_main_query(self, spark) -> None:
        """Test CTE where main query has its own WHERE clause."""
        rows = [
            {"id": 1, "status": "active", "value": 100},
            {"id": 2, "status": "active", "value": 200},
            {"id": 3, "status": "inactive", "value": 300},
        ]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("items")

        result = spark.sql("""
            WITH active_items AS (
                SELECT id, value FROM items WHERE status = 'active'
            )
            SELECT * FROM active_items WHERE value > 150
        """)

        collected = result.collect()
        assert len(collected) == 1
        assert collected[0]["id"] == 2
        assert collected[0]["value"] == 200


class TestCTEWithAggregation:
    """Test CTEs with GROUP BY and aggregations."""

    def test_cte_with_group_by(self, spark) -> None:
        """Test CTE with GROUP BY in subquery."""
        rows = [
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
            {"category": "B", "value": 30},
        ]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("sales")

        result = spark.sql("""
            WITH category_totals AS (
                SELECT category, SUM(value) AS total FROM sales GROUP BY category
            )
            SELECT * FROM category_totals
        """)

        collected = result.collect()
        assert len(collected) == 2
        totals = {row["category"]: row["total"] for row in collected}
        assert totals["A"] == 30  # 10 + 20
        assert totals["B"] == 30


class TestCTEEdgeCases:
    """Test edge cases and error handling."""

    def test_cte_with_simple_filter(self, spark) -> None:
        """Test CTE with simple filter in WHERE clause."""
        rows = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
        ]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("filter_test")

        result = spark.sql("""
            WITH filtered AS (
                SELECT id, value FROM filter_test
                WHERE value > 15
            )
            SELECT * FROM filtered
        """)

        collected = result.collect()
        # id=2 (20) and id=3 (30) match value > 15
        assert len(collected) == 2
        values = sorted([row["value"] for row in collected])
        assert values == [20, 30]

    def test_cte_case_insensitive(self, spark) -> None:
        """Test that WITH keyword is case insensitive."""
        rows = [{"id": 1}]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("case_test")

        # Test with lowercase
        result = spark.sql("""
            with my_cte as (select id from case_test)
            select * from my_cte
        """)
        assert len(result.collect()) == 1

    def test_cte_whitespace_handling(self, spark) -> None:
        """Test CTE with various whitespace patterns."""
        rows = [{"id": 1, "value": 42}]
        df = spark.createDataFrame(rows)
        df.write.mode("overwrite").saveAsTable("whitespace_test")

        # Test with extra whitespace
        result = spark.sql("""
            WITH
                my_cte
            AS
            (
                SELECT id, value FROM whitespace_test
            )
            SELECT * FROM my_cte
        """)

        collected = result.collect()
        assert len(collected) == 1
        assert collected[0]["value"] == 42


class TestCTEParser:
    """Test CTE parsing specifically."""

    def test_parser_detects_with_query(self, spark) -> None:
        """Test that parser correctly identifies WITH query type."""
        from sparkless.session.sql.parser import SQLParser

        parser = SQLParser()

        # Test WITH detection
        query = "WITH cte AS (SELECT 1) SELECT * FROM cte"
        ast = parser.parse(query)
        assert ast.query_type == "WITH"

        # Test that SELECT is not detected as WITH
        query = "SELECT * FROM table"
        ast = parser.parse(query)
        assert ast.query_type == "SELECT"

    def test_parser_extracts_ctes(self, spark) -> None:
        """Test that parser correctly extracts CTE definitions."""
        from sparkless.session.sql.parser import SQLParser

        parser = SQLParser()

        query = """
            WITH cte1 AS (SELECT id FROM t1),
                 cte2 AS (SELECT name FROM t2)
            SELECT * FROM cte1
        """
        ast = parser.parse(query)

        assert "ctes" in ast.components
        ctes = ast.components["ctes"]
        assert len(ctes) == 2
        assert ctes[0]["name"] == "cte1"
        assert ctes[1]["name"] == "cte2"
        assert "SELECT * FROM cte1" in ast.components["main_query"]
