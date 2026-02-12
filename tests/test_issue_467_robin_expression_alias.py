"""
Tests for issue #467: Robin backend expression resolution for select/withColumn.

Robin resolves columns by name; Sparkless must pass Robin Column expressions with
alias applied (e.g. concat(...).alias("full_name")) so the output column is the
alias, not a column lookup. Similarly withColumn must pass the expression (e.g.
create_map(...)), not a reference to the new column name.
"""

import pytest

try:
    import robin_sparkless  # noqa: F401
except ImportError:
    robin_sparkless = None


def _robin_available() -> bool:
    return robin_sparkless is not None


def _is_robin_backend(session) -> bool:
    return getattr(session, "backend_type", None) == "robin"


class TestIssue467RobinExpressionAlias:
    """Select/withColumn must pass Robin expressions and apply alias."""

    def test_select_aliased_concat_robin(self, spark):
        """df.select(expr.alias('full_name')) produces one column named full_name."""
        if not _is_robin_backend(spark) or not _robin_available():
            pytest.skip(
                "Issue #467 tests require Robin backend (SPARKLESS_TEST_BACKEND=robin) "
                "and robin-sparkless installed"
            )
        from sparkless.sql import functions as F

        df = spark.createDataFrame(
            [("alice", "x"), ("bob", "y")],
            ["first_name", "last_name"],
        )
        result = df.select(
            F.concat(
                F.col("first_name"),
                F.lit(" "),
                F.col("last_name"),
            ).alias("full_name")
        )
        rows = result.collect()
        assert len(rows) == 2
        assert list(rows[0].asDict().keys()) == ["full_name"]
        assert rows[0]["full_name"] == "alice x"
        assert rows[1]["full_name"] == "bob y"

    def test_with_column_create_map_robin(self, spark):
        """df.withColumn('map_col', F.create_map(...)) adds column map_col.

        If Robin does not support create_map, we skip; the important fix is that
        we pass the expression (not a column reference to 'map_col') so Robin
        does not raise "Column 'map_col' not found".
        """
        if not _is_robin_backend(spark) or not _robin_available():
            pytest.skip(
                "Issue #467 tests require Robin backend (SPARKLESS_TEST_BACKEND=robin) "
                "and robin-sparkless installed"
            )
        from sparkless.sql import functions as F

        from sparkless.core.exceptions.operation import (
            SparkUnsupportedOperationError,
        )

        df = spark.createDataFrame(
            [(1, "a", 2, "b")],
            ["k1", "v1", "k2", "v2"],
        )
        try:
            result = df.withColumn(
                "map_col",
                F.create_map(
                    F.col("k1"),
                    F.col("v1"),
                    F.col("k2"),
                    F.col("v2"),
                ),
            )
        except SparkUnsupportedOperationError:
            pytest.skip("Robin backend does not support create_map yet")
        rows = result.collect()
        assert len(rows) == 1
        row = rows[0].asDict()
        assert "map_col" in row
        # Robin may return dict or map; key is that column exists and has key-value content
        map_val = row["map_col"]
        assert map_val is not None
        if isinstance(map_val, dict):
            assert map_val.get(1) == "a" or map_val.get("1") == "a"
            assert map_val.get(2) == "b" or map_val.get("2") == "b"
