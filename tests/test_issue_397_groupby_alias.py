"""
Tests for issue #397: groupBy with aliased columns.

PySpark accepts df.groupBy(F.col('Name').alias('Key')).agg(...).
Sparkless previously raised SparkColumnNotFoundError: cannot resolve 'Key'.
"""

from tests.fixtures.spark_imports import get_spark_imports


class TestIssue397GroupByAlias:
    """Test groupBy with aliased columns (issue #397)."""

    def test_groupby_alias_exact_issue(self, spark):
        """Exact scenario from issue #397."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Alice", "Value": 2},
                {"Name": "Bob", "Value": 3},
                {"Name": "Bob", "Value": 4},
            ]
        )
        df = df.groupBy(F.col("Name").alias("Key")).agg(F.sum("Value"))
        rows = df.collect()
        assert len(rows) == 2
        alice = next(r for r in rows if r["Key"] == "Alice")
        bob = next(r for r in rows if r["Key"] == "Bob")
        assert alice["sum(Value)"] == 3
        assert bob["sum(Value)"] == 7

    def test_groupby_alias_with_show(self, spark):
        """groupBy alias + show() as in issue example."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Alice", "Value": 2},
                {"Name": "Bob", "Value": 3},
            ]
        )
        df = df.groupBy(F.col("Name").alias("Key")).agg(F.sum("Value"))
        df.show()
        rows = df.collect()
        assert len(rows) == 2

    def test_groupby_multiple_cols_one_aliased(self, spark):
        """groupBy with mix of plain and aliased columns."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"A": "x", "B": 1, "Val": 10},
                {"A": "x", "B": 1, "Val": 20},
                {"A": "y", "B": 2, "Val": 30},
            ]
        )
        df = df.groupBy(F.col("A").alias("Key"), "B").agg(F.sum("Val"))
        rows = df.collect()
        assert len(rows) == 2
        key_b_pairs = [(r["Key"], r["B"]) for r in rows]
        assert ("x", 1) in key_b_pairs
        assert ("y", 2) in key_b_pairs
