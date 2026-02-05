"""Tests for issue #413: union() with createDataFrame(data, column_names).

Issue #413 reports that createDataFrame(data, ["id", "age", "name"]) with tuples
then union(other) fails with AnalysisException due to column name/position mismatch.
PySpark's union() matches by position, not by name.

These tests verify that:
- createDataFrame(data, column_names) produces columns in the given order
- union() matches by position and succeeds when schemas are compatible by position
"""

from sparkless.sql import SparkSession


class TestIssue413UnionCreateDataFrame:
    """Regression tests for union() with createDataFrame(data, cols) (issue #413)."""

    def test_union_createDataFrame_tuple_column_names(self) -> None:
        """Exact reproduction: union of DataFrames from createDataFrame(data, cols)."""
        builder = SparkSession.builder
        assert builder is not None
        spark = builder.appName("test_413").getOrCreate()
        try:
            cols = ["id", "age", "name"]
            data_a = [(1, 25, "alice"), (2, 30, "bob")]
            data_b = [(3, 22, "carol"), (4, 28, "dave")]

            df_a = spark.createDataFrame(data_a, cols)
            df_b = spark.createDataFrame(data_b, cols)
            result = df_a.union(df_b)

            rows = result.collect()
            assert len(rows) == 4
            assert (
                rows[0]["id"] == 1
                and rows[0]["age"] == 25
                and rows[0]["name"] == "alice"
            )
            assert (
                rows[1]["id"] == 2 and rows[1]["age"] == 30 and rows[1]["name"] == "bob"
            )
            assert (
                rows[2]["id"] == 3
                and rows[2]["age"] == 22
                and rows[2]["name"] == "carol"
            )
            assert (
                rows[3]["id"] == 4
                and rows[3]["age"] == 28
                and rows[3]["name"] == "dave"
            )
        finally:
            spark.stop()

    def test_createDataFrame_preserves_column_order(self) -> None:
        """createDataFrame(data, cols) should produce columns in the given order."""
        builder = SparkSession.builder
        assert builder is not None
        spark = builder.appName("test_413").getOrCreate()
        try:
            cols = ["id", "age", "name"]
            data = [(1, 25, "alice")]
            df = spark.createDataFrame(data, cols)
            assert df.columns == ["id", "age", "name"]
        finally:
            spark.stop()

    def test_union_different_column_order_by_position(self) -> None:
        """union() matches by position; different column names at same position should still work."""
        builder = SparkSession.builder
        assert builder is not None
        spark = builder.appName("test_413").getOrCreate()
        try:
            # df_a: id, age, name
            df_a = spark.createDataFrame([(1, 25, "alice")], ["id", "age", "name"])
            # df_b: x, y, z - same types, different names
            df_b = spark.createDataFrame([(2, 30, "bob")], ["x", "y", "z"])

            # PySpark union matches by position; result uses left's column names
            result = df_a.union(df_b)
            rows = result.collect()
            assert len(rows) == 2
            assert (
                rows[0]["id"] == 1
                and rows[0]["age"] == 25
                and rows[0]["name"] == "alice"
            )
            assert (
                rows[1]["id"] == 2 and rows[1]["age"] == 30 and rows[1]["name"] == "bob"
            )
        finally:
            spark.stop()
