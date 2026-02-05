"""
Tests for issue #421: join with different column names using F.col().

Sparkless raised ValueError when joining with Column-based comparisons
where column names differ (e.g. F.col("Key") == F.col("Name")).
PySpark resolves column names by which DataFrame contains them.
"""


class TestIssue421JoinColumnNames:
    """Test join with different column names using F.col() notation."""

    def test_join_different_column_names_exact_issue(self, spark, spark_backend):
        """Exact scenario from issue #421 - F.col('Key') == F.col('Name')."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports(spark_backend).F
        df1 = spark.createDataFrame(
            [{"Name": "Alice", "Value1": 5}, {"Name": "Bob", "Value1": 7}]
        )
        df2 = spark.createDataFrame(
            [{"Key": "Alice", "Value2": "A"}, {"Key": "Bob", "Value2": "B"}]
        )
        df = df1.join(df2, F.col("Key") == F.col("Name"), "left")
        rows = df.collect()
        assert len(rows) == 2
        by_name = {r["Name"]: r for r in rows}
        assert by_name["Alice"]["Key"] == "Alice"
        assert by_name["Alice"]["Value2"] == "A"
        assert by_name["Bob"]["Key"] == "Bob"
        assert by_name["Bob"]["Value2"] == "B"

    def test_join_different_column_names_reverse_order(self, spark, spark_backend):
        """F.col('Name') == F.col('Key') - reverse order of columns."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports(spark_backend).F
        df1 = spark.createDataFrame([{"Name": "Alice"}, {"Name": "Bob"}])
        df2 = spark.createDataFrame([{"Key": "Alice"}, {"Key": "Bob"}])
        df = df1.join(df2, F.col("Name") == F.col("Key"), "inner")
        rows = df.collect()
        assert len(rows) == 2
        names = {r["Name"] for r in rows}
        assert names == {"Alice", "Bob"}
        alice_row = next(r for r in rows if r["Name"] == "Alice")
        assert alice_row["Key"] == "Alice"

    def test_join_same_column_name_string_key_still_works(self, spark, spark_backend):
        """Join on same column name via string key - regression that we didn't break it."""
        df1 = spark.createDataFrame([{"id": 1, "x": 10}])
        df2 = spark.createDataFrame([{"id": 1, "y": 20}])
        df = df1.join(df2, "id", "inner")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["id"] == 1 and rows[0]["x"] == 10 and rows[0]["y"] == 20
