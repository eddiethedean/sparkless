"""
Tests for issue #367: F.array() and F.array([]) return empty array [].

PySpark supports F.array() and F.array([]) which return an empty array column.
Sparkless now supports both (PySpark parity).
"""

from sparkless.sql import functions as F


class TestIssue367ArrayEmpty:
    """Test F.array() and F.array([]) return empty array (issue #367)."""

    def test_array_no_args_returns_empty_array(self, spark):
        """Exact scenario from issue #367: withColumn + F.array()."""
        df = spark.createDataFrame([{"Name": "Alice"}, {"Name": "Bob"}])
        df = df.withColumn("NewArray", F.array())
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["NewArray"] == []
        assert rows[1]["NewArray"] == []

    def test_array_empty_list_returns_empty_array(self, spark):
        """F.array([]) returns empty array [] (issue #367)."""
        df = spark.createDataFrame([{"Name": "Alice"}, {"Name": "Bob"}])
        df = df.withColumn("NewArray", F.array([]))
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["NewArray"] == []
        assert rows[1]["NewArray"] == []

    def test_array_empty_show(self, spark):
        """Exact issue scenario: withColumn + F.array() + show()."""
        df = spark.createDataFrame([{"Name": "Alice"}, {"Name": "Bob"}])
        df = df.withColumn("NewArray", F.array())
        df.show()
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["Name"] == "Alice" and rows[0]["NewArray"] == []
        assert rows[1]["Name"] == "Bob" and rows[1]["NewArray"] == []

    def test_array_empty_in_select(self, spark):
        """F.array() in select statement."""
        df = spark.createDataFrame([{"id": 1}, {"id": 2}, {"id": 3}])
        result = df.select(F.col("id"), F.array().alias("empty_array"))
        rows = result.collect()
        assert len(rows) == 3
        for row in rows:
            assert row["empty_array"] == []
            assert "id" in row

    def test_array_empty_list_and_array_equivalent(self, spark):
        """F.array() and F.array([]) produce identical results."""
        df = spark.createDataFrame([{"id": 1}, {"id": 2}])
        result = df.select(
            F.col("id"),
            F.array().alias("arr_no_args"),
            F.array([]).alias("arr_empty_list"),
        )
        rows = result.collect()
        assert len(rows) == 2
        for row in rows:
            assert row["arr_no_args"] == []
            assert row["arr_empty_list"] == []
            assert row["arr_no_args"] == row["arr_empty_list"]

    def test_array_empty_after_filter(self, spark):
        """F.array() works after filter."""
        df = spark.createDataFrame(
            [
                {"id": 1, "value": 10},
                {"id": 2, "value": 20},
                {"id": 3, "value": 30},
            ]
        )
        result = df.filter(F.col("value") > 15).withColumn(
            "empty_array", F.array()
        )
        rows = result.collect()
        assert len(rows) == 2
        for row in rows:
            assert row["empty_array"] == []
            assert row["value"] > 15

    def test_array_empty_in_union(self, spark):
        """F.array() works with union."""
        df1 = spark.createDataFrame([{"id": 1, "val": "a"}])
        df2 = spark.createDataFrame([{"id": 2, "val": "b"}])
        df1_with_arr = df1.withColumn("arr", F.array())
        df2_with_arr = df2.withColumn("arr", F.array())
        result = df1_with_arr.union(df2_with_arr)
        rows = result.collect()
        assert len(rows) == 2
        for row in rows:
            assert row["arr"] == []
