"""
Tests for issue #393: sum() over window on string columns with numeric data.

PySpark coerces string columns to double when using sum(); Polars raises
InvalidOperationError: cum_sum operation not supported for dtype str.
Sparkless now casts string/Utf8 columns to Float64 before sum/avg (PySpark parity).
"""

from tests.fixtures.spark_imports import get_spark_imports


def _norm(val):
    """Normalize for backend-agnostic assertion."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val) if isinstance(val, float) else int(val)
    return val


class TestIssue393SumStringColumn:
    """Test sum()/avg() on string columns with numeric content (issue #393)."""

    def test_sum_string_column_partition_by_order_by(self, spark):
        """Exact scenario from issue #393: Value as string '10','20'."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Type": "A", "Value": "10"},
                {"Name": "Bob", "Type": "A", "Value": "20"},
            ]
        )
        w = Window().partitionBy("Type").orderBy("Type")
        df = df.withColumn("SumValue", F.sum(df.Value).over(w))
        rows = df.collect()
        assert len(rows) == 2
        for r in rows:
            assert _norm(r["SumValue"]) == 30.0

    def test_avg_string_column(self, spark):
        """avg() on string column with numeric content."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "10"},
                {"Type": "A", "Value": "20"},
                {"Type": "A", "Value": "30"},
            ]
        )
        w = Window().partitionBy("Type").orderBy("Type")
        df = df.withColumn("AvgValue", F.avg("Value").over(w))
        rows = df.collect()
        for r in rows:
            assert _norm(r["AvgValue"]) == 20.0

    def test_sum_string_column_running_sum(self, spark):
        """sum() on string column with orderBy different from partitionBy -> running sum."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Type": "A", "Name": "Alice", "Value": "10"},
                {"Type": "A", "Name": "Bob", "Value": "20"},
            ]
        )
        w = Window.partitionBy("Type").orderBy("Name")
        df = df.withColumn("SumValue", F.sum("Value").over(w))
        rows = df.collect()
        alice = next(r for r in rows if r["Name"] == "Alice")
        bob = next(r for r in rows if r["Name"] == "Bob")
        assert _norm(alice["SumValue"]) == 10.0
        assert _norm(bob["SumValue"]) == 30.0
