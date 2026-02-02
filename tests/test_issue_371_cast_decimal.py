"""
Tests for issue #371: F.col(...).cast("Decimal(10,0)") raises ValueError.

PySpark supports .cast("Decimal(X,Y)"); Sparkless now parses that string in
_translate_cast and maps to DecimalType(precision, scale), then to Polars Float64.
"""


class TestIssue371CastDecimal:
    """Test cast to Decimal type from string."""

    def test_with_column_cast_decimal_10_0(self, spark):
        """Exact scenario from issue #371: withColumn + cast('Decimal(10,0)')."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Values": 10},
                {"Name": "Bob", "Values": 20},
            ]
        )
        df1 = df.withColumn("DecimalValue", F.col("Values").cast("Decimal(10,0)"))
        df1.show()
        rows = df1.collect()
        assert len(rows) == 2
        assert rows[0]["Name"] == "Alice" and rows[0]["Values"] == 10
        assert rows[0]["DecimalValue"] == 10.0  # Polars maps Decimal to Float64
        assert rows[1]["Name"] == "Bob" and rows[1]["Values"] == 20
        assert rows[1]["DecimalValue"] == 20.0

    def test_cast_decimal_lowercase(self, spark):
        """cast('decimal(10,0)') also works (case insensitive)."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"x": 5}])
        df1 = df.withColumn("d", F.col("x").cast("decimal(10,0)"))
        rows = df1.collect()
        assert len(rows) == 1 and rows[0]["d"] == 5.0
