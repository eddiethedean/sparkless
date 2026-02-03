"""
Tests for issue #396: to_date() with cast to string on numeric column.

PySpark accepts F.to_date(F.col('DateNumber').cast('string'), 'yyyyMMdd') when
DateNumber is LongType. Sparkless was rejecting it because validation checked
the source column type instead of recognizing the cast-to-string.
"""

from tests.fixtures.spark_imports import get_spark_imports


class TestIssue396ToDateCast:
    """Test to_date with cast on numeric columns (issue #396)."""

    def test_to_date_cast_string_type_exact_issue(self, spark):
        """Exact scenario from issue #396: cast(StringType())."""
        imports = get_spark_imports()
        F, StringType = imports.F, imports.StringType
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "DateNumber": 20260203},
                {"Name": "Bob", "DateNumber": 20260203},
            ]
        )
        df = df.withColumn(
            "DateCol",
            F.to_date(F.col("DateNumber").cast(StringType()), "yyyyMMdd"),
        )
        rows = df.collect()
        assert len(rows) == 2
        assert str(rows[0]["DateCol"]) == "2026-02-03"

    def test_to_date_cast_string_literal(self, spark):
        """Alternate syntax: cast('string')."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"DateNumber": 20260115},
                {"DateNumber": 20260228},
            ]
        )
        df = df.withColumn(
            "DateCol", F.to_date(F.col("DateNumber").cast("string"), "yyyyMMdd")
        )
        rows = df.collect()
        assert len(rows) == 2
        assert str(rows[0]["DateCol"]) == "2026-01-15"
        assert str(rows[1]["DateCol"]) == "2026-02-28"

    def test_to_date_cast_with_show(self, spark):
        """withColumn + show() as in issue example."""
        imports = get_spark_imports()
        F, StringType = imports.F, imports.StringType
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "DateNumber": 20260203},
                {"Name": "Bob", "DateNumber": 20260203},
            ]
        )
        df = df.withColumn(
            "DateCol",
            F.to_date(F.col("DateNumber").cast(StringType()), "yyyyMMdd"),
        )
        df.show()
        rows = df.collect()
        assert len(rows) == 2
