"""Test issue #432: cast('timestamp') and cast('date') on already-typed columns.

PySpark allows .cast('timestamp') on datetime and .cast('date') on date as no-ops.
Sparkless must not raise SchemaError: expected String, got datetime[μs].

https://github.com/eddiethedean/sparkless/issues/432
"""

import datetime

from tests.fixtures.spark_imports import get_spark_imports


def test_cast_datetime_to_timestamp_noop(spark, spark_backend):
    """F.col('DateTime').cast('timestamp') on datetime column must not raise (#432)."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "DateTime": datetime.datetime(2023, 1, 1, 12, 0, 0)},
            {"Name": "Bob", "DateTime": datetime.datetime(2024, 1, 1, 12, 0, 0)},
        ]
    )
    result = df.withColumn("DateTime", F_backend.col("DateTime").cast("timestamp"))
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["DateTime"] == datetime.datetime(2023, 1, 1, 12, 0, 0)
    assert rows[1]["DateTime"] == datetime.datetime(2024, 1, 1, 12, 0, 0)


def test_cast_date_to_date_noop(spark, spark_backend):
    """F.col('Date').cast('date') on date column must not raise (#432)."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Date": datetime.date(2024, 1, 1)},
            {"Name": "Bob", "Date": datetime.date(2024, 6, 15)},
        ]
    )
    result = df.withColumn("Date", F_backend.col("Date").cast("date"))
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["Date"] == datetime.date(2024, 1, 1)
    assert rows[1]["Date"] == datetime.date(2024, 6, 15)


def test_exact_scenario_from_issue_432(spark, spark_backend):
    """Exact scenario from issue #432 - must not raise and show correctly."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {
                "Name": "Alice",
                "DateTime": datetime.datetime(2023, 1, 1, 12, 0, 0),
                "Date": datetime.date(2024, 1, 1),
                "DateTimeString": "2023-01-01 12:00:00",
                "DateString": "2024-01-01",
            },
            {
                "Name": "Bob",
                "DateTime": datetime.datetime(2024, 1, 1, 12, 0, 0),
                "Date": datetime.date(2024, 1, 1),
                "DateTimeString": "2023-01-01 12:00:00",
                "DateString": "2024-01-01",
            },
        ]
    )
    df = df.withColumn("DateTime", F_backend.col("DateTime").cast("timestamp"))
    df = df.withColumn("Date", F_backend.col("Date").cast("date"))
    df = df.withColumn(
        "DateTimeString", F_backend.col("DateTimeString").cast("timestamp")
    )
    df = df.withColumn("DateString", F_backend.col("DateString").cast("date"))
    df.show()  # Previously failed here with SchemaError
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["Name"] == "Alice"
    assert rows[1]["Name"] == "Bob"


def test_cast_string_to_timestamp_still_works(spark, spark_backend):
    """String->timestamp cast must still work."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"s": "2024-01-15 10:30:00"}])
    result = df.withColumn("s", F_backend.col("s").cast("timestamp"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["s"] == datetime.datetime(2024, 1, 15, 10, 30, 0)


def test_cast_string_to_date_still_works(spark, spark_backend):
    """String->date cast must still work."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"s": "2024-01-15"}])
    result = df.withColumn("s", F_backend.col("s").cast("date"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["s"] == datetime.date(2024, 1, 15)
