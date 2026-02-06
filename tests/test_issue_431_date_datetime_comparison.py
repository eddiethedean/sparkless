"""Test issue #431: date vs datetime comparison must not raise TypeError.

PySpark allows comparing date and datetime columns. Sparkless must coerce
date to datetime (at midnight) for comparison instead of raising
TypeError: can't compare datetime.datetime to datetime.date.

https://github.com/eddiethedean/sparkless/issues/431
"""

import datetime

from tests.fixtures.spark_imports import get_spark_imports


def test_date_less_than_datetime(spark, spark_backend):
    """F.col('Date') < F.col('DateTime') must filter correctly (#431)."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {
                "Name": "Alice",
                "DateTime": datetime.datetime(2023, 1, 1, 12, 0, 0),
                "Date": datetime.date(2024, 1, 1),
            },
            {
                "Name": "Bob",
                "DateTime": datetime.datetime(2024, 1, 1, 12, 0, 0),
                "Date": datetime.date(2024, 1, 1),
            },
        ]
    )
    result = df.filter(F_backend.col("Date") < F_backend.col("DateTime"))
    rows = result.collect()

    # Alice: Date 2024-01-01 < DateTime 2023-01-01 12:00 -> False (filtered out)
    # Bob: Date 2024-01-01 < DateTime 2024-01-01 12:00 -> True (kept)
    assert len(rows) == 1
    assert rows[0]["Name"] == "Bob"
    assert rows[0]["Date"] == datetime.date(2024, 1, 1)
    assert rows[0]["DateTime"] == datetime.datetime(2024, 1, 1, 12, 0, 0)


def test_datetime_greater_than_date(spark, spark_backend):
    """F.col('DateTime') > F.col('Date') must work."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {
                "dt": datetime.datetime(2024, 6, 15, 10, 0, 0),
                "d": datetime.date(2024, 1, 1),
            },
        ]
    )
    result = df.filter(F_backend.col("dt") > F_backend.col("d"))
    rows = result.collect()
    assert len(rows) == 1


def test_date_eq_datetime(spark, spark_backend):
    """Date equals datetime at midnight."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {
                "d": datetime.date(2024, 1, 1),
                "dt": datetime.datetime(2024, 1, 1, 0, 0, 0),
            },
            {
                "d": datetime.date(2024, 1, 1),
                "dt": datetime.datetime(2024, 1, 1, 12, 0, 0),
            },
        ]
    )
    result = df.filter(F_backend.col("d") == F_backend.col("dt"))
    rows = result.collect()
    # Date 2024-01-01 == datetime 2024-01-01 00:00:00 -> True
    # Date 2024-01-01 == datetime 2024-01-01 12:00:00 -> False
    assert len(rows) == 1
    assert rows[0]["dt"] == datetime.datetime(2024, 1, 1, 0, 0, 0)
