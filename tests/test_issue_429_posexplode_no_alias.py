"""Test issue #429: posexplode() without alias must not raise TypeError.

PySpark posexplode() returns pos and col columns by default when used without .alias().
Sparkless must not raise TypeError: object of type 'NoneType' has no len().

https://github.com/eddiethedean/sparkless/issues/429
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


def test_posexplode_without_alias_no_type_error():
    """posexplode() without alias must not raise TypeError (#429)."""
    spark = SparkSession.builder.appName("test_429").getOrCreate()
    try:
        df = spark.createDataFrame([
            {"Name": "Alice", "Values": [10, 20]},
            {"Name": "Bob", "Values": [30, 40]},
        ])
        # Without alias - previously raised TypeError in schema projection
        result = df.select("Name", F.posexplode("Values"))
        # Must not raise; schema should have pos and col (PySpark default names)
        assert "pos" in result.columns
        assert "col" in result.columns
        assert "Name" in result.columns
        result.show()  # Previously failed here
    finally:
        spark.stop()
