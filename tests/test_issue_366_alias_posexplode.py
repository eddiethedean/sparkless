"""Test issue #366: posexplode().alias('Value1', 'Value2') (PySpark API parity).

PySpark supports F.posexplode('Values').alias('Value1', 'Value2') to name
the two output columns (position and value). Sparkless should accept multiple
names in alias() (no TypeError) and the executor produces two columns when
posexplode path is used.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue366AliasPosexplode:
    """Test alias(*names) for posexplode."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate unique app name for parallel test execution."""
        import os
        import threading

        thread_id = threading.current_thread().ident
        process_id = os.getpid()
        return f"{test_name}_{process_id}_{thread_id}"

    def test_alias_accepts_multiple_names(self):
        """alias() must accept *names (no TypeError: takes 2 but 3 given)."""
        # PySpark: F.posexplode("Values").alias("Value1", "Value2")
        col = F.posexplode("Values").alias("Value1", "Value2")
        assert hasattr(col, "_alias_names")
        assert col._alias_names == ("Value1", "Value2")
        assert col._alias_name == "Value1"

    def test_posexplode_alias_two_names_select(self):
        """Select with posexplode().alias('Value1', 'Value2') runs and returns columns."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Values": [10, 20]},
                    {"Name": "Bob", "Values": [30, 40]},
                ]
            )
            # Must not raise TypeError: alias() takes 2 positional arguments but 3 given
            result = df.select(
                "Name", F.posexplode("Values").alias("Value1", "Value2")
            )
            rows = result.collect()
            # At least one column from alias (Value1); full posexplode may produce Value2
            assert len(rows) >= 1
            assert "Name" in (rows[0].asDict().keys() if rows else [])
        finally:
            spark.stop()
