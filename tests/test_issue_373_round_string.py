"""Test issue #373: F.round() on string column (PySpark implicit cast).

PySpark supports F.round() on string columns that contain numeric values.
Sparkless should cast to numeric before rounding to match PySpark.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue373RoundString:
    """Test round() on string column."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate unique app name for parallel test execution."""
        import os
        import threading

        thread_id = threading.current_thread().ident
        process_id = os.getpid()
        return f"{test_name}_{process_id}_{thread_id}"

    def test_round_string_column(self):
        """Test round on string column with numeric content (issue example)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "10.4"},
                    {"Name": "Bob", "Value": "9.6"},
                ]
            )
            df = df.withColumn("Value", F.round("Value"))
            rows = df.collect()
            assert len(rows) == 2
            # PySpark: 10.4 -> 10.0, 9.6 -> 10.0
            assert rows[0]["Value"] == 10.0
            assert rows[1]["Value"] == 10.0
        finally:
            spark.stop()
