"""Test issue #368: F.DataFrame for reduce(F.DataFrame.union, dfs).

PySpark allows F.DataFrame.union so that reduce(F.DataFrame.union, dfs) works.
This test verifies sparkless.sql.functions exposes DataFrame for API parity.
"""

from functools import reduce

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue368FDataFrame:
    """Test F.DataFrame for union_all pattern."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate unique app name for parallel test execution."""
        import os
        import threading

        thread_id = threading.current_thread().ident
        process_id = os.getpid()
        return f"{test_name}_{process_id}_{thread_id}"

    def test_f_dataframe_union_reduce(self):
        """Test reduce(F.DataFrame.union, dfs) as in PySpark (issue example)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice"},
                    {"Name": "Bob"},
                ]
            )
            df2 = spark.createDataFrame(
                [
                    {"Name": "Charlie"},
                    {"Name": "Disco"},
                ]
            )

            def union_all(*dfs):
                return reduce(F.DataFrame.union, dfs)

            df = union_all(df1, df2)
            rows = df.collect()
            names = [r["Name"] for r in rows]
            assert names == ["Alice", "Bob", "Charlie", "Disco"]
        finally:
            spark.stop()

    def test_f_has_dataframe_attribute(self):
        """Test that F.DataFrame is available (module __getattr__)."""
        assert hasattr(F, "DataFrame")
        from sparkless.sql import DataFrame as DataFrameClass

        assert F.DataFrame is DataFrameClass
