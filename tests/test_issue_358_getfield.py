"""Test issue #358: Column.getField for array index (PySpark API parity).

PySpark supports F.col("ArrayVal").getField(0) for array element access.
This test verifies getField(int) matches getItem(int) and getField(str) for struct.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue358GetField:
    """Test Column.getField for array and struct access."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate unique app name for parallel test execution."""
        import os
        import threading

        thread_id = threading.current_thread().ident
        process_id = os.getpid()
        return f"{test_name}_{process_id}_{thread_id}"

    def test_getfield_array_index(self):
        """Test getField(0) on array column (issue example)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "ArrayVal": ["E1", "E2"]},
                    {"Name": "Bob", "ArrayVal": ["E3", "E4"]},
                ]
            )
            df = df.withColumn("Extract-Field", F.col("ArrayVal").getField(0))
            rows = df.collect()
            assert len(rows) == 2
            assert rows[0]["Extract-Field"] == "E1"
            assert rows[1]["Extract-Field"] == "E3"
        finally:
            spark.stop()

    def test_getfield_equivalent_to_getitem(self):
        """Test getField(int) matches getItem(int)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [{"arr": [10, 20, 30]}, {"arr": [40, 50]}]
            )
            df_getfield = df.withColumn("x", F.col("arr").getField(1))
            df_getitem = df.withColumn("x", F.col("arr").getItem(1))
            rows_gf = df_getfield.collect()
            rows_gi = df_getitem.collect()
            assert [r["x"] for r in rows_gf] == [r["x"] for r in rows_gi]
            assert [r["x"] for r in rows_gf] == [20, 50]
        finally:
            spark.stop()
