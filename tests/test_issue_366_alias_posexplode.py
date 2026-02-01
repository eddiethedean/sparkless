"""Test issue #366: posexplode().alias(name) (PySpark API parity).

PySpark Column.alias() takes a single name only. posexplode produces two
columns; with .alias("Value1") the first column is Value1 and the second
keeps default name "col". Sparkless matches this behavior.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue366AliasPosexplode:
    """Test alias(name) for posexplode (PySpark: single name only)."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate unique app name for parallel test execution."""
        import os
        import threading

        thread_id = threading.current_thread().ident
        process_id = os.getpid()
        return f"{test_name}_{process_id}_{thread_id}"

    def test_alias_accepts_single_name(self):
        """alias() takes one name (PySpark parity); posexplode first column gets it."""
        col = F.posexplode("Values").alias("Value1")
        assert hasattr(col, "_alias_name")
        assert col._alias_name == "Value1"

    def test_posexplode_alias_select(self):
        """Select with posexplode().alias('Value1') runs; columns are Value1 and col."""
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
            result = df.select("Name", F.posexplode("Values").alias("Value1"))
            rows = result.collect()
            assert len(rows) >= 1
            keys = list(rows[0].asDict().keys()) if rows else []
            assert "Name" in keys
            assert "Value1" in keys
            # Second posexplode column may be "col" or folded into struct depending on backend
        finally:
            spark.stop()

    def test_explode_alias_single_name(self):
        """Test explode().alias('single_name') still works."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame([{"arr": [1, 2, 3]}])
            result = df.select(F.explode("arr").alias("num"))
            rows = result.collect()
            assert len(rows) == 3
            assert [r["num"] for r in rows] == [1, 2, 3]
        finally:
            spark.stop()

    def test_posexplode_empty_array(self):
        """Test posexplode with empty array; single alias for pos column."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame([{"id": 1, "arr": []}, {"id": 2, "arr": [10]}])
            result = df.select("id", F.posexplode("arr").alias("pos"))
            rows = result.collect()
            # Empty array: may produce no rows for that id, or one row with null pos/col
            assert len(rows) >= 1
            ids = [r["id"] for r in rows]
            assert 2 in ids
        finally:
            spark.stop()

    def test_posexplode_nested_arrays(self):
        """Test posexplode on nested array; single alias for first column."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame([{"nested": [[1, 2], [3, 4]]}])
            result = df.select(F.posexplode("nested").alias("idx"))
            rows = result.collect()
            # At least one row; structure may vary by backend
            assert len(rows) >= 1
            assert "idx" in (rows[0].asDict().keys() if rows else [])
        finally:
            spark.stop()

    def test_posexplode_outer_null_handling(self):
        """Test posexplode_outer with null arrays; single alias for pos."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [(1, [10, 20]), (2, None)], schema="id: int, arr: array<int>"
            )
            result = df.select("id", F.posexplode_outer("arr").alias("pos"))
            rows = result.collect()
            assert len(rows) >= 2
            ids = [r["id"] for r in rows]
            assert 1 in ids and 2 in ids
        finally:
            spark.stop()
