"""Test issue #374: Aliased column references in joins (e.g. sm.brand_id).

PySpark allows F.col("sm.brand_id") in join conditions when the left
DataFrame is aliased as "sm". Sparkless should resolve alias.column to
the actual column name for join resolution.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue374JoinAliasedColumns:
    """Test join with aliased column references."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate unique app name for parallel test execution."""
        import os
        import threading

        thread_id = threading.current_thread().ident
        process_id = os.getpid()
        return f"{test_name}_{process_id}_{thread_id}"

    def test_join_aliased_column_refs(self):
        """Test join with F.col('sm.brand_id') == F.col('b.code') (issue example)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            mapping_df = spark.createDataFrame(
                [
                    {
                        "brand_id": "BRAND001",
                        "taxonomy_id": "TAX001",
                        "confidence": 0.95,
                    }
                ]
            )
            brands_df = spark.createDataFrame(
                [
                    {"id": "uuid-001", "code": "BRAND001"},
                ]
            )
            result = (
                mapping_df.alias("sm")
                .join(
                    brands_df.alias("b").select(
                        F.col("id").alias("brand_uuid"), F.col("code")
                    ),
                    F.col("sm.brand_id") == F.col("b.code"),
                    "inner",
                )
                .select(
                    F.col("brand_uuid"),
                    F.col("sm.taxonomy_id"),
                    F.col("sm.confidence"),
                )
            )
            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["brand_uuid"] == "uuid-001"
            # Select uses alias-prefixed names; result columns keep that (sm.taxonomy_id, sm.confidence)
            assert rows[0]["sm.taxonomy_id"] == "TAX001"
            assert rows[0]["sm.confidence"] == 0.95
        finally:
            spark.stop()
