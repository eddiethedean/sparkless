"""
Test to force reproduction of issue #160 by manipulating the cache directly.

v4: Uses BackendFactory; backend removed. Skipped.
"""

import pytest

pytest.skip(
    "v4: backend package removed; tests use Polars materializer",
    allow_module_level=True,
)
from sparkless import SparkSession, functions as F
from sparkless.core.exceptions.operation import SparkColumnNotFoundError


def test_force_bug_by_manipulating_cache():
    """
    Force the bug by directly manipulating the expression cache.

    This test:
    1. Creates a materializer (which has a translator with a cache)
    2. Translates an expression that references a column (gets cached)
    3. Manually drops the column from a DataFrame
    4. Tries to use the cached expression which references the dropped column
    """
    spark = SparkSession.builder.appName("force_bug").getOrCreate()

    # Create test data
    data = [
        ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1"),
        ("imp_002", "2024-01-16T14:20:30.789012", "campaign_2"),
    ]

    bronze_df = spark.createDataFrame(
        data,
        [
            "impression_id",
            "impression_date",  # This column will be dropped
            "campaign_id",
        ],
    )

    # Create a materializer to access the translator and cache
    from sparkless.backend.factory import BackendFactory

    materializer = BackendFactory.create_materializer("polars")

    # Create an expression that references impression_date
    # This will get cached when translated
    expr = F.regexp_replace(F.col("impression_date"), r"\.\d+", "")

    # Translate the expression - this will cache it
    materializer.translator.translate(expr)

    # Now drop the column via select
    bronze_df.select(
        "impression_id",
        "campaign_id",
        # impression_date is DROPPED
    )

    # The cached expression still references "impression_date"
    # If we try to use it on the DataFrame without impression_date, it should fail
    # But we can't directly use it because the materializer processes operations in order

    # Instead, let's try to create a scenario where the cached expression is reused
    # by creating a new DataFrame with the same structure but without impression_date
    # and then trying to use an expression that would match the cache key

    spark.stop()


def test_bug_with_write_operation():
    """
    The issue mentions the bug occurs during "materialization, validation, or write operations".
    Let's try a write operation.
    """
    spark = SparkSession.builder.appName("bug_write").getOrCreate()

    data = [
        ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1"),
        ("imp_002", "2024-01-16T14:20:30.789012", "campaign_2"),
    ]

    bronze_df = spark.createDataFrame(
        data,
        [
            "impression_id",
            "impression_date",
            "campaign_id",
        ],
    )

    # Use impression_date then drop it
    silver_df = bronze_df.withColumn(
        "impression_date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    ).select(
        "impression_id",
        "campaign_id",
        "impression_date_parsed",
        # impression_date is DROPPED
    )

    # Try to write - this might trigger the bug
    try:
        silver_df.write.mode("overwrite").saveAsTable("test_table")
        # If successful, read it back
        result = spark.table("test_table")
        count = result.count()
        assert count == 2
        # Clean up
        spark.sql("DROP TABLE IF EXISTS test_table")
    except SparkColumnNotFoundError as e:
        error_msg = str(e).lower()
        if "impression_date" in error_msg:
            pytest.fail(
                f"Bug reproduced during write! Got SparkColumnNotFoundError for dropped column 'impression_date': {e}"
            )
        raise

    spark.stop()
