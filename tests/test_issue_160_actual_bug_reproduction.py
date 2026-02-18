"""
Test to reproduce issue #160 by simulating the exact bug scenario.

v4: Uses BackendFactory; backend removed. Skipped.
"""

import os
import pytest

pytest.skip(
    "v4: backend package removed; tests use Polars materializer",
    allow_module_level=True,
)
from sparkless import SparkSession, functions as F


@pytest.fixture
def enable_cache():
    """Enable expression translation cache for this test."""
    os.environ["MOCK_SPARK_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"] = "1"
    yield
    if "MOCK_SPARK_FEATURE_enable_expression_translation_cache" in os.environ:
        del os.environ["MOCK_SPARK_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"]


def test_bug_with_lazy_polars_expression_reference(enable_cache):
    """
    Test if the bug occurs when cached Polars expressions reference dropped columns.

    The issue might be that when we:
    1. Translate F.col("impression_date") -> pl.col("impression_date") and cache it
    2. Drop "impression_date" column
    3. The cached pl.col("impression_date") expression is still in the cache
    4. If this cached expression is somehow reused, it will fail

    But actually, the cache is per-materializer instance, and each materialization
    creates a new materializer. So the cache shouldn't persist across materializations.

    Unless... maybe the issue is within a single materialization where:
    - withColumn translates and caches expressions
    - select drops columns
    - But the cached expressions are still referenced somewhere?
    """
    spark = SparkSession.builder.appName("lazy_expr_test").getOrCreate()

    # Create test data with 200 rows to ensure caching happens
    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Create a complex transformation that will cache expressions
    # Use impression_date in multiple ways to ensure it gets cached
    df_transformed = (
        df.withColumn(
            "date_cleaned", F.regexp_replace(F.col("impression_date"), r"\.\d+", "")
        )
        .withColumn(
            "date_parsed",
            F.to_timestamp(
                F.col("date_cleaned").cast("string"), "yyyy-MM-dd'T'HH:mm:ss"
            ),
        )
        .withColumn("hour", F.hour(F.col("date_parsed")))
        .withColumn("minute", F.minute(F.col("date_parsed")))
    )

    # Materialize to trigger caching
    _ = df_transformed.count()

    # Get the materializer to check cache
    from sparkless.backend.factory import BackendFactory

    materializer = BackendFactory.create_materializer("polars")
    translator = materializer.translator

    print(f"Cache size before drop: {len(translator._translation_cache)}")
    if len(translator._translation_cache) > 0:
        print(f"Sample cache keys: {list(translator._translation_cache.keys())[:3]}")
        # Check if any cached expressions reference impression_date
        for key, cached_expr in list(translator._translation_cache.items())[:5]:
            expr_str = str(cached_expr)
            if "impression_date" in expr_str:
                print(
                    f"Found cached expression referencing impression_date: {key} -> {expr_str}"
                )

    # Now drop the column
    df_dropped = df_transformed.select(
        "impression_id",
        "campaign_id",
        "date_parsed",
        "hour",
        "minute",
        # impression_date and date_cleaned are DROPPED
    )

    assert "impression_date" not in df_dropped.columns
    assert "date_cleaned" not in df_dropped.columns

    # Try to materialize - this is where the bug might occur
    # If cached expressions still reference impression_date, this might fail
    try:
        count = df_dropped.count()
        assert count == 200

        # Try to collect - this forces full materialization
        rows = df_dropped.collect()
        assert len(rows) == 200
    except Exception as e:
        error_msg = str(e).lower()
        if ("impression_date" in error_msg or "date_cleaned" in error_msg) and (
            "cannot resolve" in error_msg or "not found" in error_msg
        ):
            pytest.fail(
                f"BUG REPRODUCED! Got error about dropped column: {e}\n"
                f"This suggests cached expressions are referencing dropped columns."
            )
        raise

    spark.stop()


def test_bug_with_shared_translator_instance():
    """
    Test if bug occurs when translator instance is somehow shared.

    Actually, each materialization creates a new PolarsMaterializer, so translator
    instances shouldn't be shared. But let's test this anyway.
    """
    spark = SparkSession.builder.appName("shared_translator_test").getOrCreate()

    # Create first DataFrame and use impression_date
    data1 = [
        ("imp_001", "2024-01-15T10:30:45.123456", "campaign_1") for _ in range(200)
    ]
    df1 = spark.createDataFrame(
        data1, ["impression_id", "impression_date", "campaign_id"]
    )

    df1_transformed = df1.withColumn(
        "date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )

    # Materialize to trigger caching
    _ = df1_transformed.count()

    # Drop the column
    df1_transformed.select("impression_id", "campaign_id", "date_parsed")

    # Create a second DataFrame without impression_date
    data2 = [("imp_002", "campaign_2")]
    spark.createDataFrame(data2, ["impression_id", "campaign_id"])

    # Try to use a similar expression - if translator is shared and cache key is wrong,
    # it might use the cached expression for impression_date
    # Create a new DataFrame with a date column to test with
    data3 = [("imp_002", "2024-01-16T10:30:45", "campaign_2")]
    df3 = spark.createDataFrame(data3, ["impression_id", "other_date", "campaign_id"])

    try:
        df3_transformed = df3.withColumn(
            "date_parsed",
            F.to_timestamp(
                F.regexp_replace(F.col("other_date"), r"\.\d+", "").cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        _ = df3_transformed.count()
    except Exception as e:
        error_msg = str(e).lower()
        if "impression_date" in error_msg and (
            "cannot resolve" in error_msg or "not found" in error_msg
        ):
            pytest.fail(
                f"BUG REPRODUCED! Shared translator used wrong cached expression: {e}"
            )
        raise

    spark.stop()
