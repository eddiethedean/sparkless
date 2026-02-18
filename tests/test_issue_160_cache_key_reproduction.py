"""
Test to reproduce issue #160 by testing cache key behavior.

v4: Uses BackendFactory; backend removed. Skipped.
"""

import os
import pytest

pytest.skip(
    "v4: backend package removed; tests use Polars materializer",
    allow_module_level=True,
)
from sparkless import SparkSession, functions as F
from sparkless.backend.factory import BackendFactory


@pytest.fixture
def enable_cache():
    """Enable expression translation cache for this test."""
    os.environ["MOCK_SPARK_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"] = "1"
    yield
    if "MOCK_SPARK_FEATURE_enable_expression_translation_cache" in os.environ:
        del os.environ["MOCK_SPARK_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"]


def test_cache_key_includes_column_names(enable_cache):
    """
    Test that cache keys properly include column names.

    If cache keys don't include column names, then:
    - F.regexp_replace(F.col("col1"), ...) and F.regexp_replace(F.col("col2"), ...)
    - would have the same cache key and reuse the wrong cached expression
    """
    spark = SparkSession.builder.appName("cache_key_test").getOrCreate()

    # Create DataFrame with multiple columns
    data = [("val1", "val2", "val3") for _ in range(10)]
    df = spark.createDataFrame(data, ["col1", "col2", "col3"])

    # Get materializer to access translator

    materializer = BackendFactory.create_materializer("polars")
    translator = materializer.translator

    # Create expression with col1 - this will get cached
    expr1 = F.regexp_replace(F.col("col1"), r"\.\d+", "")
    cache_key1 = translator._build_cache_key(expr1)
    polars_expr1 = translator.translate(expr1)

    print(f"Cache key 1: {cache_key1}")
    print(f"Polars expr 1: {polars_expr1}")
    print(f"Cache size: {len(translator._translation_cache)}")

    # Create similar expression with col2 - should have different cache key
    expr2 = F.regexp_replace(F.col("col2"), r"\.\d+", "")
    cache_key2 = translator._build_cache_key(expr2)
    polars_expr2 = translator.translate(expr2)

    print(f"Cache key 2: {cache_key2}")
    print(f"Polars expr 2: {polars_expr2}")
    print(f"Cache size: {len(translator._translation_cache)}")

    # Verify cache keys are different
    assert cache_key1 != cache_key2, (
        "Cache keys should be different for different column names"
    )

    # Now drop col1
    df_dropped = df.select("col2", "col3")
    assert "col1" not in df_dropped.columns

    # Try to use expression with col2 - should work and use cached version
    # But if cache key was wrong, it might try to use the cached expr1 which references col1
    result = df_dropped.withColumn(
        "col2_cleaned", F.regexp_replace(F.col("col2"), r"\.\d+", "")
    )

    # This should work - col2 still exists
    count = result.count()
    assert count == 10

    spark.stop()


def test_nested_expression_caching_after_drop(enable_cache):
    """
    Test that nested expressions are properly cached and don't reference dropped columns.

    Scenario:
    1. Create nested expression: F.to_timestamp(F.regexp_replace(F.col("impression_date"), ...))
    2. The inner F.regexp_replace(...) gets cached
    3. Drop impression_date column
    4. Try to use a similar nested expression - the cached inner expression might reference dropped column
    """
    spark = SparkSession.builder.appName("nested_cache_test").getOrCreate()

    # Create test data
    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Get materializer

    materializer = BackendFactory.create_materializer("polars")
    translator = materializer.translator

    # Create nested expression - inner expression will get cached
    inner_expr = F.regexp_replace(F.col("impression_date"), r"\.\d+", "")
    nested_expr = F.to_timestamp(inner_expr.cast("string"), "yyyy-MM-dd'T'HH:mm:ss")

    # Translate it - this will cache the inner expression
    translator.translate(nested_expr)
    print(f"Cache size after nested expr: {len(translator._translation_cache)}")

    # Check what's in the cache
    for key, value in translator._translation_cache.items():
        print(f"Cached key: {key}")
        print(f"Cached value: {value}")

    # Now drop the column
    df_dropped = df.select("impression_id", "campaign_id")
    assert "impression_date" not in df_dropped.columns

    # Clear cache to simulate what the fix does
    # But first, let's see if we can trigger the bug by NOT clearing the cache
    # Actually, we can't use the cached expression because the column is dropped
    # But maybe the issue is that the cached expression is still referenced somewhere?

    spark.stop()


def test_reuse_cached_expression_after_drop(enable_cache):
    """
    Test reusing a cached expression after the column it references is dropped.

    This might trigger the bug if:
    1. Expression gets cached during first use
    2. Column is dropped
    3. Same expression is tried to be used again (maybe in error handling or retry)
    """
    spark = SparkSession.builder.appName("reuse_cache_test").getOrCreate()

    data = [("imp_001", "2024-01-15T10:30:45.123456", "campaign_1") for _ in range(200)]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Create expression and use it - gets cached
    df1 = df.withColumn(
        "date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )

    # Materialize to trigger caching
    _ = df1.count()

    # Get materializer to check cache

    materializer = BackendFactory.create_materializer("polars")
    translator = materializer.translator
    print(f"Cache size: {len(translator._translation_cache)}")

    # Now drop the column
    df_dropped = df1.select("impression_id", "campaign_id", "date_parsed")
    assert "impression_date" not in df_dropped.columns

    # Try to create a new DataFrame with the same structure but without impression_date
    # and see if cached expressions cause issues
    spark.createDataFrame([("imp_002", "campaign_2")], ["impression_id", "campaign_id"])

    # Try to use a similar expression but with a different column
    # If cache key is wrong, it might use the cached expression for impression_date
    # Create a new DataFrame with a date column to test with
    data3 = [("imp_002", "2024-01-16T10:30:45", "campaign_2")]
    df3 = spark.createDataFrame(data3, ["impression_id", "other_date", "campaign_id"])

    try:
        df4 = df3.withColumn(
            "date_parsed",
            F.to_timestamp(
                F.regexp_replace(F.col("other_date"), r"\.\d+", "").cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        # This should work - other_date exists and is a valid date string
        count = df4.count()
        assert count == 1
    except Exception as e:
        error_msg = str(e).lower()
        if "impression_date" in error_msg and (
            "cannot resolve" in error_msg or "not found" in error_msg
        ):
            pytest.fail(
                f"Bug reproduced! Cached expression referenced dropped column: {e}"
            )
        raise

    spark.stop()
