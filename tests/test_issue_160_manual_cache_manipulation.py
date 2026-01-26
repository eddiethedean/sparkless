"""
Test to manually manipulate the cache to simulate the bug condition.

This test tries to force the bug by:
1. Manually caching an expression that references a column
2. Dropping that column
3. Trying to use the cached expression
"""

import os
import pytest
from sparkless import SparkSession, functions as F
from sparkless.backend.factory import BackendFactory
import polars as pl


@pytest.fixture
def enable_cache():
    """Enable expression translation cache."""
    os.environ["SPARKLESS_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"] = "1"
    # Clear config cache
    from sparkless import config

    config._load_feature_flag_overrides.cache_clear()
    yield
    if "SPARKLESS_FEATURE_enable_expression_translation_cache" in os.environ:
        del os.environ["SPARKLESS_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"]
    config._load_feature_flag_overrides.cache_clear()


def test_manual_cache_manipulation_to_force_bug(enable_cache):
    """
    Manually manipulate cache to try to force the bug.

    This test:
    1. Creates an expression and caches it (references impression_date)
    2. Drops the column
    3. Tries to use the cached expression on a DataFrame without that column
    """
    spark = SparkSession.builder.appName("manual_cache_test").getOrCreate()

    # Create DataFrame with column
    data = [("imp_001", "2024-01-15T10:30:45.123456", "campaign_1") for _ in range(200)]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Get materializer and translator
    materializer = BackendFactory.create_materializer("polars")
    translator = materializer.translator

    # Create expression that references impression_date
    expr = F.regexp_replace(F.col("impression_date"), r"\.\d+", "")

    # Translate it - this will cache it
    cached_polars_expr = translator.translate(expr)
    print(f"Cache size after translate: {len(translator._translation_cache)}")
    print(f"Cached expression: {cached_polars_expr}")

    # Verify it's cached
    # Cache key now includes context: (expr_key, context_key)
    expr_key = translator._build_cache_key(expr)
    # Check if any cache key starts with this expr_key
    cache_found = any(
        isinstance(key, tuple) and len(key) >= 1 and key[0] == expr_key
        for key in translator._translation_cache
    )
    assert cache_found, (
        f"Expression not found in cache. Cache keys: {list(translator._translation_cache.keys())[:5]}"
    )

    # Now drop the column via select
    df_dropped = df.select("impression_id", "campaign_id")
    assert "impression_date" not in df_dropped.columns

    # Try to manually use the cached Polars expression on the DataFrame without the column
    # This should fail because the expression references a column that doesn't exist
    # This demonstrates that cached expressions can reference dropped columns
    # Convert to Polars DataFrame
    polars_df = pl.DataFrame(
        [{"impression_id": "imp_001", "campaign_id": "campaign_1"}]
    )

    # Try to use the cached expression - this should fail with ColumnNotFoundError
    # This demonstrates the bug: cached expressions reference dropped columns
    with pytest.raises(pl.exceptions.ColumnNotFoundError) as exc_info:
        polars_df.with_columns(cached_polars_expr.alias("date_cleaned")).collect()

    # Verify the error mentions the dropped column
    error_msg = str(exc_info.value).lower()
    assert "impression_date" in error_msg, (
        f"Error should mention impression_date: {exc_info.value}"
    )

    spark.stop()


def test_cache_reuse_after_column_drop(enable_cache):
    """
    Test if cached expressions are reused after column drop.

    Scenario:
    1. Use expression with column A - gets cached
    2. Drop column A
    3. Use same expression structure again - should it use cache?
    """
    spark = SparkSession.builder.appName("cache_reuse_test").getOrCreate()

    data = [("imp_001", "2024-01-15T10:30:45.123456", "campaign_1") for _ in range(200)]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Use expression - gets cached
    df1 = df.withColumn(
        "date_cleaned", F.regexp_replace(F.col("impression_date"), r"\.\d+", "")
    )
    _ = df1.count()  # Materialize to trigger caching

    # Get translator to check cache
    materializer = BackendFactory.create_materializer("polars")
    translator = materializer.translator
    print(f"Cache size: {len(translator._translation_cache)}")

    # Drop the column
    df_dropped = df1.select("impression_id", "campaign_id", "date_cleaned")
    assert "impression_date" not in df_dropped.columns

    # Create a new DataFrame without impression_date
    df2 = spark.createDataFrame(
        [("imp_002", "campaign_2")], ["impression_id", "campaign_id"]
    )

    # Try to use a similar expression - if cache key is the same, it might use cached version
    # But the cached version references impression_date which doesn't exist
    try:
        # This should work because we're using campaign_id, not impression_date
        df3 = df2.withColumn(
            "campaign_cleaned", F.regexp_replace(F.col("campaign_id"), r"\.\d+", "")
        )
        _ = df3.count()
    except Exception as e:
        error_msg = str(e).lower()
        if "impression_date" in error_msg:
            pytest.fail(
                f"BUG REPRODUCED! Cached expression for impression_date was reused: {e}\n"
                f"This suggests the cache key doesn't properly distinguish column names."
            )
        raise

    spark.stop()
