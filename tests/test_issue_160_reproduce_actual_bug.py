"""
Test that reproduces the actual bug from issue #160.

This test demonstrates that when:
1. An expression is translated and cached (referencing a column)
2. That column is dropped via select()
3. The cached expression is reused (in a way that triggers the bug)

The bug occurs because the cached Polars expression still references the dropped column.

v4: Uses BackendFactory.create_materializer('polars'); backend removed. Skipped.
"""

import os
import pytest
pytest.skip("v4: backend package removed; tests use Polars materializer", allow_module_level=True)
from sparkless import SparkSession, functions as F


@pytest.fixture
def enable_cache():
    """Enable expression translation cache."""
    os.environ["SPARKLESS_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"] = "1"
    # Clear config cache to ensure it's read
    from sparkless import config

    config._load_feature_flag_overrides.cache_clear()
    yield
    if "SPARKLESS_FEATURE_enable_expression_translation_cache" in os.environ:
        del os.environ["SPARKLESS_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"]
    config._load_feature_flag_overrides.cache_clear()


def test_bug_reproduction_with_cache_enabled(enable_cache):
    """
    Reproduce the bug: cached expressions reference dropped columns.

    This test reproduces the exact scenario from issue #160:
    1. Create DataFrame with impression_date
    2. Use expression that references impression_date (gets cached)
    3. Drop impression_date via select()
    4. Try to materialize - cached expression might be reused and fail

    The bug occurs when the expression cache stores Polars expressions that
    reference column names directly. When columns are dropped, these cached
    expressions become invalid but may still be used.
    """
    spark = SparkSession.builder.appName("bug_reproduction").getOrCreate()

    # Create test data with 200 rows to ensure caching happens
    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Create expression that references impression_date
    # This expression will be translated and cached
    expr = F.regexp_replace(F.col("impression_date"), r"\.\d+", "")

    # Use it in withColumn - this translates and caches the expression
    df_with_expr = df.withColumn("date_cleaned", expr)

    # Materialize to ensure expression is translated and cached
    _ = df_with_expr.count()

    # Verify cache has the expression
    from sparkless.backend.factory import BackendFactory

    materializer = BackendFactory.create_materializer("polars")
    translator = materializer.translator
    cache_size_before = len(translator._translation_cache)
    print(f"Cache size before drop: {cache_size_before}")

    # Now drop the column via select
    df_dropped = df_with_expr.select("impression_id", "campaign_id", "date_cleaned")
    assert "impression_date" not in df_dropped.columns

    # The bug: The cached expression for F.regexp_replace(F.col("impression_date"), ...)
    # still references "impression_date" in the Polars expression (pl.col("impression_date"))
    # If this cached expression is somehow reused or re-evaluated, it will fail

    # Try to materialize the dropped DataFrame
    # This should work because date_cleaned was already computed
    try:
        count = df_dropped.count()
        assert count == 200

        rows = df_dropped.collect()
        assert len(rows) == 200
    except Exception as e:
        error_msg = str(e).lower()
        if "impression_date" in error_msg and (
            "cannot resolve" in error_msg
            or "not found" in error_msg
            or "unable to find" in error_msg
        ):
            pytest.fail(
                f"BUG REPRODUCED! Cached expression referenced dropped column: {e}\n"
                f"This confirms that cached Polars expressions can reference dropped columns."
            )
        raise

    spark.stop()


def test_bug_with_nested_expression_cache(enable_cache):
    """
    Test if nested expressions in cache cause the bug.

    The issue might occur when:
    1. Nested expression F.to_timestamp(F.regexp_replace(F.col("impression_date"), ...)) is cached
    2. The inner F.regexp_replace(...) expression is also cached separately
    3. Column is dropped
    4. Cached inner expression still references dropped column
    """
    spark = SparkSession.builder.appName("nested_cache_bug").getOrCreate()

    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Create nested expression - both outer and inner expressions get cached
    df_transformed = df.withColumn(
        "date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )

    # Materialize to cache expressions
    _ = df_transformed.count()

    # Get translator to verify cache
    from sparkless.backend.factory import BackendFactory

    materializer = BackendFactory.create_materializer("polars")
    translator = materializer.translator
    print(f"Cache size: {len(translator._translation_cache)}")

    # Check if cache contains expressions referencing impression_date
    for key, cached_expr in list(translator._translation_cache.items())[:5]:
        expr_str = str(cached_expr)
        if "impression_date" in expr_str:
            print(f"Found cached expression with impression_date: {key} -> {expr_str}")

    # Drop the column
    df_dropped = df_transformed.select("impression_id", "campaign_id", "date_parsed")
    assert "impression_date" not in df_dropped.columns

    # Try to materialize - if cached expressions reference dropped column, this might fail
    try:
        count = df_dropped.count()
        assert count == 200
    except Exception as e:
        error_msg = str(e).lower()
        if "impression_date" in error_msg and (
            "cannot resolve" in error_msg
            or "not found" in error_msg
            or "unable to find" in error_msg
        ):
            pytest.fail(
                f"BUG REPRODUCED! Nested cached expression referenced dropped column: {e}"
            )
        raise

    spark.stop()
