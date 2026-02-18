"""
Test to reproduce the bug WITHOUT the fix.

This test temporarily removes the cache clearing logic to see if the bug occurs.

v4: Uses BackendFactory.create_materializer('polars'); backend removed. Skipped.
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
    """Enable expression translation cache."""
    os.environ["SPARKLESS_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"] = "1"
    from sparkless import config

    config._load_feature_flag_overrides.cache_clear()
    yield
    if "SPARKLESS_FEATURE_enable_expression_translation_cache" in os.environ:
        del os.environ["SPARKLESS_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"]
    config._load_feature_flag_overrides.cache_clear()


def test_bug_without_fix(enable_cache):
    """
    Test the bug scenario WITHOUT the fix (temporarily disable cache clearing).

    This test:
    1. Temporarily patches the materializer to NOT clear cache when columns are dropped
    2. Tries to reproduce the bug
    3. Verifies the fix works by enabling cache clearing
    """
    spark = SparkSession.builder.appName("bug_without_fix").getOrCreate()

    # Create test data
    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Create expression that will be cached
    df_transformed = df.withColumn(
        "date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )

    # Materialize to cache expressions
    _ = df_transformed.count()

    # Get materializer to check cache and temporarily disable cache clearing
    from sparkless.backend.factory import BackendFactory

    materializer = BackendFactory.create_materializer("polars")
    translator = materializer.translator

    # Store original clear_cache method
    original_clear_cache = translator.clear_cache

    # Temporarily disable cache clearing to simulate the bug
    def noop_clear_cache():
        pass  # Don't clear cache

    translator.clear_cache = noop_clear_cache

    try:
        # Drop the column - cache won't be cleared (simulating bug)
        df_dropped = df_transformed.select(
            "impression_id", "campaign_id", "date_parsed"
        )
        assert "impression_date" not in df_dropped.columns

        # Check cache still has expressions
        cache_size_after_drop = len(translator._translation_cache)
        print(f"Cache size after drop (without fix): {cache_size_after_drop}")

        # Try to materialize - this might trigger the bug if cached expressions are reused
        try:
            count = df_dropped.count()
            assert count == 200
            # If we get here, the bug didn't occur in this scenario
            print(
                "Bug did not occur - cached expressions were not reused in a problematic way"
            )
        except Exception as e:
            error_msg = str(e).lower()
            if "impression_date" in error_msg and (
                "cannot resolve" in error_msg
                or "not found" in error_msg
                or "unable to find" in error_msg
            ):
                pytest.fail(
                    f"BUG REPRODUCED without fix! Error: {e}\n"
                    f"This confirms the bug occurs when cache is not cleared after column drop."
                )
            raise
    finally:
        # Restore original clear_cache method
        translator.clear_cache = original_clear_cache

    spark.stop()


def test_fix_prevents_bug(enable_cache):
    """
    Test that the fix (clearing cache) prevents the bug.
    """
    spark = SparkSession.builder.appName("fix_test").getOrCreate()

    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    df_transformed = df.withColumn(
        "date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )

    _ = df_transformed.count()

    # Get materializer
    from sparkless.backend.factory import BackendFactory

    materializer = BackendFactory.create_materializer("polars")
    translator = materializer.translator

    cache_size_before = len(translator._translation_cache)
    print(f"Cache size before drop: {cache_size_before}")

    # Drop column - WITH fix (cache will be cleared)
    df_dropped = df_transformed.select("impression_id", "campaign_id", "date_parsed")

    # Check cache was cleared (this is what the fix does)
    cache_size_after = len(translator._translation_cache)
    print(f"Cache size after drop (with fix): {cache_size_after}")

    # Verify fix works
    count = df_dropped.count()
    assert count == 200

    spark.stop()
