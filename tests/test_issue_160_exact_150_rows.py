"""
Test with exactly 150 rows to match the issue description threshold.

v4: Uses BackendFactory; backend removed. Skipped.
"""

import os
import pytest
pytest.skip("v4: backend package removed; tests use Polars materializer", allow_module_level=True)
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


def test_exactly_150_rows_without_fix(enable_cache):
    """
    Test with exactly 150 rows (the threshold mentioned in the issue).

    Temporarily disable the fix to see if the bug occurs.
    """
    spark = SparkSession.builder.appName("exact_150_rows").getOrCreate()

    # Create data with EXACTLY 150 rows (the threshold mentioned in issue)
    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(150)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Temporarily disable cache clearing to test
    from sparkless.backend.factory import BackendFactory

    materializer = BackendFactory.create_materializer("polars")
    translator = materializer.translator
    original_clear_cache = translator.clear_cache

    def noop_clear_cache():
        pass  # Don't clear cache

    translator.clear_cache = noop_clear_cache

    try:
        # Use impression_date then drop it
        df_transformed = df.withColumn(
            "date_parsed",
            F.to_timestamp(
                F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        ).select("impression_id", "campaign_id", "date_parsed")

        assert "impression_date" not in df_transformed.columns

        # Try to materialize
        try:
            count = df_transformed.count()
            assert count == 150
            print("Test passed - bug did not occur with 150 rows")
        except Exception as e:
            error_msg = str(e).lower()
            if "impression_date" in error_msg and (
                "cannot resolve" in error_msg
                or "not found" in error_msg
                or "unable to find" in error_msg
            ):
                pytest.fail(f"BUG REPRODUCED with 150 rows! Error: {e}")
            raise
    finally:
        translator.clear_cache = original_clear_cache

    spark.stop()


def test_149_rows_vs_150_rows(enable_cache):
    """
    Test if there's a difference between 149 and 150 rows.
    """
    spark = SparkSession.builder.appName("149_vs_150").getOrCreate()

    # Test with 149 rows
    data_149 = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(149)
    ]
    df_149 = spark.createDataFrame(
        data_149, ["impression_id", "impression_date", "campaign_id"]
    )

    df_149_result = df_149.withColumn(
        "date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    ).select("impression_id", "campaign_id", "date_parsed")

    count_149 = df_149_result.count()
    assert count_149 == 149

    # Test with 150 rows
    data_150 = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(150)
    ]
    df_150 = spark.createDataFrame(
        data_150, ["impression_id", "impression_date", "campaign_id"]
    )

    df_150_result = df_150.withColumn(
        "date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    ).select("impression_id", "campaign_id", "date_parsed")

    count_150 = df_150_result.count()
    assert count_150 == 150

    spark.stop()
