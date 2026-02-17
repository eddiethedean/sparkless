"""
Test to reproduce issue #160 by testing Polars lazy frame execution plan behavior.

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
    from sparkless import config

    config._load_feature_flag_overrides.cache_clear()
    yield
    if "SPARKLESS_FEATURE_enable_expression_translation_cache" in os.environ:
        del os.environ["SPARKLESS_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"]
    config._load_feature_flag_overrides.cache_clear()


def test_lazy_frame_preserves_column_references(enable_cache):
    """
    Test if Polars lazy frames preserve column references in execution plan.

    The issue might be that when we:
    1. Create a lazy Polars DataFrame with operations
    2. Drop columns via select
    3. The lazy frame's execution plan still has references to dropped columns
    """
    spark = SparkSession.builder.appName("lazy_frame_test").getOrCreate()

    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Create operations that will build up a lazy Polars execution plan
    df_with_ops = df.withColumn(
        "date_cleaned", F.regexp_replace(F.col("impression_date"), r"\.\d+", "")
    ).withColumn(
        "date_parsed",
        F.to_timestamp(F.col("date_cleaned").cast("string"), "yyyy-MM-dd'T'HH:mm:ss"),
    )

    # At this point, we have a lazy Polars DataFrame with operations queued
    # The execution plan might reference impression_date and date_cleaned

    # Now drop columns - this should update the execution plan
    df_dropped = df_with_ops.select("impression_id", "campaign_id", "date_parsed")

    assert "impression_date" not in df_dropped.columns
    assert "date_cleaned" not in df_dropped.columns

    # Try to materialize - if the execution plan still references dropped columns, this will fail
    try:
        count = df_dropped.count()
        assert count == 200

        rows = df_dropped.collect()
        assert len(rows) == 200
    except Exception as e:
        error_msg = str(e).lower()
        if ("impression_date" in error_msg or "date_cleaned" in error_msg) and (
            "cannot resolve" in error_msg
            or "not found" in error_msg
            or "unable to find" in error_msg
        ):
            pytest.fail(
                f"BUG REPRODUCED! Execution plan references dropped columns: {e}\n"
                f"This suggests that Polars lazy execution plan preserves column references."
            )
        raise

    spark.stop()


def test_operations_after_select_with_cached_expressions(enable_cache):
    """
    Test if operations after select reuse cached expressions that reference dropped columns.

    Scenario:
    1. Use expression with column A - gets cached
    2. Drop column A via select
    3. Add another operation that might reuse cached expression
    """
    spark = SparkSession.builder.appName("ops_after_select").getOrCreate()

    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Create expression that references impression_date - gets cached
    expr = F.regexp_replace(F.col("impression_date"), r"\.\d+", "")
    df_with_expr = df.withColumn("date_cleaned", expr)

    # Materialize to cache the expression
    _ = df_with_expr.count()

    # Get translator to check cache
    from sparkless.backend.factory import BackendFactory

    materializer = BackendFactory.create_materializer("polars")
    translator = materializer.translator
    cache_size_before = len(translator._translation_cache)
    print(f"Cache size before select: {cache_size_before}")

    # Drop the column
    df_dropped = df_with_expr.select("impression_id", "campaign_id", "date_cleaned")
    assert "impression_date" not in df_dropped.columns

    # Add another operation after select
    # If this operation somehow reuses the cached expression, it might fail
    try:
        df_final = df_dropped.withColumn(
            "campaign_upper", F.upper(F.col("campaign_id"))
        )
        count = df_final.count()
        assert count == 200
    except Exception as e:
        error_msg = str(e).lower()
        if "impression_date" in error_msg and (
            "cannot resolve" in error_msg
            or "not found" in error_msg
            or "unable to find" in error_msg
        ):
            pytest.fail(
                f"BUG REPRODUCED! Cached expression reused after column drop: {e}"
            )
        raise

    spark.stop()


def test_filter_after_select_with_dropped_column_reference(enable_cache):
    """
    Test if filter operations after select try to use cached expressions for dropped columns.

    This might happen if:
    1. Filter expression is cached (references column A)
    2. Column A is dropped
    3. Filter is applied and tries to use cached expression
    """
    spark = SparkSession.builder.appName("filter_after_select").getOrCreate()

    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Create a filter that references impression_date - might get cached
    df_filtered = df.filter(F.col("impression_date").isNotNull())

    # Materialize to potentially cache the filter expression
    _ = df_filtered.count()

    # Drop the column
    df_dropped = df_filtered.select("impression_id", "campaign_id")
    assert "impression_date" not in df_dropped.columns

    # Try to add another filter - if cached filter expression is reused, it might fail
    try:
        df_final = df_dropped.filter(F.col("campaign_id").isNotNull())
        count = df_final.count()
        assert count == 200
    except Exception as e:
        error_msg = str(e).lower()
        if "impression_date" in error_msg and (
            "cannot resolve" in error_msg
            or "not found" in error_msg
            or "unable to find" in error_msg
        ):
            pytest.fail(f"BUG REPRODUCED! Filter expression cached and reused: {e}")
        raise

    spark.stop()


def test_write_operation_triggers_re_evaluation(enable_cache):
    """
    Test if write operations trigger re-evaluation of expressions that reference dropped columns.

    Write operations might re-evaluate the execution plan, which could trigger the bug.
    """
    spark = SparkSession.builder.appName("write_re_eval").getOrCreate()

    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Use impression_date then drop it
    df_transformed = df.withColumn(
        "date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    ).select("impression_id", "campaign_id", "date_parsed")

    assert "impression_date" not in df_transformed.columns

    # Write operation might trigger re-evaluation of execution plan
    try:
        df_transformed.write.mode("overwrite").saveAsTable("test_table_160_write")

        # Read back
        result = spark.table("test_table_160_write")
        count = result.count()
        assert count == 200

        # Clean up
        spark.sql("DROP TABLE IF EXISTS test_table_160_write")
    except Exception as e:
        error_msg = str(e).lower()
        if "impression_date" in error_msg and (
            "cannot resolve" in error_msg
            or "not found" in error_msg
            or "unable to find" in error_msg
        ):
            pytest.fail(
                f"BUG REPRODUCED during write! Error: {e}\n"
                f"Write operation triggered re-evaluation that referenced dropped column."
            )
        raise

    spark.stop()


def test_multiple_materializations_with_shared_cache():
    """
    Test if multiple materializations share cache somehow.

    Maybe the issue occurs when:
    1. First DataFrame materializes and caches expressions
    2. Second DataFrame drops columns but cache persists
    3. Second DataFrame tries to use cached expressions
    """
    spark = SparkSession.builder.appName("shared_cache_test").getOrCreate()

    # Create first DataFrame and materialize (caches expressions)
    data1 = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
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

    # Materialize first DataFrame - expressions get cached
    _ = df1_transformed.count()

    # Get materializer to check cache
    from sparkless.backend.factory import BackendFactory

    materializer1 = BackendFactory.create_materializer("polars")
    translator1 = materializer1.translator
    print(
        f"Cache size after first materialization: {len(translator1._translation_cache)}"
    )

    # Create second DataFrame (new materializer instance, so new cache)
    # But maybe there's some shared state?
    # Create a DataFrame with a date column to test with
    data2 = [("imp_001", "2024-01-16T10:30:45", "campaign_1")]
    df2 = spark.createDataFrame(data2, ["impression_id", "other_date", "campaign_id"])

    # Try to use similar expression - if cache is somehow shared, this might fail
    try:
        df2_transformed = df2.withColumn(
            "date_parsed",
            F.to_timestamp(
                F.regexp_replace(F.col("other_date"), r"\.\d+", "").cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        _ = df2_transformed.count()
    except Exception as e:
        error_msg = str(e).lower()
        if "impression_date" in error_msg and (
            "cannot resolve" in error_msg or "not found" in error_msg
        ):
            pytest.fail(f"BUG REPRODUCED! Shared cache issue: {e}")
        raise

    spark.stop()
