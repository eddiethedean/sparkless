"""Robin backend test helpers (outside tests.unit so mypy resolves callables correctly)."""

from __future__ import annotations

from typing import Callable

from sparkless import DataFrame, SparkSession
from sparkless.dataframe.protocols import SupportsDataFrameOps
from sparkless.sql import functions as F


def _trigger_collect_impl(df: SupportsDataFrameOps) -> None:
    """Call collect() to trigger materialization (used in tests that expect an error)."""
    df.collect()


trigger_collect: Callable[[SupportsDataFrameOps], None] = _trigger_collect_impl


def run_unsupported_regexp_extract_all_select(
    app_name: str = "test-robin",
) -> None:
    """Create session, build df with regexp_extract_all select, call collect (raises)."""
    spark: SparkSession = SparkSession(app_name, backend_type="robin")
    try:
        created: DataFrame = spark.createDataFrame([{"s": "a1 b22 c333"}])
        selected: SupportsDataFrameOps = created.select(
            F.regexp_extract_all("s", r"\d+", 0).alias("m")
        )
        _trigger_collect_impl(selected)
    finally:
        spark.stop()


def run_unsupported_regexp_extract_all_select_with_message_check(
    app_name: str = "test-robin",
) -> str:
    """Same as above but returns the exception message for assertion."""
    from sparkless.core.exceptions.operation import SparkUnsupportedOperationError

    spark: SparkSession = SparkSession(app_name, backend_type="robin")
    try:
        created: DataFrame = spark.createDataFrame([{"s": "a1 b2"}])
        selected: SupportsDataFrameOps = created.select(
            F.regexp_extract_all("s", r"\d+", 0).alias("m")
        )
        try:
            _trigger_collect_impl(selected)
        except SparkUnsupportedOperationError as e:
            return str(e)
        raise AssertionError("expected SparkUnsupportedOperationError")
    finally:
        spark.stop()
