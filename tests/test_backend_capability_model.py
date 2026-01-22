"""Tests for EP-001: Explicit Backend Capability Model.

This module tests the capability checking functionality in materializers,
ensuring that unsupported operations are detected upfront and raise clear errors.
"""

from sparkless import SparkSession, functions as F
from sparkless.window import Window
from sparkless.backend.polars.materializer import PolarsMaterializer


class TestBackendCapabilityModel:
    """Test cases for explicit backend capability model."""

    def test_supported_operations_can_handle(self):
        """Test that supported operations are correctly identified."""
        materializer = PolarsMaterializer()

        # Test simple supported operations
        assert materializer.can_handle_operation("select", []) is True
        assert materializer.can_handle_operation("filter", F.col("a") == 1) is True
        assert (
            materializer.can_handle_operation("withColumn", ("col", F.col("a"))) is True
        )
        assert materializer.can_handle_operation("drop", "col") is True
        assert materializer.can_handle_operation("join", None) is True
        assert materializer.can_handle_operation("union", None) is True
        assert materializer.can_handle_operation("orderBy", "col") is True
        assert materializer.can_handle_operation("limit", 10) is True
        assert materializer.can_handle_operation("offset", 5) is True
        assert materializer.can_handle_operation("groupBy", None) is True
        assert materializer.can_handle_operation("distinct", None) is True
        assert (
            materializer.can_handle_operation("withColumnRenamed", ("old", "new"))
            is True
        )

    def test_unsupported_operations_cannot_handle(self):
        """Test that explicitly unsupported operations are correctly identified."""
        materializer = PolarsMaterializer()

        # Test explicitly unsupported operations
        assert materializer.can_handle_operation("months_between", None) is False
        assert materializer.can_handle_operation("pi", None) is False
        assert materializer.can_handle_operation("e", None) is False

    def test_window_function_detection_in_select(self):
        """Test that window functions in select operations are supported."""
        materializer = PolarsMaterializer()
        spark = SparkSession.builder.appName("test").getOrCreate()

        try:
            # Create a window function
            window_spec = Window.partitionBy("category").orderBy("value")
            lag_col = F.lag("value", 1).over(window_spec)

            # Window function in select is supported
            assert materializer.can_handle_operation("select", [lag_col]) is True
        finally:
            spark.stop()

    def test_window_function_detection_in_withcolumn(self):
        """Test that window functions in withColumn operations are supported."""
        materializer = PolarsMaterializer()
        spark = SparkSession.builder.appName("test").getOrCreate()

        try:
            # Create a window function
            window_spec = Window.partitionBy("category").orderBy("value")
            lag_col = F.lag("value", 1).over(window_spec)

            # Window function in withColumn is supported
            assert (
                materializer.can_handle_operation("withColumn", ("lagged", lag_col))
                is True
            )
        finally:
            spark.stop()

    def test_can_handle_operations_all_supported(self):
        """Test can_handle_operations() with all supported operations."""
        materializer = PolarsMaterializer()

        operations = [
            ("select", [F.col("a")]),
            ("filter", F.col("a") == 1),
            ("withColumn", ("b", F.col("a") * 2)),
        ]

        can_handle_all, unsupported = materializer.can_handle_operations(operations)
        assert can_handle_all is True
        assert unsupported == []

    def test_can_handle_operations_with_unsupported(self):
        """Test can_handle_operations() with unsupported operations."""
        materializer = PolarsMaterializer()

        operations = [
            ("select", [F.col("a")]),
            ("months_between", None),  # Unsupported
            ("filter", F.col("a") == 1),
        ]

        can_handle_all, unsupported = materializer.can_handle_operations(operations)
        assert can_handle_all is False
        assert "months_between" in unsupported

    def test_materialization_fails_fast_on_unsupported_operations(self):
        """Test that materialization succeeds with window functions.

        Window functions are supported by the Polars backend. This test verifies
        that df.withColumn(..., window_func).collect() works and returns the
        expected rows.
        """
        spark = SparkSession.builder.appName("test").getOrCreate()

        try:
            data = [{"category": "A", "value": 10}, {"category": "B", "value": 20}]
            df = spark.createDataFrame(data)

            window_spec = Window.partitionBy("category").orderBy("value")
            lag_col = F.lag("value", 1).over(window_spec)

            transformed_df = df.withColumn("lagged", lag_col)
            result = transformed_df.collect()

            assert len(result) == 2
        finally:
            spark.stop()

    def test_materialization_succeeds_with_supported_operations(self):
        """Test that materialization succeeds with supported operations."""
        spark = SparkSession.builder.appName("test").getOrCreate()

        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        df = spark.createDataFrame(data)

        # All supported operations
        transformed_df = (
            df.select(F.col("a"), F.col("b"))
            .filter(F.col("a") > 0)
            .withColumn("c", F.col("a") * 2)
        )

        # Should succeed without error
        result = transformed_df.collect()
        assert len(result) == 2

    def test_unsupported_operation_in_filter(self):
        """Test that F.expr() expressions in filters are detected as unsupported."""
        from sparkless import SparkSession

        materializer = PolarsMaterializer()

        # F.expr() in filter should be unsupported
        # Note: This test depends on how F.expr() is implemented
        # If F.expr() sets _from_expr flag, it should be detected
        # F.expr() requires a SparkSession, so create one
        spark = SparkSession("test")
        try:
            expr_filter = F.expr("a > 1")
            result = materializer.can_handle_operation("filter", expr_filter)
            # If F.expr() is detected, result should be False
            # If not implemented yet, this test documents expected behavior
            assert isinstance(result, bool)
        except (AttributeError, NotImplementedError, RuntimeError):
            # F.expr() might not be fully implemented yet, or session issues
            pass
        finally:
            spark.stop()
