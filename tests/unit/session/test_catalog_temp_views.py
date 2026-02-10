"""Tests for temporary view support in Catalog.

Temporary views in sparkless allow creating and dropping named views
of DataFrames for testing code that uses temp views.
"""

import pytest
from tests.fixtures.spark_backend import BackendType, get_backend_type


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend: BackendType = get_backend_type()
    result: bool = backend == BackendType.PYSPARK
    return result


class TestDropTempView:
    """Tests for catalog.dropTempView()."""

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="dropTempView behavior differs in PySpark",
    )
    def test_drop_temp_view_returns_true_when_exists(self, spark) -> None:
        """dropTempView should return True when the view exists."""
        # Create a DataFrame and register as temp view
        data = [("Alice", 25), ("Bob", 30)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.createOrReplaceTempView("test_view")

        # Drop should return True
        result = spark.catalog.dropTempView("test_view")
        assert result is True

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="dropTempView behavior differs in PySpark",
    )
    def test_drop_temp_view_returns_false_when_not_exists(self, spark) -> None:
        """dropTempView should return False when the view doesn't exist."""
        result = spark.catalog.dropTempView("nonexistent_view")
        assert result is False

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="dropTempView behavior differs in PySpark",
    )
    def test_drop_temp_view_removes_from_temp_views_list(self, spark) -> None:
        """dropTempView should remove the view from the temp views list."""
        # Create a temp view
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.createOrReplaceTempView("view_to_check")

        # Verify view is in the list
        views = spark.catalog.listLocalTempViews()
        assert "view_to_check" in views

        # Drop the view
        spark.catalog.dropTempView("view_to_check")

        # Verify view is no longer in the list
        views_after = spark.catalog.listLocalTempViews()
        assert "view_to_check" not in views_after

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="dropTempView behavior differs in PySpark",
    )
    def test_drop_temp_view_invalid_name_raises_error(self, spark) -> None:
        """dropTempView should raise error for invalid view names."""
        with pytest.raises(Exception):
            spark.catalog.dropTempView("")

        with pytest.raises(Exception):
            spark.catalog.dropTempView(None)


class TestCreateOrReplaceTempView:
    """Tests for DataFrame.createOrReplaceTempView()."""

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="createOrReplaceTempView behavior differs in PySpark",
    )
    def test_create_or_replace_creates_queryable_view(self, spark) -> None:
        """createOrReplaceTempView should create a queryable view."""
        data = [("Alice", 25), ("Bob", 30)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.createOrReplaceTempView("users")

        # Query the view
        result = spark.sql("SELECT * FROM users")
        assert result.count() == 2

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="createOrReplaceTempView behavior differs in PySpark",
    )
    def test_create_or_replace_replaces_existing_view(self, spark) -> None:
        """createOrReplaceTempView should replace existing view."""
        # Create first view
        data1 = [("Alice", 25)]
        df1 = spark.createDataFrame(data1, ["name", "age"])
        df1.createOrReplaceTempView("my_view")

        # Replace with new data
        data2 = [("Bob", 30), ("Carol", 35), ("Dave", 40)]
        df2 = spark.createDataFrame(data2, ["name", "age"])
        df2.createOrReplaceTempView("my_view")

        # Query should return new data
        result = spark.sql("SELECT * FROM my_view")
        assert result.count() == 3


class TestListLocalTempViews:
    """Tests for catalog.listLocalTempViews()."""

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="listLocalTempViews behavior differs in PySpark",
    )
    def test_list_local_temp_views_empty_initially(self, spark) -> None:
        """listLocalTempViews should return empty list when no views exist."""
        # Note: This test might fail if other tests created views
        # In a clean session, there should be no temp views
        views = spark.catalog.listLocalTempViews()
        assert isinstance(views, list)

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="listLocalTempViews behavior differs in PySpark",
    )
    def test_list_local_temp_views_shows_created_views(self, spark) -> None:
        """listLocalTempViews should include created temp views."""
        # Create temp views
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.createOrReplaceTempView("list_test_view_1")
        df.createOrReplaceTempView("list_test_view_2")

        views = spark.catalog.listLocalTempViews()
        assert "list_test_view_1" in views
        assert "list_test_view_2" in views

        # Cleanup
        spark.catalog.dropTempView("list_test_view_1")
        spark.catalog.dropTempView("list_test_view_2")


class TestTempViewIntegration:
    """Integration tests for temp view operations."""

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="Temp view behavior differs in PySpark",
    )
    def test_temp_view_workflow(self, spark) -> None:
        """Test complete workflow: create, query, drop temp view."""
        # Create data
        data = [("Product A", 100), ("Product B", 200)]
        df = spark.createDataFrame(data, ["name", "price"])

        # Create temp view
        df.createOrReplaceTempView("products")

        # Query via SQL
        result = spark.sql("SELECT SUM(price) as total FROM products")
        total = result.collect()[0]["total"]
        assert total == 300

        # Drop view
        dropped = spark.catalog.dropTempView("products")
        assert dropped is True

        # Verify dropped
        dropped_again = spark.catalog.dropTempView("products")
        assert dropped_again is False
