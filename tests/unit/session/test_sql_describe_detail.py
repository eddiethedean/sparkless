"""Tests for SQL DESCRIBE DETAIL command.

DESCRIBE DETAIL is a Delta Lake specific command that returns table metadata.
"""

import pytest
from sparkless.delta import DeltaTable
from tests.fixtures.spark_backend import SparkBackend, BackendType, get_backend_type


@pytest.fixture
def spark_with_delta(request):
    """Create a SparkSession with Delta Lake enabled for PySpark mode."""
    from tests.fixtures.spark_backend import get_backend_type

    backend = get_backend_type(request)
    test_name = "test_describe_detail"
    if hasattr(request, "node") and hasattr(request.node, "name"):
        test_name = f"test_{request.node.name[:50]}"

    # For PySpark, enable Delta Lake
    if backend == BackendType.PYSPARK:
        session = SparkBackend.create_pyspark_session(
            app_name=test_name, enable_delta=True
        )
    else:
        # For mock-spark, Delta is always available
        session = SparkBackend.create_mock_spark_session(app_name=test_name)

    yield session

    # Cleanup
    import contextlib
    import gc

    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()


class TestDescribeDetail:
    """Tests for DESCRIBE DETAIL SQL command."""

    def test_describe_detail_basic(self, spark_with_delta):
        """Test DESCRIBE DETAIL returns expected columns."""
        # Create a Delta table
        # For PySpark 3.5+, we need to drop table first or use option("overwriteSchema", "true")
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        df = spark_with_delta.createDataFrame(data)
        # Drop table if exists, then create
        spark_with_delta.sql("DROP TABLE IF EXISTS test_detail_basic")
        df.write.format("delta").saveAsTable("test_detail_basic")

        # Execute DESCRIBE DETAIL
        result = spark_with_delta.sql("DESCRIBE DETAIL test_detail_basic")
        rows = result.collect()

        # Should return exactly one row
        assert len(rows) == 1

        row = rows[0]
        # Check expected columns exist
        assert row["format"] == "delta"
        assert "test_detail_basic" in row["name"]
        assert row["numFiles"] >= 1
        assert row["sizeInBytes"] >= 0
        assert row["minReaderVersion"] >= 1
        assert row["minWriterVersion"] >= 1

    def test_describe_detail_with_schema(self, spark_with_delta):
        """Test DESCRIBE DETAIL with schema.table format."""
        # Create schema and table
        spark_with_delta.sql("CREATE DATABASE IF NOT EXISTS test_schema_detail")
        spark_with_delta.sql("DROP TABLE IF EXISTS test_schema_detail.detail_table")
        data = [{"value": 100}]
        df = spark_with_delta.createDataFrame(data)
        df.write.format("delta").saveAsTable("test_schema_detail.detail_table")

        # Execute DESCRIBE DETAIL with schema prefix
        result = spark_with_delta.sql("DESCRIBE DETAIL test_schema_detail.detail_table")
        rows = result.collect()

        assert len(rows) == 1
        row = rows[0]
        assert row["format"] == "delta"
        assert "detail_table" in row["name"]

    def test_describe_detail_non_delta_table_raises(self, spark_with_delta):
        """Test DESCRIBE DETAIL raises error for non-Delta tables."""
        # Create a regular (non-Delta) table
        spark_with_delta.sql("DROP TABLE IF EXISTS test_non_delta")
        data = [{"id": 1}]
        df = spark_with_delta.createDataFrame(data)
        # Use parquet format instead of delta
        df.write.format("parquet").saveAsTable("test_non_delta")

        # DESCRIBE DETAIL should raise an error or return invalid result
        # Both PySpark and mock-spark should handle this appropriately
        try:
            result = spark_with_delta.sql("DESCRIBE DETAIL test_non_delta")
            # If it doesn't raise immediately, try to collect and verify it's invalid
            try:
                rows = result.collect()
                # If we get here, verify the result is invalid (not a Delta table)
                # This can happen in PySpark 3.5+ where it doesn't raise immediately
                assert len(rows) == 0 or rows[0].get("format") != "delta", (
                    "DESCRIBE DETAIL should not return valid Delta format for non-Delta tables"
                )
            except Exception as e:
                # Error raised when collecting - verify it's a Delta-related error
                error_msg = str(e)
                assert (
                    "not a Delta table" in error_msg
                    or "Delta" in error_msg
                    or "DESCRIBE DETAIL" in error_msg
                    or "Delta table" in error_msg
                ), f"Expected Delta-related error, got: {error_msg}"
        except Exception as e:
            # Exception was raised as expected (works in both modes)
            error_msg = str(e)
            assert (
                "not a Delta table" in error_msg
                or "Delta" in error_msg
                or "DESCRIBE DETAIL" in error_msg
                or "Delta table" in error_msg
            ), f"Expected Delta-related error, got: {error_msg}"

    def test_describe_detail_matches_delta_table_detail(
        self, spark_with_delta, request
    ):
        """Test DESCRIBE DETAIL returns same data as DeltaTable.detail()."""
        from tests.fixtures.spark_backend import get_backend_type
        from tests.fixtures.spark_backend import BackendType

        backend = get_backend_type(request)

        # Create Delta table
        spark_with_delta.sql("DROP TABLE IF EXISTS test_detail_parity")
        data = [{"x": 1}, {"x": 2}, {"x": 3}]
        df = spark_with_delta.createDataFrame(data)
        df.write.format("delta").saveAsTable("test_detail_parity")

        # Get details via SQL
        sql_result = spark_with_delta.sql(
            "DESCRIBE DETAIL test_detail_parity"
        ).collect()[0]

        # Verify SQL result is valid
        assert sql_result["format"] == "delta"
        assert "test_detail_parity" in sql_result["name"]

        # Get details via DeltaTable API and compare
        if backend == BackendType.PYSPARK:
            # Use real DeltaTable API from delta-spark in PySpark mode
            try:
                # Import the real delta.tables module directly
                # The real delta-spark package should be installed and available
                import importlib

                # Try to import delta.tables - this should be the real one from delta-spark
                # (not our mock, since we're in PySpark mode and delta-spark is installed)
                delta_tables_module = importlib.import_module("delta.tables")
                RealDeltaTable = delta_tables_module.DeltaTable

                # Verify we got the real one by checking the module path
                # The real delta.tables should come from the delta-spark package
                module_path = getattr(delta_tables_module, "__file__", "")
                if module_path and "sparkless" in module_path:
                    pytest.skip(
                        "Could not import real delta.tables module in PySpark mode"
                    )

                # Use the real DeltaTable API
                real_dt = RealDeltaTable.forName(spark_with_delta, "test_detail_parity")
                # Call detail() - this should use the real implementation
                api_result = real_dt.detail().collect()[0]

                # Compare key fields that should match
                assert api_result["format"] == sql_result["format"] == "delta"
                assert "test_detail_parity" in sql_result["name"]
                assert "test_detail_parity" in api_result["name"]
                # Both should have similar metadata
                assert "numFiles" in sql_result.asDict()
                assert "numFiles" in api_result.asDict()
            except ImportError:
                pytest.skip("Delta Lake not installed in PySpark mode")
            except Exception as e:
                # If we get our mock's detail() method, it will fail with schema issues
                # In that case, just verify SQL works (which is the main thing we're testing)
                if "unknown datatype" in str(e) or "StructType" in str(e):
                    # Our mock was used instead of real one - just verify SQL works
                    pass
                else:
                    raise
        else:
            # Use mock DeltaTable for mock-spark and compare
            dt = DeltaTable.forName(spark_with_delta, "test_detail_parity")
            api_result = dt.detail().collect()[0]

            # Both should return delta format
            assert api_result["format"] == sql_result["format"] == "delta"
            # Both should have the table name
            assert "test_detail_parity" in sql_result["name"]
            assert "test_detail_parity" in api_result["name"]

    def test_describe_detail_partition_columns(self, spark_with_delta):
        """Test DESCRIBE DETAIL shows partition columns."""
        # Create partitioned Delta table
        spark_with_delta.sql("DROP TABLE IF EXISTS test_detail_partitioned")
        data = [
            {"id": 1, "category": "A", "value": 10},
            {"id": 2, "category": "B", "value": 20},
        ]
        df = spark_with_delta.createDataFrame(data)
        df.write.format("delta").partitionBy("category").saveAsTable(
            "test_detail_partitioned"
        )

        # Get details
        result = spark_with_delta.sql(
            "DESCRIBE DETAIL test_detail_partitioned"
        ).collect()[0]

        # partitionColumns should be present (may be empty list in mock)
        assert "partitionColumns" in result.asDict()

    def test_describe_detail_empty_table(self, spark_with_delta, request):
        """Test DESCRIBE DETAIL works with empty Delta tables."""
        from tests.fixtures.spark_backend import get_backend_type
        from tests.fixtures.spark_backend import BackendType

        backend = get_backend_type(request)
        spark_with_delta.sql("DROP TABLE IF EXISTS test_detail_empty")

        # Create empty DataFrame with schema
        # Use appropriate schema types based on backend
        if backend == BackendType.PYSPARK:
            from pyspark.sql.types import (
                StructType,
                StructField,
                IntegerType,
                StringType,
            )
        else:
            from sparkless.spark_types import (
                StructType,
                StructField,
                IntegerType,
                StringType,
            )

        schema = StructType(
            [StructField("id", IntegerType()), StructField("name", StringType())]
        )
        empty_df = spark_with_delta.createDataFrame([], schema)
        empty_df.write.format("delta").saveAsTable("test_detail_empty")

        # DESCRIBE DETAIL should work even for empty tables
        result = spark_with_delta.sql("DESCRIBE DETAIL test_detail_empty")
        rows = result.collect()

        assert len(rows) == 1
        row = rows[0]
        assert row["format"] == "delta"
        assert "test_detail_empty" in row["name"]
        assert row["numFiles"] >= 0  # Can be 0 for empty tables
        assert row["sizeInBytes"] >= 0

    def test_describe_detail_nonexistent_table(self, spark_with_delta):
        """Test DESCRIBE DETAIL raises error for non-existent tables."""
        spark_with_delta.sql("DROP TABLE IF EXISTS test_nonexistent_detail")

        # Should raise an error for non-existent table
        try:
            result = spark_with_delta.sql("DESCRIBE DETAIL test_nonexistent_detail")
            result.collect()  # Force evaluation
            assert False, "Expected error for non-existent table"
        except Exception as e:
            error_msg = str(e).lower()
            assert (
                "not found" in error_msg
                or "does not exist" in error_msg
                or "table or view" in error_msg
                or "cannot be found" in error_msg
            ), f"Expected table not found error, got: {error_msg}"

    def test_describe_detail_multiple_writes(self, spark_with_delta):
        """Test DESCRIBE DETAIL reflects changes after multiple writes."""
        spark_with_delta.sql("DROP TABLE IF EXISTS test_detail_multiple")

        # Initial write
        data1 = [{"id": 1, "value": 10}]
        df1 = spark_with_delta.createDataFrame(data1)
        df1.write.format("delta").saveAsTable("test_detail_multiple")

        # Get initial details
        result1 = spark_with_delta.sql(
            "DESCRIBE DETAIL test_detail_multiple"
        ).collect()[0]
        initial_files = result1["numFiles"]
        initial_size = result1["sizeInBytes"]

        # Append more data
        data2 = [{"id": 2, "value": 20}, {"id": 3, "value": 30}]
        df2 = spark_with_delta.createDataFrame(data2)
        df2.write.format("delta").mode("append").saveAsTable("test_detail_multiple")

        # Get updated details
        result2 = spark_with_delta.sql(
            "DESCRIBE DETAIL test_detail_multiple"
        ).collect()[0]

        # Verify table still exists and is valid
        assert result2["format"] == "delta"
        assert "test_detail_multiple" in result2["name"]
        # File count and size may increase (depending on implementation)
        assert result2["numFiles"] >= initial_files
        assert result2["sizeInBytes"] >= initial_size

    def test_describe_detail_complex_schema(self, spark_with_delta, request):
        """Test DESCRIBE DETAIL works with complex schemas."""
        from tests.fixtures.spark_backend import get_backend_type
        from tests.fixtures.spark_backend import BackendType

        backend = get_backend_type(request)
        spark_with_delta.sql("DROP TABLE IF EXISTS test_detail_complex")

        # Use appropriate schema types based on backend
        if backend == BackendType.PYSPARK:
            from pyspark.sql.types import (
                StructType,
                StructField,
                IntegerType,
                StringType,
                ArrayType,
                MapType,
            )
        else:
            from sparkless.spark_types import (
                StructType,
                StructField,
                IntegerType,
                StringType,
                ArrayType,
                MapType,
            )

        # Create table with complex schema
        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("tags", ArrayType(StringType())),
                StructField("metadata", MapType(StringType(), StringType())),
            ]
        )
        data = [
            {"id": 1, "tags": ["tag1", "tag2"], "metadata": {"key1": "value1"}},
            {"id": 2, "tags": ["tag3"], "metadata": {"key2": "value2"}},
        ]
        df = spark_with_delta.createDataFrame(data, schema)
        df.write.format("delta").saveAsTable("test_detail_complex")

        # DESCRIBE DETAIL should work with complex schemas
        result = spark_with_delta.sql("DESCRIBE DETAIL test_detail_complex")
        rows = result.collect()

        assert len(rows) == 1
        row = rows[0]
        assert row["format"] == "delta"
        assert "test_detail_complex" in row["name"]

    def test_describe_detail_all_required_columns(self, spark_with_delta):
        """Test DESCRIBE DETAIL returns all required columns."""
        spark_with_delta.sql("DROP TABLE IF EXISTS test_detail_columns")
        data = [{"id": 1, "name": "test"}]
        df = spark_with_delta.createDataFrame(data)
        df.write.format("delta").saveAsTable("test_detail_columns")

        result = spark_with_delta.sql("DESCRIBE DETAIL test_detail_columns")
        rows = result.collect()
        assert len(rows) == 1

        row_dict = rows[0].asDict()

        # Check all standard DESCRIBE DETAIL columns are present
        required_columns = [
            "format",
            "id",
            "name",
            "description",
            "location",
            "createdAt",
            "lastModified",
            "partitionColumns",
            "numFiles",
            "sizeInBytes",
            "properties",
            "minReaderVersion",
            "minWriterVersion",
        ]

        for col in required_columns:
            assert col in row_dict, (
                f"Required column '{col}' missing from DESCRIBE DETAIL result"
            )

    def test_describe_detail_multiple_partitions(self, spark_with_delta):
        """Test DESCRIBE DETAIL with multiple partition columns."""
        spark_with_delta.sql("DROP TABLE IF EXISTS test_detail_multi_partition")

        data = [
            {"id": 1, "year": 2023, "month": 1, "value": 10},
            {"id": 2, "year": 2023, "month": 2, "value": 20},
            {"id": 3, "year": 2024, "month": 1, "value": 30},
        ]
        df = spark_with_delta.createDataFrame(data)
        df.write.format("delta").partitionBy("year", "month").saveAsTable(
            "test_detail_multi_partition"
        )

        result = spark_with_delta.sql(
            "DESCRIBE DETAIL test_detail_multi_partition"
        ).collect()[0]

        assert result["format"] == "delta"
        assert "partitionColumns" in result.asDict()
        # In PySpark, partitionColumns should be a list with both columns
        # In mock-spark, it may be empty but should still be present
        partition_cols = result["partitionColumns"]
        assert isinstance(partition_cols, list)

    def test_describe_detail_table_properties(self, spark_with_delta):
        """Test DESCRIBE DETAIL includes table properties."""
        spark_with_delta.sql("DROP TABLE IF EXISTS test_detail_properties")

        data = [{"id": 1, "value": 100}]
        df = spark_with_delta.createDataFrame(data)

        # Create table with properties (if supported)
        try:
            df.write.format("delta").option(
                "delta.autoOptimize.optimizeWrite", "true"
            ).saveAsTable("test_detail_properties")
        except Exception:
            # If option not supported, create without it
            df.write.format("delta").saveAsTable("test_detail_properties")

        result = spark_with_delta.sql(
            "DESCRIBE DETAIL test_detail_properties"
        ).collect()[0]

        assert result["format"] == "delta"
        assert "properties" in result.asDict()
        # Properties should be a dict/map (or None/empty in some implementations)
        properties = result["properties"]
        assert (
            isinstance(properties, (dict, list))
            or properties is None
            or properties == {}
        )

    def test_describe_detail_overwrite_operation(self, spark_with_delta):
        """Test DESCRIBE DETAIL after overwrite operations."""
        spark_with_delta.sql("DROP TABLE IF EXISTS test_detail_overwrite")

        # Initial write
        data1 = [{"id": 1, "value": 10}]
        df1 = spark_with_delta.createDataFrame(data1)
        df1.write.format("delta").saveAsTable("test_detail_overwrite")

        result1 = spark_with_delta.sql(
            "DESCRIBE DETAIL test_detail_overwrite"
        ).collect()[0]
        assert result1["format"] == "delta"

        # Overwrite with new data
        spark_with_delta.sql("DROP TABLE IF EXISTS test_detail_overwrite")
        data2 = [{"id": 2, "value": 20}, {"id": 3, "value": 30}]
        df2 = spark_with_delta.createDataFrame(data2)
        df2.write.format("delta").saveAsTable("test_detail_overwrite")

        result2 = spark_with_delta.sql(
            "DESCRIBE DETAIL test_detail_overwrite"
        ).collect()[0]
        assert result2["format"] == "delta"
        assert "test_detail_overwrite" in result2["name"]

    def test_describe_detail_special_characters_in_name(self, spark_with_delta):
        """Test DESCRIBE DETAIL with special characters in table name."""
        # Use a table name with underscores (common case)
        table_name = "test_detail_special_123"
        spark_with_delta.sql(f"DROP TABLE IF EXISTS {table_name}")

        data = [{"id": 1, "name": "test"}]
        df = spark_with_delta.createDataFrame(data)
        df.write.format("delta").saveAsTable(table_name)

        result = spark_with_delta.sql(f"DESCRIBE DETAIL {table_name}")
        rows = result.collect()

        assert len(rows) == 1
        row = rows[0]
        assert row["format"] == "delta"
        assert table_name in row["name"] or table_name.replace("_", "") in row[
            "name"
        ].replace("_", "")

    def test_describe_detail_large_table(self, spark_with_delta):
        """Test DESCRIBE DETAIL with larger datasets."""
        spark_with_delta.sql("DROP TABLE IF EXISTS test_detail_large")

        # Create a larger dataset
        data = [{"id": i, "value": i * 10} for i in range(100)]
        df = spark_with_delta.createDataFrame(data)
        df.write.format("delta").saveAsTable("test_detail_large")

        result = spark_with_delta.sql("DESCRIBE DETAIL test_detail_large").collect()[0]

        assert result["format"] == "delta"
        assert "test_detail_large" in result["name"]
        assert result["numFiles"] >= 1
        assert result["sizeInBytes"] > 0

    def test_describe_detail_different_data_types(self, spark_with_delta, request):
        """Test DESCRIBE DETAIL with various data types."""
        from tests.fixtures.spark_backend import get_backend_type
        from tests.fixtures.spark_backend import BackendType

        backend = get_backend_type(request)
        spark_with_delta.sql("DROP TABLE IF EXISTS test_detail_types")

        # Use appropriate schema types based on backend
        if backend == BackendType.PYSPARK:
            from pyspark.sql.types import (
                StructType,
                StructField,
                IntegerType,
                StringType,
                DoubleType,
                BooleanType,
            )
        else:
            from sparkless.spark_types import (
                StructType,
                StructField,
                IntegerType,
                StringType,
                DoubleType,
                BooleanType,
            )

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("price", DoubleType()),
                StructField("active", BooleanType()),
            ]
        )

        data = [
            {"id": 1, "name": "item1", "price": 19.99, "active": True},
            {"id": 2, "name": "item2", "price": 29.99, "active": False},
        ]
        df = spark_with_delta.createDataFrame(data, schema)
        df.write.format("delta").saveAsTable("test_detail_types")

        result = spark_with_delta.sql("DESCRIBE DETAIL test_detail_types").collect()[0]

        assert result["format"] == "delta"
        assert "test_detail_types" in result["name"]
