"""Test parquet format table append persistence - ensures data is visible after append writes.

Tests fix for issue #114: Table data not visible after append write via parquet format tables.
"""

import os
import pytest
from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_backend import get_backend_type, BackendType
from sparkless.spark_types import StructType, StructField, IntegerType, StringType
from sparkless.sql import SparkSession


@pytest.mark.skipif(
    get_backend_type() == BackendType.ROBIN,
    reason="v4: Robin path has schema-from-empty / catalog limitations for parquet table append",
)
class TestParquetFormatTableAppend(ParityTestBase):
    """Test that table data is immediately visible after append writes with parquet format."""

    @pytest.fixture(autouse=True)
    def setup_method(self, spark):
        """Set up test environment and clean up after test."""
        self.spark = spark
        self.schema_name = "test_schema"
        self.table_name = "test_table"
        self.table_fqn = f"{self.schema_name}.{self.table_name}"

        # Ensure schema exists
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")

        yield

        # Clean up table and schema after test
        self.spark.sql(f"DROP TABLE IF EXISTS {self.table_fqn}")
        self.spark.sql(f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE")

        # v4: session uses MemoryStorageManager (no db_path); skip file cleanup
        if (
            hasattr(self.spark._storage, "db_path")
            and self.spark._storage.db_path
            and self.spark._storage.db_path != ":memory:"
        ):
            table_path = os.path.join(
                self.spark._storage.db_path,
                self.schema_name,
                f"{self.table_name}.parquet",
            )
            if os.path.exists(table_path):
                os.remove(table_path)
            schema_dir = os.path.join(self.spark._storage.db_path, self.schema_name)
            if os.path.exists(schema_dir) and not os.listdir(schema_dir):
                os.rmdir(schema_dir)

    def test_parquet_format_append_to_existing_table(self, spark):
        """Test that data appended to an existing parquet format table is immediately visible."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

        # Create empty table with parquet format (like LogWriter does)
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("parquet").mode("overwrite").saveAsTable(self.table_fqn)

        # Verify initial state
        result1 = spark.table(self.table_fqn)
        assert result1.count() == 0, "Initial table should be empty"

        # Append data with parquet format
        data1 = [{"id": 1, "name": "test1"}]
        df1 = spark.createDataFrame(data1, schema)
        df1.write.format("parquet").mode("append").saveAsTable(self.table_fqn)

        # Verify appended data is immediately visible
        result2 = spark.table(self.table_fqn)
        count = result2.count()
        assert count == 1, (
            f"Table should have 1 row after append, got {count}. "
            "This verifies fix for issue #114."
        )

        # Append more data
        data2 = [{"id": 2, "name": "test2"}]
        df2 = spark.createDataFrame(data2, schema)
        df2.write.format("parquet").mode("append").saveAsTable(self.table_fqn)

        # Verify all data is visible
        result3 = spark.table(self.table_fqn)
        count3 = result3.count()
        assert count3 == 2, (
            f"Table should have 2 rows after second append, got {count3}"
        )
        rows = result3.collect()
        assert {row["id"] for row in rows} == {1, 2}

    def test_parquet_format_append_to_new_table(self, spark):
        """Test that appending to a non-existent parquet format table creates it correctly."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

        # Append to non-existent table with parquet format (should create it)
        data1 = [{"id": 1, "name": "initial"}]
        df1 = spark.createDataFrame(data1, schema)
        df1.write.format("parquet").mode("append").saveAsTable(self.table_fqn)

        # Verify data is immediately visible
        result = spark.table(self.table_fqn)
        count = result.count()
        assert count == 1, f"New table created by append should have 1 row, got {count}"
        assert result.collect()[0]["id"] == 1

        # Append more data
        data2 = [{"id": 2, "name": "appended"}]
        df2 = spark.createDataFrame(data2, schema)
        df2.write.format("parquet").mode("append").saveAsTable(self.table_fqn)

        # Verify all data is visible
        result_after_append = spark.table(self.table_fqn)
        count_after_append = result_after_append.count()
        assert count_after_append == 2, (
            f"Table should have 2 rows after append, got {count_after_append}"
        )
        rows = result_after_append.collect()
        assert {row["id"] for row in rows} == {1, 2}

    def test_parquet_format_multiple_append_operations(self, spark):
        """Test that multiple parquet format append operations preserve all data."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
            ]
        )

        # Create initial table with parquet format
        data1 = [{"id": 1, "value": "a"}]
        df1 = spark.createDataFrame(data1, schema)
        df1.write.format("parquet").mode("overwrite").saveAsTable(self.table_fqn)

        # Perform multiple append operations with parquet format
        for i in range(2, 6):
            data = [{"id": i, "value": chr(ord("a") + i - 1)}]
            df = spark.createDataFrame(data, schema)
            df.write.format("parquet").mode("append").saveAsTable(self.table_fqn)

            # Verify data is visible after each append
            result = spark.table(self.table_fqn)
            assert result.count() == i, (
                f"After {i - 1} appends, table should have {i} rows, got {result.count()}"
            )

        # Final verification
        result = spark.table(self.table_fqn)
        assert result.count() == 5, "Final table should have 5 rows"
        rows = result.collect()
        ids = {row["id"] for row in rows}
        assert ids == {1, 2, 3, 4, 5}, "All rows should be present"

    def test_parquet_format_append_detached_df_visible_to_active_session(self, spark):
        """Detached DataFrame writes should be immediately visible to the active session."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

        # Construct DataFrame without using the session (no shared storage)
        from sparkless.dataframe import DataFrame

        detached_df = DataFrame(
            [
                {"id": 1, "name": "alpha"},
                {"id": 2, "name": "beta"},
            ],
            schema,
        )

        # Write via detached DataFrame; should sync into the active session's storage
        detached_df.write.format("parquet").mode("append").saveAsTable(self.table_fqn)

        result = spark.table(self.table_fqn)
        assert result.count() == 2
        names = {row["name"] for row in result.collect()}
        assert names == {"alpha", "beta"}

    def test_parquet_format_append_detached_df_visible_to_multiple_sessions(
        self, spark
    ):
        """Detached DataFrame writes should sync to all active sessions."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

        # Create a second active session to verify synchronization
        spark2 = SparkSession("second_session")

        from sparkless.dataframe import DataFrame

        detached_df = DataFrame(
            [
                {"id": 1, "name": "gamma"},
                {"id": 2, "name": "delta"},
            ],
            schema,
        )

        detached_df.write.format("parquet").mode("append").saveAsTable(self.table_fqn)

        # Visible in the original session
        result1 = spark.table(self.table_fqn)
        assert result1.count() == 2
        assert {row["name"] for row in result1.collect()} == {"gamma", "delta"}

        # Visible in the second active session
        result2 = spark2.table(self.table_fqn)
        assert result2.count() == 2
        assert {row["name"] for row in result2.collect()} == {"gamma", "delta"}

        # Clean up second session artifacts
        try:
            spark2.sql(f"DROP TABLE IF EXISTS {self.table_fqn}")
            spark2.sql(f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE")
        finally:
            spark2.stop()

    def test_storage_manager_detached_write_visible_to_session(self, spark):
        """Writes via the session's storage should be visible via spark.table()."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

        storage = spark._storage  # v4: session uses single MemoryStorageManager
        storage.create_schema(self.schema_name)
        storage.create_table(self.schema_name, self.table_name, schema.fields)

        data = [{"id": 1, "name": "omega"}, {"id": 2, "name": "sigma"}]
        storage.insert_data(self.schema_name, self.table_name, data)

        result = spark.table(self.table_fqn)
        assert result.count() == 2
        names = {row["name"] for row in result.collect()}
        assert names == {"omega", "sigma"}

    def test_pipeline_logs_like_write_visible_immediately(self, spark):
        """Simulate pipeline_logs writes: append to new table then read back immediately."""
        schema = StructType(
            [
                StructField("run_id", StringType(), True),
                StructField("step", StringType(), True),
                StructField("status", StringType(), True),
            ]
        )

        rows = [
            {"run_id": "run1", "step": "pipeline", "status": "success"},
            {"run_id": "run1", "step": "extract", "status": "success"},
            {"run_id": "run1", "step": "transform", "status": "success"},
            {"run_id": "run1", "step": "load", "status": "success"},
        ]

        df = spark.createDataFrame(rows, schema)
        df.write.format("parquet").mode("append").saveAsTable(self.table_fqn)

        result = spark.table(self.table_fqn)
        assert result.count() == 4, (
            "pipeline_logs table should return all appended rows"
        )

        # Append a second run to mimic incremental pipeline logging
        rows2 = [
            {"run_id": "run2", "step": "pipeline", "status": "success"},
            {"run_id": "run2", "step": "validate", "status": "success"},
        ]
        df2 = spark.createDataFrame(rows2, schema)
        df2.write.format("parquet").mode("append").saveAsTable(self.table_fqn)

        result_after = spark.table(self.table_fqn)
        assert result_after.count() == 6, (
            "All runs should be visible immediately after append"
        )
