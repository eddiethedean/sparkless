"""Tests for SQL DESCRIBE DETAIL command.

DESCRIBE DETAIL is a Delta Lake specific command that returns table metadata.
"""

from sparkless.delta import DeltaTable


class TestDescribeDetail:
    """Tests for DESCRIBE DETAIL SQL command."""

    def test_describe_detail_basic(self, spark):
        """Test DESCRIBE DETAIL returns expected columns."""
        # Create a Delta table
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").saveAsTable("test_detail_basic")

        # Execute DESCRIBE DETAIL
        result = spark.sql("DESCRIBE DETAIL test_detail_basic")
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

    def test_describe_detail_with_schema(self, spark):
        """Test DESCRIBE DETAIL with schema.table format."""
        # Create schema and table
        spark.sql("CREATE DATABASE IF NOT EXISTS test_schema_detail")
        data = [{"value": 100}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").saveAsTable(
            "test_schema_detail.detail_table"
        )

        # Execute DESCRIBE DETAIL with schema prefix
        result = spark.sql("DESCRIBE DETAIL test_schema_detail.detail_table")
        rows = result.collect()

        assert len(rows) == 1
        row = rows[0]
        assert row["format"] == "delta"
        assert "detail_table" in row["name"]

    def test_describe_detail_non_delta_table_raises(self, spark):
        """Test DESCRIBE DETAIL raises error for non-Delta tables."""
        # Create a regular (non-Delta) table
        data = [{"id": 1}]
        df = spark.createDataFrame(data)
        # Use parquet format instead of delta
        df.write.format("parquet").mode("overwrite").saveAsTable("test_non_delta")

        # DESCRIBE DETAIL should raise an error
        try:
            spark.sql("DESCRIBE DETAIL test_non_delta")
            assert False, "Expected AnalysisException for non-Delta table"
        except Exception as e:
            assert "not a Delta table" in str(e) or "DESCRIBE DETAIL" in str(e)

    def test_describe_detail_matches_delta_table_detail(self, spark):
        """Test DESCRIBE DETAIL returns same data as DeltaTable.detail()."""
        # Create Delta table
        data = [{"x": 1}, {"x": 2}, {"x": 3}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").saveAsTable("test_detail_parity")

        # Get details via SQL
        sql_result = spark.sql("DESCRIBE DETAIL test_detail_parity").collect()[0]

        # Get details via DeltaTable API
        dt = DeltaTable.forName(spark, "test_detail_parity")
        api_result = dt.detail().collect()[0]

        # Both should return delta format
        assert sql_result["format"] == api_result["format"] == "delta"
        # Both should have the table name
        assert "test_detail_parity" in sql_result["name"]
        assert "test_detail_parity" in api_result["name"]

    def test_describe_detail_partition_columns(self, spark):
        """Test DESCRIBE DETAIL shows partition columns."""
        # Create partitioned Delta table
        data = [
            {"id": 1, "category": "A", "value": 10},
            {"id": 2, "category": "B", "value": 20},
        ]
        df = spark.createDataFrame(data)
        df.write.format("delta").partitionBy("category").mode("overwrite").saveAsTable(
            "test_detail_partitioned"
        )

        # Get details
        result = spark.sql("DESCRIBE DETAIL test_detail_partitioned").collect()[0]

        # partitionColumns should be present (may be empty list in mock)
        assert "partitionColumns" in result.asDict()
