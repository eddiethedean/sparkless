"""Tests for SQL VACUUM command.

VACUUM is a Delta Lake specific command that removes old/unused data files.
"""

from sparkless.delta import DeltaTable


class TestVacuum:
    """Tests for VACUUM SQL command."""

    def test_vacuum_basic(self, spark):
        """Test basic VACUUM command."""
        # Create a Delta table
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").saveAsTable("test_vacuum_basic")

        # Execute VACUUM (no-op in sparkless, but should not error)
        result = spark.sql("VACUUM test_vacuum_basic")
        rows = result.collect()

        # Should return empty DataFrame (no files deleted in mock)
        assert len(rows) == 0

    def test_vacuum_with_retention(self, spark):
        """Test VACUUM with RETAIN hours clause."""
        # Create Delta table
        data = [{"value": 100}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").saveAsTable("test_vacuum_retain")

        # Execute VACUUM with retention
        result = spark.sql("VACUUM test_vacuum_retain RETAIN 168 HOURS")
        rows = result.collect()

        # Should return empty DataFrame
        assert len(rows) == 0

    def test_vacuum_dry_run(self, spark):
        """Test VACUUM DRY RUN returns files to be deleted."""
        # Create Delta table
        data = [{"x": 1}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").saveAsTable("test_vacuum_dry")

        # Execute VACUUM DRY RUN
        result = spark.sql("VACUUM test_vacuum_dry DRY RUN")
        rows = result.collect()

        # Should return mock files list
        assert len(rows) >= 1
        # Check the schema has 'path' column
        assert "path" in rows[0].asDict()

    def test_vacuum_with_retention_dry_run(self, spark):
        """Test VACUUM with both RETAIN and DRY RUN."""
        # Create Delta table
        data = [{"y": 2}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").saveAsTable("test_vacuum_both")

        # Execute VACUUM with both options
        result = spark.sql("VACUUM test_vacuum_both RETAIN 24 HOURS DRY RUN")
        rows = result.collect()

        # Should return mock files list
        assert len(rows) >= 1

    def test_vacuum_with_schema(self, spark):
        """Test VACUUM with schema.table format."""
        # Create schema and table
        spark.sql("CREATE DATABASE IF NOT EXISTS test_schema_vacuum")
        data = [{"value": 100}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").saveAsTable(
            "test_schema_vacuum.vacuum_table"
        )

        # Execute VACUUM with schema prefix
        result = spark.sql("VACUUM test_schema_vacuum.vacuum_table")
        rows = result.collect()

        assert len(rows) == 0

    def test_vacuum_non_delta_table_raises(self, spark):
        """Test VACUUM raises error for non-Delta tables."""
        # Create a regular (non-Delta) table
        data = [{"id": 1}]
        df = spark.createDataFrame(data)
        df.write.format("parquet").mode("overwrite").saveAsTable("test_non_delta_vac")

        # VACUUM should raise an error
        try:
            spark.sql("VACUUM test_non_delta_vac")
            assert False, "Expected AnalysisException for non-Delta table"
        except Exception as e:
            assert "not a Delta table" in str(e) or "VACUUM" in str(e)

    def test_vacuum_nonexistent_table_raises(self, spark):
        """Test VACUUM raises error for non-existent table."""
        try:
            spark.sql("VACUUM nonexistent_table_xyz")
            assert False, "Expected AnalysisException for non-existent table"
        except Exception as e:
            assert "does not exist" in str(e) or "not found" in str(e).lower()

    def test_vacuum_matches_delta_table_vacuum(self, spark):
        """Test VACUUM SQL has same effect as DeltaTable.vacuum()."""
        # Create Delta table
        data = [{"x": 1}, {"x": 2}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").saveAsTable("test_vacuum_parity")

        # Both should complete without error
        spark.sql("VACUUM test_vacuum_parity")

        dt = DeltaTable.forName(spark, "test_vacuum_parity")
        dt.vacuum()  # This is a no-op in sparkless

        # Table should still be accessible
        result = spark.table("test_vacuum_parity").collect()
        assert len(result) == 2
