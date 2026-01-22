"""Tests for SQL RESTORE TABLE command.

RESTORE is a Delta Lake specific command that restores a table to a previous version.
"""


class TestRestore:
    """Tests for RESTORE TABLE SQL command."""

    def test_restore_to_version(self, spark):
        """Test RESTORE TABLE to a specific version."""
        # Create a Delta table
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").saveAsTable("test_restore_version")

        # Execute RESTORE to version
        result = spark.sql("RESTORE TABLE test_restore_version TO VERSION AS OF 0")
        rows = result.collect()

        # Should return restore operation details
        assert len(rows) == 1
        row = rows[0].asDict()
        assert "table_size_after_restore" in row
        assert "num_of_files_after_restore" in row
        assert "num_removed_files" in row
        assert "num_restored_files" in row

    def test_restore_to_timestamp(self, spark):
        """Test RESTORE TABLE to a specific timestamp."""
        # Create Delta table
        data = [{"value": 100}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").saveAsTable("test_restore_timestamp")

        # Execute RESTORE to timestamp
        result = spark.sql(
            "RESTORE TABLE test_restore_timestamp TO TIMESTAMP AS OF '2024-01-01 00:00:00'"
        )
        rows = result.collect()

        # Should return restore operation details
        assert len(rows) == 1
        row = rows[0].asDict()
        assert "removed_files_size" in row
        assert "restored_files_size" in row

    def test_restore_with_schema(self, spark):
        """Test RESTORE TABLE with schema.table format."""
        # Create schema and table
        spark.sql("CREATE DATABASE IF NOT EXISTS test_schema_restore")
        data = [{"x": 1}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").saveAsTable(
            "test_schema_restore.restore_table"
        )

        # Execute RESTORE with schema prefix
        result = spark.sql(
            "RESTORE TABLE test_schema_restore.restore_table TO VERSION AS OF 0"
        )
        rows = result.collect()

        assert len(rows) == 1

    def test_restore_non_delta_table_raises(self, spark):
        """Test RESTORE raises error for non-Delta tables."""
        # Create a regular (non-Delta) table
        data = [{"id": 1}]
        df = spark.createDataFrame(data)
        df.write.format("parquet").mode("overwrite").saveAsTable(
            "test_non_delta_restore"
        )

        # RESTORE should raise an error
        try:
            spark.sql("RESTORE TABLE test_non_delta_restore TO VERSION AS OF 0")
            assert False, "Expected AnalysisException for non-Delta table"
        except Exception as e:
            assert "not" in str(e).lower() or "Delta" in str(e)

    def test_restore_nonexistent_table_raises(self, spark):
        """Test RESTORE raises error for non-existent table."""
        try:
            spark.sql("RESTORE TABLE nonexistent_table_xyz TO VERSION AS OF 0")
            assert False, "Expected AnalysisException for non-existent table"
        except Exception as e:
            assert "not found" in str(e).lower() or "does not exist" in str(e).lower()

    def test_restore_invalid_syntax_raises(self, spark):
        """Test RESTORE raises error for invalid syntax."""
        # Create Delta table
        data = [{"id": 1}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").saveAsTable("test_restore_syntax")

        # Missing VERSION AS OF or TIMESTAMP AS OF
        try:
            spark.sql("RESTORE TABLE test_restore_syntax TO")
            assert False, "Expected AnalysisException for invalid syntax"
        except Exception as e:
            assert "syntax" in str(e).lower() or "invalid" in str(e).lower()

    def test_restore_table_still_accessible(self, spark):
        """Test table is still accessible after RESTORE SQL command."""
        # Create Delta table
        data = [{"x": 1}, {"x": 2}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").saveAsTable("test_restore_parity")

        # SQL RESTORE should complete without error
        result = spark.sql("RESTORE TABLE test_restore_parity TO VERSION AS OF 0")
        assert len(result.collect()) == 1  # Returns restore operation details

        # Table should still be accessible after restore
        table_data = spark.table("test_restore_parity").collect()
        assert len(table_data) == 2

    def test_restore_to_version_with_different_versions(self, spark):
        """Test RESTORE to different version numbers."""
        # Create Delta table
        data = [{"id": 1}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").saveAsTable("test_restore_versions")

        # Test restoring to version 0
        result = spark.sql("RESTORE TABLE test_restore_versions TO VERSION AS OF 0")
        assert len(result.collect()) == 1

        # Test restoring to version 5 (mock accepts any version)
        result = spark.sql("RESTORE TABLE test_restore_versions TO VERSION AS OF 5")
        assert len(result.collect()) == 1
