"""Tests for ALTER TABLE SQL support.

ALTER TABLE operations in sparkless are primarily no-ops that validate
table existence. They are used to support testing of code that uses
ALTER TABLE statements without requiring actual schema modifications.
"""

import pytest
from tests.fixtures.spark_backend import BackendType, get_backend_type


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend: BackendType = get_backend_type()
    return backend == BackendType.PYSPARK


class TestAlterTableClusterBy:
    """Tests for ALTER TABLE ... CLUSTER BY."""

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="ALTER TABLE CLUSTER BY requires Databricks runtime",
    )
    def test_alter_table_cluster_by_columns(self, spark) -> None:
        """ALTER TABLE ... CLUSTER BY (col1, col2) should succeed."""
        try:
            # Create test table
            data = [("Alice", 25, "NYC"), ("Bob", 30, "LA")]
            df = spark.createDataFrame(data, ["name", "age", "city"])
            df.write.mode("overwrite").saveAsTable("cluster_test")

            # ALTER TABLE with CLUSTER BY should succeed (no-op in sparkless)
            spark.sql("ALTER TABLE cluster_test CLUSTER BY (name, city)")

            # Table should still be queryable
            result = spark.sql("SELECT * FROM cluster_test")
            assert result.count() == 2
        finally:
            spark.sql("DROP TABLE IF EXISTS cluster_test")

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="ALTER TABLE CLUSTER BY requires Databricks runtime",
    )
    def test_alter_table_cluster_by_none(self, spark) -> None:
        """ALTER TABLE ... CLUSTER BY NONE should disable clustering."""
        try:
            # Create test table
            data = [("Alice", 25)]
            df = spark.createDataFrame(data, ["name", "age"])
            df.write.mode("overwrite").saveAsTable("cluster_none_test")

            # CLUSTER BY NONE should succeed
            spark.sql("ALTER TABLE cluster_none_test CLUSTER BY NONE")

            # Table should still be queryable
            result = spark.sql("SELECT * FROM cluster_none_test")
            assert result.count() == 1
        finally:
            spark.sql("DROP TABLE IF EXISTS cluster_none_test")

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="ALTER TABLE CLUSTER BY requires Databricks runtime",
    )
    def test_alter_table_cluster_by_nonexistent_table(self, spark) -> None:
        """ALTER TABLE on non-existent table should raise error."""
        with pytest.raises(Exception) as exc_info:
            spark.sql("ALTER TABLE nonexistent_table CLUSTER BY (col1)")

        assert "does not exist" in str(exc_info.value).lower()


class TestAlterTableAddColumn:
    """Tests for ALTER TABLE ... ADD COLUMN."""

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="ALTER TABLE ADD COLUMN behavior differs in PySpark",
    )
    def test_alter_table_add_column(self, spark) -> None:
        """ALTER TABLE ... ADD COLUMN should succeed."""
        try:
            # Create test table
            data = [("Alice", 25)]
            df = spark.createDataFrame(data, ["name", "age"])
            df.write.mode("overwrite").saveAsTable("add_col_test")

            # ADD COLUMN should succeed (no-op in sparkless)
            spark.sql("ALTER TABLE add_col_test ADD COLUMN city STRING")

            # Table should still be queryable
            result = spark.sql("SELECT * FROM add_col_test")
            assert result.count() == 1
        finally:
            spark.sql("DROP TABLE IF EXISTS add_col_test")


class TestAlterTableAlterColumn:
    """Tests for ALTER TABLE ... ALTER COLUMN."""

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="ALTER TABLE ALTER COLUMN behavior differs in PySpark",
    )
    def test_alter_column_type(self, spark) -> None:
        """ALTER TABLE ... ALTER COLUMN col TYPE should succeed."""
        try:
            # Create test table
            data = [("Alice", 25)]
            df = spark.createDataFrame(data, ["name", "age"])
            df.write.mode("overwrite").saveAsTable("alter_type_test")

            # ALTER COLUMN TYPE should succeed (no-op in sparkless)
            spark.sql("ALTER TABLE alter_type_test ALTER COLUMN age TYPE BIGINT")

            # Table should still be queryable
            result = spark.sql("SELECT * FROM alter_type_test")
            assert result.count() == 1
        finally:
            spark.sql("DROP TABLE IF EXISTS alter_type_test")

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="ALTER TABLE SET NOT NULL behavior differs in PySpark",
    )
    def test_alter_column_set_not_null(self, spark) -> None:
        """ALTER TABLE ... ALTER COLUMN col SET NOT NULL should succeed."""
        try:
            # Create test table
            data = [("Alice", 25)]
            df = spark.createDataFrame(data, ["name", "age"])
            df.write.mode("overwrite").saveAsTable("set_not_null_test")

            # SET NOT NULL should succeed (no-op in sparkless)
            spark.sql("ALTER TABLE set_not_null_test ALTER COLUMN name SET NOT NULL")

            # Table should still be queryable
            result = spark.sql("SELECT * FROM set_not_null_test")
            assert result.count() == 1
        finally:
            spark.sql("DROP TABLE IF EXISTS set_not_null_test")

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="ALTER TABLE DROP NOT NULL behavior differs in PySpark",
    )
    def test_alter_column_drop_not_null(self, spark) -> None:
        """ALTER TABLE ... ALTER COLUMN col DROP NOT NULL should succeed."""
        try:
            # Create test table
            data = [("Alice", 25)]
            df = spark.createDataFrame(data, ["name", "age"])
            df.write.mode("overwrite").saveAsTable("drop_not_null_test")

            # DROP NOT NULL should succeed (no-op in sparkless)
            spark.sql("ALTER TABLE drop_not_null_test ALTER COLUMN name DROP NOT NULL")

            # Table should still be queryable
            result = spark.sql("SELECT * FROM drop_not_null_test")
            assert result.count() == 1
        finally:
            spark.sql("DROP TABLE IF EXISTS drop_not_null_test")


class TestAlterTableDropColumn:
    """Tests for ALTER TABLE ... DROP COLUMN."""

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="ALTER TABLE DROP COLUMN behavior differs in PySpark",
    )
    def test_alter_table_drop_column(self, spark) -> None:
        """ALTER TABLE ... DROP COLUMN should succeed."""
        try:
            # Create test table
            data = [("Alice", 25, "NYC")]
            df = spark.createDataFrame(data, ["name", "age", "city"])
            df.write.mode("overwrite").saveAsTable("drop_col_test")

            # DROP COLUMN should succeed (no-op in sparkless)
            spark.sql("ALTER TABLE drop_col_test DROP COLUMN city")

            # Table should still be queryable
            result = spark.sql("SELECT * FROM drop_col_test")
            assert result.count() == 1
        finally:
            spark.sql("DROP TABLE IF EXISTS drop_col_test")


class TestAlterTableSetProperties:
    """Tests for ALTER TABLE ... SET TBLPROPERTIES."""

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="ALTER TABLE SET TBLPROPERTIES behavior differs in PySpark",
    )
    def test_alter_table_set_tblproperties(self, spark) -> None:
        """ALTER TABLE ... SET TBLPROPERTIES should succeed."""
        try:
            # Create test table
            data = [("Alice", 25)]
            df = spark.createDataFrame(data, ["name", "age"])
            df.write.mode("overwrite").saveAsTable("props_test")

            # SET TBLPROPERTIES should succeed (no-op in sparkless)
            spark.sql(
                "ALTER TABLE props_test SET TBLPROPERTIES "
                "('delta.columnMapping.mode' = 'name')"
            )

            # Table should still be queryable
            result = spark.sql("SELECT * FROM props_test")
            assert result.count() == 1
        finally:
            spark.sql("DROP TABLE IF EXISTS props_test")


class TestAlterTableWithSchema:
    """Tests for ALTER TABLE with schema-qualified table names."""

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="ALTER TABLE CLUSTER BY requires Databricks runtime",
    )
    def test_alter_table_with_schema_prefix(self, spark) -> None:
        """ALTER TABLE schema.table should work with qualified names."""
        try:
            # Create schema if not exists
            spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

            # Create test table in schema
            data = [("Alice", 25)]
            df = spark.createDataFrame(data, ["name", "age"])
            df.write.mode("overwrite").saveAsTable("test_schema.schema_test")

            # ALTER with schema prefix should succeed
            spark.sql("ALTER TABLE test_schema.schema_test CLUSTER BY (name)")

            # Table should still be queryable
            result = spark.sql("SELECT * FROM test_schema.schema_test")
            assert result.count() == 1
        finally:
            spark.sql("DROP TABLE IF EXISTS test_schema.schema_test")
            spark.sql("DROP SCHEMA IF EXISTS test_schema")
