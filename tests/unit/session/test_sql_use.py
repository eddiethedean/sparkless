"""Tests for USE SQL support.

USE operations in sparkless allow switching catalogs and databases
for testing code that uses USE statements.
"""

import pytest
from tests.fixtures.spark_backend import BackendType, get_backend_type


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend: BackendType = get_backend_type()
    result: bool = backend == BackendType.PYSPARK
    return result


class TestUseCatalog:
    """Tests for USE CATALOG."""

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="USE CATALOG behavior differs in PySpark",
    )
    def test_use_catalog_sets_current_catalog(self, spark) -> None:
        """USE CATALOG should set the current catalog."""
        # Set catalog
        spark.sql("USE CATALOG my_catalog")

        # Verify catalog was set
        assert spark.catalog.currentCatalog() == "my_catalog"

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="USE CATALOG behavior differs in PySpark",
    )
    def test_use_catalog_with_backticks(self, spark) -> None:
        """USE CATALOG should handle backtick-quoted names."""
        spark.sql("USE CATALOG `another_catalog`")
        assert spark.catalog.currentCatalog() == "another_catalog"


class TestUseDatabase:
    """Tests for USE DATABASE."""

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="USE DATABASE behavior differs in PySpark",
    )
    def test_use_database_sets_current_database(self, spark) -> None:
        """USE DATABASE should set the current database."""
        try:
            # Create test database
            spark.sql("CREATE SCHEMA IF NOT EXISTS test_db")

            # Set database
            spark.sql("USE DATABASE test_db")

            # Verify database was set
            assert spark.catalog.currentDatabase() == "test_db"
        finally:
            spark.sql("DROP SCHEMA IF EXISTS test_db")

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="USE DATABASE behavior differs in PySpark",
    )
    def test_use_database_nonexistent_raises_error(self, spark) -> None:
        """USE DATABASE on non-existent database should raise error."""
        with pytest.raises(Exception) as exc_info:
            spark.sql("USE DATABASE nonexistent_db")

        assert "does not exist" in str(exc_info.value).lower()


class TestUseSchema:
    """Tests for USE SCHEMA."""

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="USE SCHEMA behavior differs in PySpark",
    )
    def test_use_schema_sets_current_database(self, spark) -> None:
        """USE SCHEMA should set the current database (schema = database)."""
        try:
            # Create test schema
            spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

            # Set schema
            spark.sql("USE SCHEMA test_schema")

            # Verify schema/database was set
            assert spark.catalog.currentDatabase() == "test_schema"
        finally:
            spark.sql("DROP SCHEMA IF EXISTS test_schema")


class TestUseShorthand:
    """Tests for USE shorthand (USE database_name)."""

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="USE shorthand behavior differs in PySpark",
    )
    def test_use_shorthand_sets_current_database(self, spark) -> None:
        """USE database_name should set the current database."""
        try:
            # Create test database
            spark.sql("CREATE SCHEMA IF NOT EXISTS shorthand_db")

            # Use shorthand syntax
            spark.sql("USE shorthand_db")

            # Verify database was set
            assert spark.catalog.currentDatabase() == "shorthand_db"
        finally:
            spark.sql("DROP SCHEMA IF EXISTS shorthand_db")

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="USE shorthand behavior differs in PySpark",
    )
    def test_use_shorthand_with_backticks(self, spark) -> None:
        """USE `database_name` should handle backticks."""
        try:
            spark.sql("CREATE SCHEMA IF NOT EXISTS backtick_db")
            spark.sql("USE `backtick_db`")
            assert spark.catalog.currentDatabase() == "backtick_db"
        finally:
            spark.sql("DROP SCHEMA IF EXISTS backtick_db")


class TestUseCatalogIntegration:
    """Integration tests for USE with other operations."""

    @pytest.mark.skipif(  # type: ignore[untyped-decorator]
        _is_pyspark_mode(),
        reason="USE CATALOG behavior differs in PySpark",
    )
    def test_use_database_affects_table_resolution(self, spark) -> None:
        """USE DATABASE should affect table name resolution."""
        try:
            # Create two databases with tables
            spark.sql("CREATE SCHEMA IF NOT EXISTS db_a")
            spark.sql("CREATE SCHEMA IF NOT EXISTS db_b")

            # Create table in db_a
            spark.sql("USE DATABASE db_a")
            data = [("Alice", 25)]
            df = spark.createDataFrame(data, ["name", "age"])
            df.write.mode("overwrite").saveAsTable("users")

            # Verify table is in db_a
            assert spark.catalog.tableExists("users")

            # Switch to db_b
            spark.sql("USE DATABASE db_b")

            # Table should not exist in db_b
            assert not spark.catalog.tableExists("users")

            # But should still be accessible with qualified name
            result = spark.sql("SELECT * FROM db_a.users")
            assert result.count() == 1

        finally:
            spark.sql("DROP TABLE IF EXISTS db_a.users")
            spark.sql("DROP SCHEMA IF EXISTS db_a")
            spark.sql("DROP SCHEMA IF EXISTS db_b")
