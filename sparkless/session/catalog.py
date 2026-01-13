"""
Mock Catalog implementation for Sparkless.

This module provides a mock implementation of PySpark's Catalog
that behaves identically to the real Catalog for testing and development.
It includes database and table management, caching operations, and
catalog queries without requiring a JVM or actual Spark installation.

Key Features:
    - Complete PySpark Catalog API compatibility
    - Database management (create, list, drop)
    - Table management (create, list, drop, cache)
    - Schema validation and error handling
    - Integration with storage manager

Example:
    >>> from sparkless.session import Catalog
    >>> catalog = Catalog(storage_manager)
    >>> catalog.createDatabase("test_db")
    >>> catalog.listDatabases()
    [Database(name='test_db')]
"""

from typing import Any, List, Optional, Set
from ..core.interfaces.storage import IStorageManager
from ..core.exceptions.analysis import AnalysisException
from ..core.exceptions.validation import IllegalArgumentException


class Database:
    """Mock database object for catalog operations."""

    def __init__(self, name: str):
        """Initialize Database.

        Args:
            name: Database name.
        """
        self.name = name

    def __str__(self) -> str:
        """String representation."""
        return f"Database(name='{self.name}')"

    def __repr__(self) -> str:
        """Representation."""
        return self.__str__()


class Table:
    """Mock table object for catalog operations."""

    def __init__(self, name: str, database: str = "default"):
        """Initialize Table.

        Args:
            name: Table name.
            database: Database name.
        """
        self.name = name
        self.database = database

    def __str__(self) -> str:
        """String representation."""
        return f"Table(name='{self.name}', database='{self.database}')"

    def __repr__(self) -> str:
        """Representation."""
        return self.__str__()


class Catalog:
    """Mock Catalog for Spark session.

    Provides a comprehensive mock implementation of PySpark's Catalog
    that supports all major operations including database management,
    table operations, and caching without requiring actual Spark installation.

    Attributes:
        storage: Storage manager for data persistence.
        spark: Optional SparkSession reference for SQL-based operations.

    Example:
        >>> catalog = Catalog(storage_manager, spark_session)
        >>> catalog.createDatabase("test_db")
        >>> catalog.listDatabases()
        [Database(name='test_db')]
    """

    def __init__(self, storage: IStorageManager, spark: Optional[Any] = None):
        """Initialize Catalog.

        Args:
            storage: Storage manager instance.
            spark: Optional SparkSession instance for SQL-based operations.
                  If provided, createDatabase() will use SQL instead of direct storage calls.
        """
        self._storage = storage
        self.spark = spark
        self._cached_tables: Set[str] = set()  # Track cached tables

    def get_storage_backend(self) -> IStorageManager:
        """Get the storage backend instance.

        Public accessor method for the storage backend, allowing access
        without breaking encapsulation.

        Returns:
            The storage manager instance.
        """
        return self._storage

    def listDatabases(self) -> List[Database]:
        """List all databases.

        Returns:
            List of Database objects.
        """
        return [Database(name) for name in self._storage.list_schemas()]

    def setCurrentDatabase(self, dbName: str) -> None:
        """Set current/active database.

        Args:
            dbName: Database name to set as current.

        Raises:
            AnalysisException: If database does not exist.
        """
        if not self._storage.schema_exists(dbName):
            from sparkless.core.exceptions.analysis import AnalysisException

            raise AnalysisException(f"Database '{dbName}' does not exist")
        self._storage.set_current_schema(dbName)

    def currentDatabase(self) -> str:
        """Get current database name.

        Returns:
            Current database name.
        """
        return self._storage.get_current_schema()

    def currentCatalog(self) -> str:
        """Get current catalog name (Spark SQL compatibility).

        Returns:
            Catalog identifier. Sparkless exposes a single catalog.
        """
        return "spark_catalog"

    def createDatabase(self, name: str, ignoreIfExists: bool = True) -> None:
        """Create a database.

        This method uses SQL internally to match PySpark's behavior, where
        database creation is done via SQL statements rather than direct API calls.
        However, to avoid infinite recursion when called from SQL execution,
        it checks if the database already exists first and uses direct storage
        calls when appropriate.

        Args:
            name: Database name.
            ignoreIfExists: Whether to ignore if database already exists.

        Raises:
            IllegalArgumentException: If name is not a string or is empty.
            AnalysisException: If database already exists and ignoreIfExists is False.
        """
        if not isinstance(name, str):
            raise IllegalArgumentException("Database name must be a string")

        if not name:
            raise IllegalArgumentException("Database name cannot be empty")

        # Check if database already exists
        if self._storage.schema_exists(name):
            if not ignoreIfExists:
                raise AnalysisException(f"Database '{name}' already exists")
            # Database exists and ignoreIfExists is True, nothing to do
            return

        # Database doesn't exist, create it
        # Use direct storage call to avoid infinite recursion with SQL execution
        # (SQL CREATE DATABASE would call this method again)
        try:
            self._storage.create_schema(name)
        except Exception as e:
            if isinstance(e, (AnalysisException, IllegalArgumentException)):
                raise
            raise AnalysisException(f"Failed to create database '{name}': {str(e)}")

    def dropDatabase(
        self,
        name: str,
        ignoreIfNotExists: bool = True,
        ignore_if_not_exists: Optional[bool] = None,
        cascade: bool = False,
    ) -> None:
        """Drop a database.

        Args:
            name: Database name.
            ignoreIfNotExists: Whether to ignore if database doesn't exist (PySpark style).
            ignore_if_not_exists: Whether to ignore if database doesn't exist (Python style).
            cascade: Whether to drop tables in the database (ignored in mock).

        Raises:
            IllegalArgumentException: If name is not a string or is empty.
            AnalysisException: If database doesn't exist and ignoreIfNotExists is False.
        """
        if not isinstance(name, str):
            raise IllegalArgumentException("Database name must be a string")

        if not name:
            raise IllegalArgumentException("Database name cannot be empty")

        # Support both camelCase (PySpark) and snake_case (Python) parameter names
        ignore_flag = (
            ignore_if_not_exists
            if ignore_if_not_exists is not None
            else ignoreIfNotExists
        )

        if not ignore_flag and not self._storage.schema_exists(name):
            raise AnalysisException(f"Database '{name}' does not exist")

        if self._storage.schema_exists(name):
            try:
                self._storage.drop_schema(name)
            except Exception as e:
                if isinstance(e, (AnalysisException, IllegalArgumentException)):
                    raise
                raise AnalysisException(f"Failed to drop database '{name}': {str(e)}")

    def tableExists(self, tableName: str, dbName: Optional[str] = None) -> bool:
        """Check if table exists.

        Args:
            tableName: Table name or qualified name (schema.table).
            dbName: Optional database name. Uses current database if None.

        Returns:
            True if table exists, False otherwise.

        Raises:
            IllegalArgumentException: If names are not strings or are empty.
            AnalysisException: If there's an error checking table existence.
        """
        # Handle qualified table names (schema.table)
        if "." in tableName and dbName is None:
            parts = tableName.split(".", 1)
            if len(parts) == 2:
                dbName, tableName = parts
        elif dbName is not None and "." not in tableName:
            # If both tableName and dbName are provided and tableName is not qualified,
            # then dbName is the schema and tableName is the table name
            # This handles the case: tableExists("test_table", "test_schema")
            # But also check if tableName might actually be a schema name
            # (some PySpark code might call it as tableExists("schema", "table"))
            # Check if reversing makes more sense (if "tableName" exists as a schema)
            if self._storage.schema_exists(
                tableName
            ) and not self._storage.schema_exists(dbName):
                # If tableName is actually a schema and dbName is not, swap them
                # This handles: tableExists("test_schema", "test_table")
                # which should check table "test_table" in schema "test_schema"
                dbName, tableName = tableName, dbName

        if dbName is None:
            dbName = self._storage.get_current_schema()
        if not isinstance(dbName, str):
            raise IllegalArgumentException("Database name must be a string")

        if not isinstance(tableName, str):
            raise IllegalArgumentException("Table name must be a string")

        if not dbName:
            raise IllegalArgumentException("Database name cannot be empty")

        if not tableName:
            raise IllegalArgumentException("Table name cannot be empty")

        try:
            # First check storage directly
            exists = self._storage.table_exists(dbName, tableName)
            if exists:
                return True

            # If storage says it doesn't exist, try to verify by attempting to get schema
            # This handles cases where table exists but isn't properly registered
            # But only if schema exists (otherwise table definitely doesn't exist)
            if self._storage.schema_exists(dbName):
                try:
                    table_schema = self._storage.get_table_schema(dbName, tableName)
                    # Empty schema means table doesn't exist - check if it has fields
                    if table_schema is not None and len(table_schema.fields) > 0:
                        # Table exists in storage but wasn't detected by table_exists()
                        # This is a synchronization issue - return True since we can access it
                        return True
                except Exception:
                    # Table doesn't exist or can't be accessed
                    pass

            # Final check: try to list tables in the database and check if our table is there
            # This is a more comprehensive check that catches any synchronization issues
            try:
                table_list = self._storage.list_tables(dbName)
                if tableName in table_list:
                    return True
            except Exception:
                # Can't list tables or database doesn't exist
                pass

            return False
        except Exception as e:
            if isinstance(e, (AnalysisException, IllegalArgumentException)):
                raise
            raise AnalysisException(
                f"Failed to check table existence '{dbName}.{tableName}': {str(e)}"
            )

    def listTables(self, dbName: Optional[str] = None) -> List[Table]:
        """List tables in database.

        Args:
            dbName: Optional database name. Uses current database if None.

        Returns:
            List of MockTable objects.

        Raises:
            IllegalArgumentException: If dbName is not a string or is empty.
            AnalysisException: If database doesn't exist or there's an error.
        """
        if dbName is None:
            dbName = self._storage.get_current_schema()
        if not isinstance(dbName, str):
            raise IllegalArgumentException("Database name must be a string")

        if not dbName:
            raise IllegalArgumentException("Database name cannot be empty")

        if not self._storage.schema_exists(dbName):
            raise AnalysisException(f"Database '{dbName}' does not exist")

        try:
            table_names = self._storage.list_tables(dbName)
            return [Table(name, dbName) for name in table_names]
        except Exception as e:
            if isinstance(e, (AnalysisException, IllegalArgumentException)):
                raise
            raise AnalysisException(
                f"Failed to list tables in database '{dbName}': {str(e)}"
            )

    def createTable(
        self,
        tableName: str,
        path: str,
        source: str = "parquet",
        schema: Optional[Any] = None,
        **options: Any,
    ) -> None:
        """Create table.

        Args:
            tableName: Table name.
            path: Path to data.
            source: Data source format.
            schema: Table schema.
            **options: Additional options.
        """
        # Mock implementation - in real Spark this would create a table
        pass

    def dropTable(self, tableName: str) -> None:
        """Drop table.

        Args:
            tableName: Table name or qualified name (schema.table).

        Raises:
            IllegalArgumentException: If table name is invalid.
            AnalysisException: If table doesn't exist or can't be dropped.
        """
        if not isinstance(tableName, str):
            raise IllegalArgumentException("Table name must be a string")

        if not tableName:
            raise IllegalArgumentException("Table name cannot be empty")

        # Handle qualified table names (schema.table)
        if "." in tableName:
            parts = tableName.split(".", 1)
            if len(parts) == 2:
                dbName, tableName = parts
            else:
                raise IllegalArgumentException(
                    f"Invalid qualified table name: {tableName}"
                )
        else:
            dbName = self._storage.get_current_schema()

        try:
            # Check if table exists first
            if not self._storage.table_exists(dbName, tableName):
                raise AnalysisException(f"Table '{dbName}.{tableName}' does not exist")

            # Drop the table from storage
            self._storage.drop_table(dbName, tableName)
        except Exception as e:
            if isinstance(e, (AnalysisException, IllegalArgumentException)):
                raise
            raise AnalysisException(
                f"Failed to drop table '{dbName}.{tableName}': {str(e)}"
            )

    def isCached(self, tableName: str) -> bool:
        """Check if table is cached.

        Args:
            tableName: Table name or qualified name (schema.table).

        Returns:
            True if table is cached, False otherwise.

        Raises:
            IllegalArgumentException: If table name is invalid.
        """
        if not isinstance(tableName, str):
            raise IllegalArgumentException("Table name must be a string")

        if not tableName:
            raise IllegalArgumentException("Table name cannot be empty")

        # Handle qualified table names (schema.table)
        if "." in tableName:
            parts = tableName.split(".", 1)
            if len(parts) == 2:
                dbName, tableName = parts
                qualified_name = f"{dbName}.{tableName}"
            else:
                raise IllegalArgumentException(
                    f"Invalid qualified table name: {tableName}"
                )
        else:
            dbName = self._storage.get_current_schema()
            qualified_name = f"{dbName}.{tableName}"

        return qualified_name in self._cached_tables

    def cacheTable(self, tableName: str) -> None:
        """Cache table.

        Args:
            tableName: Table name or qualified name (schema.table).

        Raises:
            IllegalArgumentException: If table name is invalid.
            AnalysisException: If table doesn't exist.
        """
        if not isinstance(tableName, str):
            raise IllegalArgumentException("Table name must be a string")

        if not tableName:
            raise IllegalArgumentException("Table name cannot be empty")

        # Handle qualified table names (schema.table)
        if "." in tableName:
            parts = tableName.split(".", 1)
            if len(parts) == 2:
                dbName, tableName = parts
                qualified_name = f"{dbName}.{tableName}"
            else:
                raise IllegalArgumentException(
                    f"Invalid qualified table name: {tableName}"
                )
        else:
            dbName = self._storage.get_current_schema()
            qualified_name = f"{dbName}.{tableName}"

        # Check if table exists
        if not self._storage.table_exists(dbName, tableName):
            raise AnalysisException(f"Table '{qualified_name}' does not exist")

        # Add to cache
        self._cached_tables.add(qualified_name)

    def uncacheTable(self, tableName: str) -> None:
        """Uncache table.

        Args:
            tableName: Table name or qualified name (schema.table).

        Raises:
            IllegalArgumentException: If table name is invalid.
        """
        if not isinstance(tableName, str):
            raise IllegalArgumentException("Table name must be a string")

        if not tableName:
            raise IllegalArgumentException("Table name cannot be empty")

        # Handle qualified table names (schema.table)
        if "." in tableName:
            parts = tableName.split(".", 1)
            if len(parts) == 2:
                dbName, tableName = parts
                qualified_name = f"{dbName}.{tableName}"
            else:
                raise IllegalArgumentException(
                    f"Invalid qualified table name: {tableName}"
                )
        else:
            dbName = self._storage.get_current_schema()
            qualified_name = f"{dbName}.{tableName}"

        # Remove from cache
        self._cached_tables.discard(qualified_name)

    def refreshTable(self, tableName: str) -> None:
        """Refresh table.

        Args:
            tableName: Table name.
        """
        # Mock implementation - in real Spark this would refresh a table
        pass

    def refreshByPath(self, path: str) -> None:
        """Refresh by path.

        Args:
            path: Path to refresh.
        """
        # Mock implementation - in real Spark this would refresh by path
        pass

    def recoverPartitions(self, tableName: str) -> None:
        """Recover partitions.

        Args:
            tableName: Table name.
        """
        # Mock implementation - in real Spark this would recover partitions
        pass

    def getDatabase(self, dbName: str) -> Database:
        """Get database information.

        Args:
            dbName: Database name.

        Returns:
            Database object with database information.

        Raises:
            IllegalArgumentException: If database name is invalid.
            AnalysisException: If database doesn't exist.

        Example:
            >>> db = catalog.getDatabase("test_db")
            >>> print(db.name)
            test_db
        """
        if not isinstance(dbName, str):
            raise IllegalArgumentException("Database name must be a string")

        if not dbName:
            raise IllegalArgumentException("Database name cannot be empty")

        if not self._storage.schema_exists(dbName):
            raise AnalysisException(f"Database '{dbName}' does not exist")

        return Database(dbName)

    def getTable(
        self,
        tableName: Optional[str] = None,
        dbName: Optional[str] = None,
        *,
        databaseName: Optional[str] = None,
    ) -> Table:
        """Get table information.

        Args:
            tableName: Table name or qualified name (schema.table).
                      When called with two positional args, this may be dbName (PySpark compatibility).
            dbName: Optional database name. When called with two positional args, this may be tableName.
            databaseName: Optional keyword argument for database name (PySpark compatibility).

        Returns:
            Table object with table information.

        Raises:
            IllegalArgumentException: If table name is invalid.
            AnalysisException: If table doesn't exist.

        Example:
            >>> table = catalog.getTable("users", "test_db")  # Standard: (tableName, dbName)
            >>> table = catalog.getTable("test_db", "users")  # PySpark style: (dbName, tableName)
            >>> table = catalog.getTable(tableName="users", databaseName="test_db")  # Keyword args
        """
        # Use databaseName keyword arg if provided (PySpark style)
        if databaseName is not None:
            dbName = databaseName

        # Handle PySpark compatibility: when called with two positional args,
        # PySpark accepts (dbName, tableName) order as an alternative
        # We need to detect which order was intended. Try both and see which works.
        if tableName is not None and dbName is not None and databaseName is None:
            # Try standard order first: (tableName, dbName)
            # If that doesn't work, try PySpark order: (dbName, tableName)
            # Check if standard order would work by testing table existence
            if self._storage.table_exists(dbName, tableName):
                # Standard order works, use it
                pass
            else:
                # Standard order doesn't work, try PySpark order (swap)
                actual_table_name = dbName  # Second arg is table name in PySpark order
                actual_db_name = tableName  # First arg is db name in PySpark order
                # Only swap if the swapped version would work
                if self._storage.table_exists(actual_db_name, actual_table_name):
                    tableName = actual_table_name
                    dbName = actual_db_name
                # If neither works, proceed with standard order and let the error happen below

        # Handle case where only one positional arg is provided
        if tableName is None:
            raise IllegalArgumentException("Table name must be provided")

        if not isinstance(tableName, str):
            raise IllegalArgumentException("Table name must be a string")

        if not tableName:
            raise IllegalArgumentException("Table name cannot be empty")

        # Handle qualified table names (schema.table)
        if "." in tableName and dbName is None:
            parts = tableName.split(".", 1)
            if len(parts) == 2:
                dbName, tableName = parts
            else:
                raise IllegalArgumentException(
                    f"Invalid qualified table name: {tableName}"
                )
        elif dbName is None:
            dbName = self._storage.get_current_schema()

        if not isinstance(dbName, str):
            raise IllegalArgumentException("Database name must be a string")

        if not dbName:
            raise IllegalArgumentException("Database name cannot be empty")

        # Check if table exists
        if not self._storage.table_exists(dbName, tableName):
            qualified_name = f"{dbName}.{tableName}" if dbName else tableName
            raise AnalysisException(f"Table '{qualified_name}' does not exist")

        return Table(tableName, dbName)

    def clearCache(self) -> None:
        """Clear cache."""
        # Mock implementation - in real Spark this would clear the cache
        pass

    def _ensure_table_visible(self, schema: str, table: str) -> None:
        """Ensure table is immediately visible in catalog after creation.

        This method forces catalog/storage synchronization to ensure that
        tables written via saveAsTable() are immediately queryable via
        spark.table() without any delays.

        Args:
            schema: Schema/database name
            table: Table name

        Raises:
            AnalysisException: If table is not visible after all checks
        """
        # Check 1: Verify table_exists() returns True
        if self._storage.table_exists(schema, table):
            # Verify we can get schema (ensures table is fully registered)
            try:
                table_schema = self._storage.get_table_schema(schema, table)
                if table_schema is not None:
                    # Table is visible and has schema - success
                    return
            except Exception:
                # Schema retrieval failed, but table_exists returned True
                # This might indicate a partial registration
                pass

        # Check 2: Try to get schema directly (fallback for edge cases)
        try:
            table_schema = self._storage.get_table_schema(schema, table)
            if table_schema is not None and len(table_schema.fields) > 0:
                # Table exists in storage but wasn't detected by table_exists()
                # This is acceptable - table is queryable
                return
        except Exception:
            pass

        # Check 3: Verify table appears in list_tables()
        try:
            table_list = self._storage.list_tables(schema)
            if table in table_list:
                # Table is in the list - it's visible
                return
        except Exception:
            pass

        # All checks failed - table is not visible
        qualified_name = f"{schema}.{table}" if schema else table
        raise AnalysisException(
            f"Table '{qualified_name}' is not immediately visible in catalog "
            f"after saveAsTable(). This indicates a catalog synchronization issue."
        )
