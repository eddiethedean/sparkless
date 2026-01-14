"""
Core session implementation for Sparkless.

This module provides the core SparkSession class for session management,
maintaining compatibility with PySpark's SparkSession interface.
"""

from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple, Union, cast

try:
    import pandas as pd
except ImportError:
    pd = None
from ...core.interfaces.dataframe import IDataFrame
from ..context import SparkContext
from ..catalog import Catalog
from ..config import Configuration, SparkConfig
from ..sql.executor import SQLExecutor
from sparkless.backend.factory import BackendFactory
from sparkless.backend.protocols import StorageBackend
from sparkless.config import resolve_backend_type
from sparkless.dataframe import DataFrame, DataFrameReader
from ...spark_types import (
    StructType,
)
from ..services.dataframe_factory import DataFrameFactory
from ..services.sql_parameter_binder import SQLParameterBinder
from ..services.lifecycle_manager import SessionLifecycleManager
from ..services.mocking_coordinator import MockingCoordinator
import contextlib

if TYPE_CHECKING:
    from ..services.protocols import (
        IDataFrameFactory,
        ISQLParameterBinder,
        ISessionLifecycleManager,
        IMockingCoordinator,
    )
    from ...core.interfaces.session import ISession


class SparkSession:
    """SparklessSession providing complete PySpark API compatibility.

    Provides a comprehensive mock implementation of PySpark's SparkSession
    that supports all major operations including DataFrame creation, SQL
    queries, catalog management, and configuration without requiring JVM.

    Attributes:
        app_name: Application name for the Spark session.
        sparkContext: SparkContext instance for session context.
        catalog: Catalog instance for database and table operations.
        conf: Configuration object for session settings.

    Example:
        >>> spark = SparkSession("MyApp")
        >>> df = spark.createDataFrame([{"name": "Alice", "age": 25}])
        >>> df.select("name").show()
        >>> spark.sql("CREATE DATABASE test")
        >>> spark.stop()
    """

    # Class attribute for builder pattern
    builder: Optional["SparkSessionBuilder"] = None
    _singleton_session: Optional["SparkSession"] = None
    _active_sessions: List["SparkSession"] = []

    def __init__(
        self,
        app_name: str = "SparklessApp",
        validation_mode: str = "relaxed",
        enable_type_coercion: bool = True,
        enable_lazy_evaluation: bool = True,
        max_memory: str = "1GB",
        allow_disk_spillover: bool = False,
        storage_backend: Optional[StorageBackend] = None,
        backend_type: Optional[str] = None,
        db_path: Optional[str] = None,
        performance_mode: str = "fast",
    ):
        """Initialize SparkSession.

        Args:
            app_name: Application name for the Spark session.
            validation_mode: "strict", "relaxed", or "minimal" validation behavior.
            enable_type_coercion: Whether to coerce basic types during DataFrame creation.
            enable_lazy_evaluation: Whether to enable lazy evaluation (default True).
            max_memory: Maximum memory for backend to use (e.g., '1GB', '4GB', '8GB').
                       Default is '1GB' for test isolation.
            allow_disk_spillover: If True, allows backend to spill to disk when memory is full.
                                 If False (default), disables spillover for test isolation.
            storage_backend: Optional storage backend instance. If None, creates backend based on backend_type.
            backend_type: Type of backend to use ("polars", "memory", "file", optional "duckdb").
                If omitted, resolves from the ``MOCK_SPARK_BACKEND`` environment variable or defaults
                to "polars".
            db_path: Optional path to persistent database file. If provided, tables will persist across sessions.
                    If None (default), uses in-memory storage and tables don't persist.
                    For test scenarios requiring table persistence across multiple pipeline runs,
                    provide a db_path (e.g., "test.db" or ":memory:" for in-memory).
                    Note: In-memory storage (default) provides test isolation but tables don't persist
                    between session restarts. Use persistent storage for incremental pipeline testing.
        """
        self.app_name = app_name
        self.performance_mode = performance_mode
        self._jvm_overhead = 0.001 if performance_mode == "realistic" else 0.00001
        resolved_backend_type = resolve_backend_type(backend_type)
        self.backend_type = resolved_backend_type
        # Use dependency injection for storage backend
        if storage_backend is None:
            self._storage = BackendFactory.create_storage_backend(
                backend_type=resolved_backend_type,
                db_path=db_path,
                max_memory=max_memory,
                allow_disk_spillover=allow_disk_spillover,
            )
        else:
            self._storage = storage_backend
        from typing import cast

        self._catalog = Catalog(self._storage, spark=self)
        self.sparkContext = SparkContext(app_name)
        self._conf = Configuration()
        from ..._version import __version__

        self._version: str = __version__  # Use centralized version

        self._sql_executor = SQLExecutor(cast("ISession", self))

        # Mockable method implementations
        self._createDataFrame_impl = self._real_createDataFrame
        self._original_createDataFrame_impl = (
            self._real_createDataFrame
        )  # Store original for mock detection
        self._table_impl = self._real_table
        self._sql_impl = self._real_sql
        # Plugins (Phase 4)
        self._plugins: List[Any] = []

        # Error simulation

        # Validation settings (Phase 2 plumbing)
        self._engine_config = SparkConfig(
            validation_mode=validation_mode,
            enable_type_coercion=enable_type_coercion,
            enable_lazy_evaluation=enable_lazy_evaluation,
        )

        # Performance and memory tracking (delegated to SessionPerformanceTracker)
        from ..performance_tracker import SessionPerformanceTracker

        self._performance_tracker = SessionPerformanceTracker()

        # Service dependencies (injected, typed with Protocols)
        self._dataframe_factory: IDataFrameFactory = DataFrameFactory()
        self._sql_parameter_binder: ISQLParameterBinder = SQLParameterBinder()
        self._lifecycle_manager: ISessionLifecycleManager = SessionLifecycleManager()
        self._mocking_coordinator: IMockingCoordinator = MockingCoordinator()

        # Register this session as active
        SparkSession._active_sessions.append(self)
        SparkSession._singleton_session = self

    def _simulate_jvm_overhead(self, operations: int = 1) -> None:
        """Simulate JVM overhead for realistic performance.

        Args:
            operations: Number of operations to simulate overhead for.
        """
        if self.performance_mode == "realistic":
            import time

            time.sleep(self._jvm_overhead * operations)

    @property
    def appName(self) -> str:
        """Get application name."""
        return self.app_name

    @property
    def version(self) -> str:
        """Get Spark version."""
        return self._version

    @property
    def catalog(self) -> Catalog:
        """Get the catalog."""
        return self._catalog

    @property
    def conf(self) -> Configuration:
        """Get configuration."""
        return self._conf

    @property
    def read(self) -> DataFrameReader:
        """Get DataFrame reader."""
        return DataFrameReader(cast("ISession", self))

    def createDataFrame(
        self,
        data: Union[List[Dict[str, Any]], List[Any]],
        schema: Optional[Union[StructType, List[str]]] = None,
    ) -> "DataFrame":
        """Create a DataFrame from data (mockable version)."""
        # Use the mock implementation if set, otherwise use the real implementation
        return self._createDataFrame_impl(data, schema)

    def _real_createDataFrame(
        self,
        data: Union[List[Dict[str, Any]], List[Any], Any],
        schema: Optional[Union[StructType, List[str], str]] = None,
    ) -> "DataFrame":
        """Create a DataFrame from data.

        Args:
            data: List of dictionaries or tuples representing rows.
            schema: Optional schema definition (StructType or list of column names).

        Returns:
            DataFrame instance with the specified data and schema.

        Raises:
            IllegalArgumentException: If data is not in the expected format.

        Example:
            >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
            >>> df = spark.createDataFrame(data)
            >>> df = spark.createDataFrame(data, ["name", "age"])
        """
        # Plugin hook: before_create_dataframe
        for plugin in getattr(self, "_plugins", []):
            if hasattr(plugin, "before_create_dataframe"):
                with contextlib.suppress(Exception):
                    data, schema = plugin.before_create_dataframe(self, data, schema)

        # Delegate to DataFrameFactory service
        df = self._dataframe_factory.create_dataframe(
            data, schema, self._engine_config, self._storage
        )

        # Apply lazy/eager mode based on session config
        try:
            if hasattr(df, "withLazy"):
                lazy_enabled = getattr(
                    self._engine_config, "enable_lazy_evaluation", True
                )
                df = df.withLazy(lazy_enabled)
        except Exception:
            pass

        # Plugin hook: after_create_dataframe
        for plugin in getattr(self, "_plugins", []):
            if hasattr(plugin, "after_create_dataframe"):
                with contextlib.suppress(Exception):
                    df = plugin.after_create_dataframe(self, df)

        # Track memory usage for newly created DataFrame
        with contextlib.suppress(Exception):
            self._track_dataframe(df)
        return df

    def _track_dataframe(self, df: DataFrame) -> None:
        """Track DataFrame for approximate memory accounting."""
        self._performance_tracker.track_dataframe(df)

    def _get_memory_usage(self) -> int:
        """Return approximate memory usage in bytes for tracked DataFrames."""
        return self._performance_tracker.get_memory_usage()

    def clear_cache(self) -> None:
        """Clear tracked DataFrames to free memory accounting."""
        self._performance_tracker.clear_cache()

    def _benchmark_operation(
        self, operation_name: str, func: Any, *args: Any, **kwargs: Any
    ) -> Any:
        """Benchmark an operation and record simple telemetry.

        Returns the function result. Records duration (s), memory_used (bytes),
        and result_size when possible.
        """
        return self._performance_tracker.benchmark_operation(
            operation_name, func, *args, **kwargs
        )

    def _get_benchmark_results(self) -> Dict[str, Dict[str, Any]]:
        """Return a copy of the latest benchmark results."""
        return self._performance_tracker.get_benchmark_results()

    def _infer_type(self, value: Any) -> Any:
        """Infer data type from value.

        Delegates to SchemaInferenceEngine for consistency.

        Args:
            value: Value to infer type from.

        Returns:
            Inferred data type.
        """
        from ...core.schema_inference import SchemaInferenceEngine

        return SchemaInferenceEngine._infer_type(value)

    # ---------------------------
    # Validation and Coercion
    # ---------------------------
    # NOTE: Validation and coercion logic extracted to DataValidator
    # See: sparkless/core/data_validation.py

    def sql(self, query: str, *args: Any, **kwargs: Any) -> IDataFrame:
        """Execute SQL query with optional parameters (mockable version).

        Args:
            query: SQL query string with optional placeholders.
            *args: Positional parameters for ? placeholders.
            **kwargs: Named parameters for :name placeholders.

        Returns:
            DataFrame with query results.

        Example:
            >>> spark.sql("SELECT * FROM users WHERE age > ?", 18)
            >>> spark.sql("SELECT * FROM users WHERE age > :min_age", min_age=18)
        """
        return self._sql_impl(query, *args, **kwargs)

    def _real_sql(self, query: str, *args: Any, **kwargs: Any) -> IDataFrame:
        """Execute SQL query with optional parameters.

        Args:
            query: SQL query string with optional placeholders.
            *args: Positional parameters for ? placeholders.
            **kwargs: Named parameters for :name placeholders.

        Returns:
            DataFrame with query results.

        Example:
            >>> df = spark.sql("SELECT * FROM users WHERE age > ?", 18)
            >>> df = spark.sql("SELECT * FROM users WHERE name = :name", name="Alice")
        """
        # Process parameters if provided
        if args or kwargs:
            query = self._sql_parameter_binder.bind_parameters(query, args, kwargs)

        return self._sql_executor.execute(query)

    def _bind_parameters(
        self, query: str, args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> str:
        """Bind parameters to SQL query safely.

        Args:
            query: SQL query with placeholders.
            args: Positional parameters.
            kwargs: Named parameters.

        Returns:
            Query with parameters bound.
        """
        # Delegate to SQLParameterBinder service
        return self._sql_parameter_binder.bind_parameters(query, args, kwargs)

    def _format_param(self, value: Any) -> str:
        """Format a parameter value for SQL safely.

        Args:
            value: Parameter value.

        Returns:
            Formatted parameter string.
        """
        # Delegate to SQLParameterBinder service
        return self._sql_parameter_binder._format_param(value)

    def table(self, table_name: str) -> IDataFrame:
        """Get table as DataFrame (mockable version)."""
        self._check_error_rules("table", table_name)
        return self._table_impl(table_name)

    # ---------------------------
    # Plugin registration (Phase 4)
    # ---------------------------
    def _register_plugin(self, plugin: Any) -> None:
        self._plugins.append(plugin)

    def _real_table(self, table_name: str) -> IDataFrame:
        """Get table as DataFrame.

        Args:
            table_name: Table name.

        Returns:
            DataFrame with table data.

        Example:
            >>> df = spark.table("users")
        """
        # Parse table name
        if "." in table_name:
            schema, table = table_name.split(".", 1)
        else:
            # Use current schema instead of hardcoded "default"
            schema = self._storage.get_current_schema()
            table = table_name
        # Handle global temp views using Spark's convention 'global_temp'
        if schema == "global_temp":
            schema = "global_temp"

        # Check if table exists
        if not self._storage.table_exists(schema, table):
            # Fallback: Try to get table schema directly
            # This handles edge cases where storage has the table but table_exists() check fails
            # This can happen immediately after saveAsTable() if there's a brief synchronization delay
            try:
                table_schema = self._storage.get_table_schema(schema, table)
                if table_schema is not None and len(table_schema.fields) > 0:
                    # Table exists in storage but wasn't detected by table_exists()
                    # This is acceptable - proceed with reading the table
                    pass  # Continue to read the table below
                else:
                    # Table doesn't exist - raise exception
                    from sparkless.errors import AnalysisException

                    raise AnalysisException(f"Table or view not found: {table_name}")
            except Exception:
                # Schema retrieval failed - table doesn't exist
                from sparkless.errors import AnalysisException

                raise AnalysisException(f"Table or view not found: {table_name}")

        # Get table data and schema
        table_data = self._storage.get_data(schema, table)
        table_schema = self._storage.get_table_schema(schema, table)

        # Ensure schema is not None
        if table_schema is None:
            from ...spark_types import StructType  # type: ignore[unreachable]

            table_schema = StructType([])
        else:
            # When reading from a table, reset all fields to nullable=True to match PySpark behavior
            # Storage formats (Parquet/Delta) typically make columns nullable by default
            from ...spark_types import StructField, StructType
            from ...spark_types import (
                BooleanType,
                IntegerType,
                LongType,
                DoubleType,
                StringType,
                DataType,
            )

            updated_fields = []
            for field in table_schema.fields:
                # Create new data type with nullable=True
                data_type: DataType
                field_type = getattr(field, "dataType", None) or getattr(
                    field, "data_type", None
                )
                if isinstance(field_type, BooleanType):
                    data_type = BooleanType(nullable=True)
                elif isinstance(field_type, IntegerType):
                    data_type = IntegerType(nullable=True)
                elif isinstance(field_type, LongType):
                    data_type = LongType(nullable=True)
                elif isinstance(field_type, DoubleType):
                    data_type = DoubleType(nullable=True)
                elif isinstance(field_type, StringType):
                    data_type = StringType(nullable=True)
                else:
                    # For other types, create with nullable=True
                    if field_type is None:
                        data_type = StringType(nullable=True)  # fallback
                    else:
                        data_type = field_type.__class__(nullable=True)

                # Create new field with nullable=True
                updated_field = StructField(field.name, data_type, nullable=True)
                updated_fields.append(updated_field)

            table_schema = StructType(updated_fields)

        return cast("IDataFrame", DataFrame(table_data, table_schema, self._storage))

    def range(
        self, start: int, end: int, step: int = 1, numPartitions: Optional[int] = None
    ) -> "DataFrame":
        """Create DataFrame with range of numbers.

        Args:
            start: Start value (inclusive).
            end: End value (exclusive).
            step: Step size.
            numPartitions: Number of partitions (ignored in mock).

        Returns:
            DataFrame with range data.

        Example:
            >>> df = spark.range(0, 10, 2)
        """
        data = [{"id": i} for i in range(start, end, step)]
        return self.createDataFrame(data, ["id"])

    def stop(self) -> None:
        """Stop the session and clean up resources."""
        # Delegate to SessionLifecycleManager service
        self._lifecycle_manager.stop_session(self._storage, self._performance_tracker)
        # Remove from active sessions list
        if self in SparkSession._active_sessions:
            SparkSession._active_sessions.remove(self)
        # Clear singleton if this is the active singleton session
        if SparkSession._singleton_session is self:
            SparkSession._singleton_session = None

    def __enter__(self) -> "SparkSession":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        # Delegate to SessionLifecycleManager service
        self._lifecycle_manager.stop_session(self._storage, self._performance_tracker)
        # Remove from active sessions list
        if self in SparkSession._active_sessions:
            SparkSession._active_sessions.remove(self)
        # Clear singleton if this is the active singleton session
        if SparkSession._singleton_session is self:
            SparkSession._singleton_session = None

    def newSession(self) -> "SparkSession":
        """Create new session.

        Returns:
            New SparkSession instance.
        """
        return SparkSession(self.app_name)

    @classmethod
    def _get_active_session(cls) -> Optional["SparkSession"]:
        """Get the most recently created active session.

        Returns:
            The most recent active SparkSession, or None if no sessions are active.
        """
        if cls._active_sessions:
            return cls._active_sessions[-1]
        return None

    @classmethod
    def getActiveSession(cls) -> Optional["SparkSession"]:
        """Get the currently active SparkSession (PySpark-compatible).

        This method matches PySpark's SparkSession.getActiveSession() API,
        allowing code written for PySpark to work seamlessly with sparkless.

        Returns:
            The currently active SparkSession, or None if no session is active.
            Prefers singleton session if available, otherwise returns the most
            recent active session.

        Example:
            >>> spark = SparkSession("MyApp")
            >>> active = SparkSession.getActiveSession()
            >>> assert active is spark
        """
        # Prefer singleton session (matches PySpark behavior)
        if cls._singleton_session is not None:
            return cls._singleton_session
        # Fallback to most recent active session
        if cls._active_sessions:
            return cls._active_sessions[-1]
        return None

    @classmethod
    def _has_active_session(cls) -> bool:
        """Check if there's at least one active session.

        Returns:
            True if there's at least one active session, False otherwise.
        """
        return len(cls._active_sessions) > 0

    # Mockable methods for testing
    def _mock_createDataFrame(
        self, side_effect: Any = None, return_value: Any = None
    ) -> None:
        """Mock createDataFrame method for testing."""
        # Delegate to MockingCoordinator service
        self._createDataFrame_impl = self._mocking_coordinator.setup_mock_impl(
            "createDataFrame", side_effect, return_value
        )

    def _mock_table(self, side_effect: Any = None, return_value: Any = None) -> None:
        """Mock table method for testing."""
        # Delegate to MockingCoordinator service
        self._table_impl = self._mocking_coordinator.setup_mock_impl(
            "table", side_effect, return_value
        )

    def _mock_sql(self, side_effect: Any = None, return_value: Any = None) -> None:
        """Mock sql method for testing."""
        # Delegate to MockingCoordinator service
        self._sql_impl = self._mocking_coordinator.setup_mock_impl(
            "sql", side_effect, return_value
        )

    # Error simulation methods
    def _add_error_rule(
        self, method_name: str, error_condition: Any, error_exception: Any
    ) -> None:
        """Add error simulation rule."""
        # Delegate to MockingCoordinator service
        self._mocking_coordinator.add_error_rule(
            method_name, error_condition, error_exception
        )

    def _clear_error_rules(self) -> None:
        """Clear all error simulation rules."""
        # Delegate to MockingCoordinator service
        self._mocking_coordinator.clear_error_rules()

    def _reset_mocks(self) -> None:
        """Reset all mocks to original implementations."""
        # Delegate to MockingCoordinator service
        original_impls = {
            "createDataFrame": self._real_createDataFrame,
            "table": self._real_table,
            "sql": self._real_sql,
        }
        self._mocking_coordinator.reset_all_mocks(original_impls)

        # Reset implementations to original
        self._createDataFrame_impl = self._real_createDataFrame
        self._table_impl = self._real_table
        self._sql_impl = self._real_sql

    def _check_error_rules(self, method_name: str, *args: Any, **kwargs: Any) -> None:
        """Check if error should be raised for method."""
        # Delegate to MockingCoordinator service
        exception = self._mocking_coordinator.check_error_rules(
            method_name, *args, **kwargs
        )
        if exception:
            raise exception

    # Integration with MockErrorSimulator
    # Note: _add_error_rule is already defined above at line 606
    # This comment is kept for documentation purposes

    def _remove_error_rule(self, method_name: str, condition: Any = None) -> None:
        """Remove error rule (used by MockErrorSimulator)."""
        # Note: MockingCoordinator doesn't support removal yet, but we can clear all rules
        if condition is None:
            self._mocking_coordinator.clear_error_rules()

    def _should_raise_error(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        """Check if error should be raised (used by MockErrorSimulator)."""
        # Delegate to MockingCoordinator service
        return self._mocking_coordinator.check_error_rules(method_name, *args, **kwargs)


# Set the builder attribute on SparkSession
from .builder import SparkSessionBuilder  # noqa: E402

SparkSession.builder = SparkSessionBuilder()
