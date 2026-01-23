"""
Mock DataFrame implementation for Sparkless.

This module provides a complete mock implementation of PySpark DataFrame
that behaves identically to the real PySpark DataFrame for testing and
development purposes. It supports all major DataFrame operations including
selection, filtering, grouping, joining, and window functions.

Key Features:
    - Complete PySpark API compatibility
    - 100% type-safe operations with mypy compliance
    - Window function support with partitioning and ordering
    - Comprehensive error handling matching PySpark exceptions
    - In-memory storage for fast test execution
    - Mockable methods for error testing scenarios
    - Enhanced DataFrameWriter with all save modes
    - Advanced data type support (15+ types including complex types)

Example:
    >>> from sparkless.sql import SparkSession, functions as F
    >>> spark = SparkSession("test")
    >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    >>> df = spark.createDataFrame(data)
    >>> df.select("name", "age").filter(F.col("age") > 25).show()
    +----+---+
    |name|age|
    +----+---+
    | Bob| 30|
    +----+---+
"""

from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING, Tuple, Union, cast

from .protocols import SupportsDataFrameOps

# Type checking: Explicitly declare DataFrame implements SupportsDataFrameOps
# This helps mypy understand the type relationship
if TYPE_CHECKING:
    # Type stub to help mypy understand DataFrame satisfies SupportsDataFrameOps
    def _type_check_dataframe_implements_protocol() -> None:
        """Type checking helper - DataFrame implements SupportsDataFrameOps."""
        pass  # This function is only for type checking, not runtime


if TYPE_CHECKING:
    from .collection_handler import CollectionHandler
    from .condition_handler import ConditionHandler
    from .validation_handler import ValidationHandler
    from .window_handler import WindowFunctionHandler
    from ..core.protocols import ColumnExpression

    def _ensure_dataframe_protocol(df: "DataFrame") -> "SupportsDataFrameOps":
        return cast("SupportsDataFrameOps", df)


if TYPE_CHECKING:
    from .lazy import LazyEvaluationEngine
    from ..backend.protocols import StorageBackend
    from .grouped import GroupedData
else:
    StorageBackend = Any

from ..spark_types import (
    StructType,
    Row,
    StringType,
    LongType,
    DoubleType,
    DataType,
    IntegerType,
    ArrayType,
    MapType,
    TimestampType,
    DateType,
)
from ..functions import Column, ColumnOperation
from ..functions.core.literals import Literal
from ..storage import MemoryStorageManager
from .rdd import MockRDD
from .writer import DataFrameWriter
from .evaluation.expression_evaluator import ExpressionEvaluator
from .attribute_handler import DataFrameAttributeHandler


class DataFrame:
    """Mock DataFrame implementation with complete PySpark API compatibility.

    Provides a comprehensive mock implementation of PySpark DataFrame that supports
    all major operations including selection, filtering, grouping, joining, and
    window functions. Designed for testing and development without requiring JVM.

    Attributes:
        data: List of dictionaries representing DataFrame rows.
        schema: StructType defining the DataFrame schema.
        storage: Optional storage manager for persistence operations.

    Example:
        >>> from sparkless.sql import SparkSession, functions as F
        >>> spark = SparkSession("test")
        >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
        >>> df = spark.createDataFrame(data)
        >>> df.select("name").filter(F.col("age") > 25).show()
        +----+
        |name|
        +----+
        | Bob|
        +----+
    """

    data: List[Dict[str, Any]]
    _schema: StructType
    storage: StorageBackend
    _operations_queue: List[Tuple[str, Any]]
    _cached_count: Optional[int]
    _watermark_col: Optional[str]
    _watermark_delay: Optional[str]

    def __init__(
        self,
        data: List[Dict[str, Any]],
        schema: StructType,
        storage: Optional["StorageBackend"] = None,
        operations: Optional[List[Tuple[str, Any]]] = None,
    ):
        """Initialize DataFrame.

        Args:
            data: List of dictionaries representing DataFrame rows.
            schema: StructType defining the DataFrame schema.
            storage: Optional storage manager for persistence operations.
                    Defaults to a new MemoryStorageManager instance.
            operations: Optional list of queued operations as (operation_name, payload) tuples.
        """
        self.data = data
        self._schema = schema
        self.storage: StorageBackend = storage or MemoryStorageManager()
        self._cached_count: Optional[int] = None
        self._operations_queue: List[Tuple[str, Any]] = operations or []

        # Materialization tracking for column availability
        # For DataFrames created from data, all columns are immediately materialized
        self._materialized: bool = bool(data)
        self._materialized_columns: Set[str] = set()
        if data:
            # All columns from schema are materialized when DataFrame is created from data
            self._materialized_columns = set(self._schema.fieldNames())

        # Caching state tracking for PySpark compatibility
        # When a DataFrame is cached, string concatenation with + operator returns None
        self._is_cached: bool = False

        # Initialize service classes for composition-based architecture
        from .services import (
            TransformationService,
            JoinService,
            AggregationService,
            DisplayService,
            SchemaService,
            AssertionService,
            MiscService,
        )

        self._transformations = TransformationService(self)
        self._joins = JoinService(self)
        self._aggregations = AggregationService(self)
        self._display = DisplayService(self)
        self._schema_ops = SchemaService(self)
        self._assertions = AssertionService(self)
        self._misc = MiscService(self)
        self._lazy_engine: Optional[LazyEvaluationEngine] = None
        self._expression_evaluator: Optional[ExpressionEvaluator] = None
        self._window_handler: Optional[WindowFunctionHandler] = None
        self._collection_handler: Optional[CollectionHandler] = None
        self._validation_handler: Optional[ValidationHandler] = None
        self._condition_handler: Optional[ConditionHandler] = None
        # Observations for metrics tracking (PySpark 3.3+)
        self._observations: Dict[str, Tuple[Column, ...]] = {}

    def _get_lazy_engine(self) -> "LazyEvaluationEngine":
        """Get or create the lazy evaluation engine."""
        if self._lazy_engine is None:
            from .lazy import LazyEvaluationEngine

            self._lazy_engine = LazyEvaluationEngine()
        return self._lazy_engine

    def _get_window_handler(self) -> "WindowFunctionHandler":
        """Get or create the window function handler."""
        if self._window_handler is None:
            from .window_handler import WindowFunctionHandler

            self._window_handler = WindowFunctionHandler(self)
        return self._window_handler

    def _get_collection_handler(self) -> "CollectionHandler":
        """Get or create the collection handler."""
        if self._collection_handler is None:
            from .collection_handler import CollectionHandler

            self._collection_handler = CollectionHandler()
        return self._collection_handler

    def _get_validation_handler(self) -> "ValidationHandler":
        """Get or create the validation handler."""
        if self._validation_handler is None:
            from .validation_handler import ValidationHandler

            self._validation_handler = ValidationHandler(self)
        return self._validation_handler

    def _get_condition_handler(self) -> "ConditionHandler":
        """Get or create the condition handler."""
        if self._condition_handler is None:
            from .condition_handler import ConditionHandler

            self._condition_handler = ConditionHandler(dataframe_context=self)
        return self._condition_handler

    def _get_expression_evaluator(self) -> ExpressionEvaluator:
        """Get or create the expression evaluator."""
        if self._expression_evaluator is None:
            self._expression_evaluator = ExpressionEvaluator(dataframe_context=self)
        elif self._expression_evaluator._dataframe_context is not self:
            # Update context if DataFrame changed
            self._expression_evaluator._dataframe_context = self
        return self._expression_evaluator

    def _mark_materialized(self) -> None:
        """Mark DataFrame as materialized."""
        self._materialized = True
        # Update materialized columns from current schema
        self._materialized_columns = set(self._schema.fieldNames())

    def _get_available_columns(self) -> List[str]:
        """Get columns that are actually available (materialized).

        For validation purposes, only return columns that have been materialized.

        Returns:
            List of column names that are materialized and available.
        """
        if self._materialized:
            return list(self._materialized_columns)
        # Return only columns from base schema (not from transforms)
        return [f.name for f in self._schema.fields]

    def _is_case_sensitive(self) -> bool:
        """Get case sensitivity setting from session.

        Returns:
            True if case-sensitive mode is enabled, False otherwise.
            Defaults to False (case-insensitive) to match PySpark behavior.
        """
        try:
            # Try to get session from active sessions
            from sparkless.session.core.session import SparkSession

            active_sessions = getattr(SparkSession, "_active_sessions", [])
            if active_sessions:
                # Use the most recent active session
                session = active_sessions[-1]
                if hasattr(session, "conf"):
                    return bool(session.conf.is_case_sensitive())
        except Exception:
            # If we can't get session, default to case-insensitive
            pass
        return False  # Default to case-insensitive (matching PySpark)

    # ============================================================================
    # Delegation methods for service classes
    # ============================================================================

    # Transformation operations
    def select(
        self, *columns: Union[str, Column, Literal, Any]
    ) -> "SupportsDataFrameOps":
        """Select columns from the DataFrame."""
        return self._transformations.select(*columns)

    def selectExpr(self, *exprs: str) -> "SupportsDataFrameOps":
        """Select columns or expressions using SQL-like syntax."""
        return self._transformations.selectExpr(*exprs)

    def filter(
        self, condition: Union[ColumnOperation, Column, "Literal", str]
    ) -> "SupportsDataFrameOps":
        """Filter rows based on condition.

        Args:
            condition: Filter condition. Can be:
                - ColumnOperation or Column (e.g., df.salary > 55000)
                - String SQL expression (e.g., "salary > 55000")
                - Literal boolean value
        """
        return self._transformations.filter(condition)

    def where(
        self, condition: Union[ColumnOperation, Column, str]
    ) -> "SupportsDataFrameOps":
        """Alias for filter() - Filter rows based on condition.

        Args:
            condition: Filter condition. Can be:
                - ColumnOperation or Column (e.g., df.salary > 55000)
                - String SQL expression (e.g., "salary > 55000")
        """
        return self._transformations.where(condition)

    def withColumn(
        self, col_name: str, col: Union[Column, ColumnOperation, Literal, Any]
    ) -> "SupportsDataFrameOps":
        """Add or replace column."""
        return self._transformations.withColumn(col_name, col)

    def withColumns(
        self, colsMap: Dict[str, Union[Column, ColumnOperation, Literal, Any]]
    ) -> "SupportsDataFrameOps":
        """Add or replace multiple columns at once."""
        return self._transformations.withColumns(colsMap)

    def withColumnRenamed(self, existing: str, new: str) -> "SupportsDataFrameOps":
        """Rename a column."""
        return self._transformations.withColumnRenamed(existing, new)

    def withColumnsRenamed(self, colsMap: Dict[str, str]) -> "SupportsDataFrameOps":
        """Rename multiple columns."""
        return self._transformations.withColumnsRenamed(colsMap)

    def drop(self, *cols: str) -> "SupportsDataFrameOps":
        """Drop columns."""
        return self._transformations.drop(*cols)

    def distinct(self) -> "SupportsDataFrameOps":
        """Return distinct rows."""
        return self._transformations.distinct()

    def dropDuplicates(
        self, subset: Optional[List[str]] = None
    ) -> "SupportsDataFrameOps":
        """Drop duplicate rows."""
        return self._transformations.dropDuplicates(subset)

    def drop_duplicates(
        self, subset: Optional[List[str]] = None
    ) -> "SupportsDataFrameOps":
        """Alias for dropDuplicates()."""
        return self._transformations.drop_duplicates(subset)

    def orderBy(
        self, *columns: Union[str, Column], ascending: bool = True
    ) -> "SupportsDataFrameOps":
        """Order by columns.

        Args:
            *columns: Column names or Column objects to order by
            ascending: Whether to sort in ascending order (default: True)

        Returns:
            DataFrame sorted by the specified columns
        """
        return self._transformations.orderBy(*columns, ascending=ascending)

    def sort(
        self, *columns: Union[str, Column], **kwargs: Any
    ) -> "SupportsDataFrameOps":
        """Alias for orderBy() - Sort DataFrame by columns."""
        return self._transformations.sort(*columns, **kwargs)

    def limit(self, n: int) -> "SupportsDataFrameOps":
        """Limit number of rows."""
        return self._transformations.limit(n)

    def offset(self, n: int) -> "SupportsDataFrameOps":
        """Skip first n rows (SQL OFFSET clause)."""
        return self._transformations.offset(n)

    def repartition(self, numPartitions: int, *cols: Any) -> "SupportsDataFrameOps":
        """Repartition DataFrame (no-op in mock; returns self)."""
        return self._transformations.repartition(numPartitions, *cols)

    def coalesce(self, numPartitions: int) -> "SupportsDataFrameOps":
        """Coalesce partitions (no-op in mock; returns self)."""
        return self._transformations.coalesce(numPartitions)

    def replace(
        self,
        to_replace: Union[int, float, str, List[Any], Dict[Any, Any]],
        value: Optional[Union[int, float, str, List[Any]]] = None,
        subset: Optional[List[str]] = None,
    ) -> "SupportsDataFrameOps":
        """Replace values in DataFrame."""
        return self._transformations.replace(to_replace, value, subset)

    def colRegex(self, colName: str) -> Column:
        """Select columns matching a regex pattern."""
        return self._transformations.colRegex(colName)

    # Join operations
    def join(
        self,
        other: SupportsDataFrameOps,
        on: Union[str, List[str], "ColumnOperation"],
        how: str = "inner",
    ) -> "SupportsDataFrameOps":
        """Join with another DataFrame."""
        return self._joins.join(other, on, how)

    def crossJoin(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Cross join (Cartesian product) with another DataFrame."""
        return self._joins.crossJoin(other)

    def union(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Union with another DataFrame."""
        return self._joins.union(other)

    def unionByName(
        self,
        other: SupportsDataFrameOps,
        allowMissingColumns: bool = False,
    ) -> "SupportsDataFrameOps":
        """Union with another DataFrame by column names."""
        return self._joins.unionByName(other, allowMissingColumns)

    def unionAll(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Deprecated alias for union() - Use union() instead."""
        return self._joins.unionAll(other)

    def intersect(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Intersect with another DataFrame."""
        return self._joins.intersect(other)

    def intersectAll(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Return intersection with duplicates."""
        return self._joins.intersectAll(other)

    def exceptAll(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Except all with another DataFrame."""
        return self._joins.exceptAll(other)

    def subtract(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Return rows in this DataFrame but not in another."""
        return self._joins.subtract(other)

    # Aggregation operations
    def groupBy(self, *columns: Union[str, Column]) -> "GroupedData":
        """Group DataFrame by columns for aggregation operations."""
        return self._aggregations.groupBy(*columns)

    def groupby(self, *cols: Union[str, Column], **kwargs: Any) -> "GroupedData":
        """Lowercase alias for groupBy()."""
        return self._aggregations.groupby(*cols, **kwargs)

    def rollup(self, *columns: Union[str, Column]) -> Any:
        """Create rollup grouped data for hierarchical grouping."""
        return self._aggregations.rollup(*columns)

    def cube(self, *columns: Union[str, Column]) -> Any:
        """Create cube grouped data for multi-dimensional grouping."""
        return self._aggregations.cube(*columns)

    def agg(
        self, *exprs: Union[str, Column, ColumnOperation, Dict[str, str]]
    ) -> "SupportsDataFrameOps":
        """Aggregate DataFrame without grouping (global aggregation)."""
        return self._aggregations.agg(*exprs)

    # Display operations
    def show(self, n: int = 20, truncate: bool = True) -> None:
        """Display DataFrame content in a clean table format."""
        # Materialize if needed (this updates the schema and data)
        if self._operations_queue:
            materialized = self._materialize_if_lazy()
            # Update schema and data from materialized DataFrame
            self._schema = materialized.schema
            self.data = materialized.data
            self._operations_queue = []
        # Mark as materialized after show (schema and data are now updated)
        self._mark_materialized()
        self._display.show(n, truncate)

    def _to_markdown(
        self,
        n: int = 20,
        truncate: bool = True,
        underline_headers: bool = True,
    ) -> str:
        """Return DataFrame as a markdown table string."""
        return self._display.to_markdown(n, truncate, underline_headers)

    def printSchema(self) -> None:
        """Print DataFrame schema."""
        self._display.printSchema()

    def collect(self) -> List["Row"]:
        """Collect all data as list of Row objects."""
        # Materialize if needed (this updates the schema and data)
        if self._operations_queue:
            materialized = self._materialize_if_lazy()
            # Update schema and data from materialized DataFrame
            self._schema = materialized.schema
            self.data = materialized.data
            self._operations_queue = []
        # Mark as materialized after collection (schema and data are now updated)
        self._mark_materialized()
        return self._display.collect()

    def take(self, n: int) -> List["Row"]:
        """Take first n rows as list of Row objects."""
        self._mark_materialized()
        return self._display.take(n)

    def head(self, n: Optional[int] = None) -> Union["Row", List["Row"]]:
        """
        Return first n rows.

        PySpark behavior:
        - head() (no args) returns a single Row
        - head(1) or head(n) returns a list of Rows
        """
        if n is None:
            # No argument provided - return single Row (PySpark behavior)
            result = self._display.head(1)
            # _display.head(1) always returns List[Row]
            if isinstance(result, list):
                return cast("Row", result[0] if result else None)
            return cast("Row", result)  # type: ignore[unreachable]

        # Explicit n provided - return list (PySpark behavior)
        result = self._display.head(n)
        # _display.head(n) always returns List[Row]
        if isinstance(result, list):
            return result
        # Wrap single Row in list (shouldn't happen, but defensive)
        return [cast("Row", result)]  # type: ignore[unreachable]

    def first(self) -> Union["Row", None]:
        """Return the first row, or None if the DataFrame is empty.

        This method matches PySpark's DataFrame.first() behavior:
        - Returns a single Row object (not a list like head())
        - Returns None if the DataFrame is empty

        Returns:
            First Row, or None if DataFrame is empty.

        Example:
            >>> df = spark.createDataFrame([{"name": "Alice"}, {"name": "Bob"}])
            >>> df.first()
            Row(name='Alice')
            >>> empty_df.first()
            None
        """
        return self._display.first()

    def tail(self, n: int = 1) -> List["Row"]:
        """Return last n rows. Always returns a list, matching PySpark behavior."""
        result = self._display.tail(n)
        # Ensure we always return a list, even if n=1 (PySpark behavior)
        if result is None:
            return []
        if isinstance(result, list):
            return result
        return [result]

    def toPandas(self) -> Any:
        """Convert to pandas DataFrame."""
        return self._display.toPandas()

    def toJSON(self) -> "SupportsDataFrameOps":
        """Return a single-column DataFrame of JSON strings."""
        return self._display.toJSON()

    def count(self) -> int:
        """Count number of rows."""
        # Materialize if needed
        if self._operations_queue:
            materialized = self._materialize_if_lazy()
            # Update schema and data from materialized DataFrame
            self._schema = materialized.schema
            self.data = materialized.data
            self._operations_queue = []
            # Preserve cached state
            self._is_cached = getattr(materialized, "_is_cached", False)
        self._mark_materialized()
        return self._display.count()

    def cache(self) -> "SupportsDataFrameOps":
        """Cache the DataFrame for reuse.

        In PySpark, caching a DataFrame that uses string concatenation with +
        operator causes the concatenation to return None/null values.
        This method marks the DataFrame as cached to match that behavior.

        Returns:
            Self (cached DataFrame).
        """
        # Mark as cached BEFORE materialization so post-processing can detect it
        self._is_cached = True

        # Materialize if needed before caching
        if self._operations_queue:
            materialized = self._materialize_if_lazy()
            # Update self with materialized data
            self.data = materialized.data
            self._schema = materialized.schema
            self._operations_queue = []
            # Ensure cached state is preserved
            self._is_cached = True

        self._mark_materialized()
        return cast("SupportsDataFrameOps", self)

    def persist(self, storageLevel: Any = None) -> "SupportsDataFrameOps":
        """Persist the DataFrame with the given storage level.

        Args:
            storageLevel: Storage level (ignored in mock, but kept for API compatibility).

        Returns:
            Self (persisted DataFrame).
        """
        # Same behavior as cache for mock implementation
        return self.cache()

    def isEmpty(self) -> bool:
        """Check if DataFrame is empty."""
        return self._display.isEmpty()

    # Assertion operations
    def _assert_has_columns(self, expected_columns: List[str]) -> None:
        """Assert that DataFrame has the expected columns."""
        self._assertions.assert_has_columns(expected_columns)

    def _assert_row_count(self, expected_count: int) -> None:
        """Assert that DataFrame has the expected row count."""
        self._assertions.assert_row_count(expected_count)

    def _assert_schema_matches(self, expected_schema: StructType) -> None:
        """Assert that DataFrame schema matches the expected schema."""
        self._assertions.assert_schema_matches(expected_schema)

    def _assert_data_equals(self, expected_data: List[Dict[str, Any]]) -> None:
        """Assert that DataFrame data equals the expected data."""
        self._assertions.assert_data_equals(expected_data)

    # Miscellaneous operations
    def dropna(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[List[str]] = None,
    ) -> "SupportsDataFrameOps":
        """Drop rows with null values."""
        return self._misc.dropna(how, thresh, subset)

    def fillna(
        self,
        value: Union[Any, Dict[str, Any]],
        subset: Optional[Union[str, List[str], Tuple[str, ...]]] = None,
    ) -> "SupportsDataFrameOps":
        """Fill null values.

        Args:
            value: Value to fill nulls with. Can be a single value or a dict mapping
                   column names to fill values.
            subset: Optional column name(s) to limit fillna operation to. Can be a
                    string (single column), list, or tuple of column names. If value
                    is a dict, subset is ignored.

        Returns:
            DataFrame with null values filled.

        Raises:
            ColumnNotFoundException: If any column in subset doesn't exist.
        """
        return self._misc.fillna(value, subset)

    @property
    def na(self) -> Any:
        """Access null-handling methods via .na namespace.

        Provides PySpark-compatible API for handling null values in DataFrame.

        Returns:
            NAHandler instance for null-handling operations.

        Example:
            >>> df.na.fill(0)  # Fill all nulls with 0
            >>> df.na.fill({"col1": 0, "col2": "default"})  # Fill with dict
        """
        from .attribute_handler import NAHandler

        return NAHandler(self)

    def sample(
        self,
        fraction: float,
        seed: Optional[int] = None,
        withReplacement: bool = False,
    ) -> "SupportsDataFrameOps":
        """Sample rows from DataFrame."""
        return self._misc.sample(fraction, seed, withReplacement)

    def sampleBy(
        self,
        col: str,
        fractions: Dict[Any, float],
        seed: Optional[int] = None,
    ) -> "SupportsDataFrameOps":
        """Stratified sampling."""
        return self._misc.sampleBy(col, fractions, seed)

    def randomSplit(
        self, weights: List[float], seed: Optional[int] = None
    ) -> List["SupportsDataFrameOps"]:
        """Randomly split DataFrame into multiple DataFrames."""
        return self._misc.randomSplit(weights, seed)

    def describe(self, *cols: str) -> "SupportsDataFrameOps":
        """Compute basic statistics for numeric columns."""
        return self._misc.describe(*cols)

    def summary(self, *stats: str) -> "SupportsDataFrameOps":
        """Compute extended statistics for numeric columns."""
        return self._misc.summary(*stats)

    def crosstab(self, col1: str, col2: str) -> "SupportsDataFrameOps":
        """Calculate cross-tabulation."""
        return self._misc.crosstab(col1, col2)

    def freqItems(
        self, cols: List[str], support: Optional[float] = None
    ) -> "SupportsDataFrameOps":
        """Find frequent items."""
        return self._misc.freqItems(cols, support)

    def approxQuantile(
        self,
        col: Union[str, List[str]],
        probabilities: List[float],
        relativeError: float,
    ) -> Union[List[float], List[List[float]]]:
        """Calculate approximate quantiles."""
        return self._misc.approxQuantile(col, probabilities, relativeError)

    def cov(self, col1: str, col2: str) -> float:
        """Calculate covariance between two columns."""
        return self._misc.cov(col1, col2)

    def transform(self, func: Any) -> "SupportsDataFrameOps":
        """Apply a function to transform a DataFrame."""
        return self._misc.transform(func)

    def mapPartitions(
        self, func: Any, preservesPartitioning: bool = False
    ) -> "SupportsDataFrameOps":
        """Apply a function to each partition of the DataFrame."""
        return self._misc.mapPartitions(func, preservesPartitioning)

    def mapInPandas(self, func: Any, schema: Any) -> "SupportsDataFrameOps":
        """Map an iterator of pandas DataFrames to another iterator of pandas DataFrames."""
        return self._misc.mapInPandas(func, schema)

    def unpivot(
        self,
        ids: Union[str, List[str]],
        values: Union[str, List[str]],
        variableColumnName: str = "variable",
        valueColumnName: str = "value",
    ) -> "SupportsDataFrameOps":
        """Unpivot columns into rows."""
        return self._misc.unpivot(ids, values, variableColumnName, valueColumnName)

    def melt(
        self,
        ids: Optional[List[str]] = None,
        values: Optional[List[str]] = None,
        variableColumnName: str = "variable",
        valueColumnName: str = "value",
    ) -> "SupportsDataFrameOps":
        """Unpivot DataFrame from wide to long format."""
        return self._misc.melt(ids, values, variableColumnName, valueColumnName)

    def explain(
        self,
        extended: bool = False,
        codegen: bool = False,
        cost: bool = False,
        formatted: bool = False,
        mode: Optional[str] = None,
    ) -> Optional[str]:
        """Explain the execution plan."""
        return self._misc.explain(extended, codegen, cost, formatted, mode)

    def toDF(self, *cols: str) -> "SupportsDataFrameOps":
        """Rename columns of DataFrame."""
        return self._misc.toDF(*cols)

    def alias(self, alias: str) -> "SupportsDataFrameOps":
        """Give DataFrame an alias for join operations."""
        return self._misc.alias(alias)

    def hint(self, name: str, *parameters: Any) -> "SupportsDataFrameOps":
        """Provide query optimization hints."""
        return self._misc.hint(name, *parameters)

    def withWatermark(
        self, eventTime: str, delayThreshold: str
    ) -> "SupportsDataFrameOps":
        """Define watermark for streaming."""
        return self._misc.withWatermark(eventTime, delayThreshold)

    def sameSemantics(self, other: "DataFrame") -> bool:
        """Check if this DataFrame has the same semantics as another."""
        return self._misc.sameSemantics(other)

    def semanticHash(self) -> int:
        """Return semantic hash of this DataFrame."""
        return self._misc.semanticHash()

    def inputFiles(self) -> List[str]:
        """Return list of input files for this DataFrame."""
        return self._misc.inputFiles()

    def repartitionByRange(
        self,
        numPartitions: Union[int, str, "Column"],
        *cols: Union[str, "Column"],
    ) -> "SupportsDataFrameOps":
        """Repartition by range of column values."""
        return self._misc.repartitionByRange(numPartitions, *cols)

    def sortWithinPartitions(
        self, *cols: Union[str, "Column"], **kwargs: Any
    ) -> "SupportsDataFrameOps":
        """Sort within partitions."""
        return self._misc.sortWithinPartitions(*cols, **kwargs)

    def toLocalIterator(self, prefetchPartitions: bool = False) -> Any:
        """Return iterator over rows."""
        return self._misc.toLocalIterator(prefetchPartitions)

    def localCheckpoint(self, eager: bool = True) -> "SupportsDataFrameOps":
        """Local checkpoint to truncate lineage."""
        return self._misc.localCheckpoint(eager)

    def isLocal(self) -> bool:
        """Check if running in local mode."""
        return self._misc.isLocal()

    @property
    def isStreaming(self) -> bool:
        """Whether this DataFrame is streaming (always False in mock)."""
        return self._misc.isStreaming

    def foreach(self, f: Any) -> None:
        """Apply function to each row."""
        self._misc.foreach(f)

    def foreachPartition(self, f: Any) -> None:
        """Apply function to each partition."""
        self._misc.foreachPartition(f)

    def writeTo(self, table: str) -> Any:
        """Write DataFrame to a table using the Table API."""
        return self._misc.writeTo(table)

    # Schema properties (from SchemaOperations - properties stay on DataFrame)
    @property
    def columns(self) -> List[str]:
        """Get column names."""
        # Get schema (handles lazy evaluation)
        current_schema = self.schema

        # Ensure schema has fields attribute
        if not hasattr(current_schema, "fields"):
            return []

        # Return column names from schema fields
        # This works even for empty DataFrames with explicit schemas
        return [field.name for field in current_schema.fields]

    @property
    def schema(self) -> StructType:
        """Get DataFrame schema.

        If lazy with queued operations, project the resulting schema without materializing data.
        """
        if self._operations_queue:
            return self._project_schema_with_operations()
        return self._schema

    @schema.setter
    def schema(self, value: StructType) -> None:
        """Set DataFrame schema."""
        self._schema = value

    @property
    def dtypes(self) -> List[Tuple[str, str]]:
        """Get column names and their data types."""
        return [(field.name, field.dataType.typeName()) for field in self.schema.fields]

    def _queue_op(self, op_name: str, payload: Any) -> SupportsDataFrameOps:
        """Queue an operation for lazy evaluation.

        Args:
            op_name: Name of the operation (e.g., "select", "filter", "join").
            payload: Operation-specific payload (columns, condition, etc.).

        Returns:
            New DataFrame with the operation queued.
        """
        new_ops: List[Tuple[str, Any]] = self._operations_queue + [(op_name, payload)]
        return cast(
            "SupportsDataFrameOps",
            DataFrame(
                data=self.data,
                schema=self.schema,
                storage=self.storage,
                operations=new_ops,
            ),
        )

    def _materialize_if_lazy(self) -> SupportsDataFrameOps:
        """Materialize lazy operations if any are queued."""
        if self._operations_queue:
            lazy_engine = self._get_lazy_engine()
            result = cast("SupportsDataFrameOps", lazy_engine.materialize(self))

            return result
        return cast("SupportsDataFrameOps", self)

    def __repr__(self) -> str:
        return f"DataFrame[{len(self.data)} rows, {len(self.schema.fields)} columns]"

    def __getattribute__(self, name: str) -> Any:
        """Custom attribute access for DataFrame."""
        return DataFrameAttributeHandler.handle_getattribute(
            self, name, super().__getattribute__
        )

    def __getitem__(
        self, key: Union[str, int, slice, List[str], Tuple[str, ...]]
    ) -> Union[Column, "SupportsDataFrameOps"]:
        """Access column by name using dictionary-style syntax.

        Args:
            key: Column name (str), column index (int), or slice for multiple columns.

        Returns:
            Column object for single column, or DataFrame for slice.

        Example:
            >>> df["name"]  # Returns Column
            >>> df[["name", "age"]]  # Returns DataFrame with selected columns
        """
        if isinstance(key, str):
            # Single column access - return Column
            from ..functions import Column

            return Column(key)
        elif isinstance(key, (list, tuple)):
            # Multiple column selection - return DataFrame with selected columns
            return self.select(*key)
        elif isinstance(key, slice):
            # Slice selection - return DataFrame with selected columns
            all_cols = [field.name for field in self.schema.fields]
            selected = all_cols[key]
            return self.select(*selected)
        else:
            raise TypeError(
                f"DataFrame indices must be strings, lists, or slices, not {type(key)}"
            )

    def __getattr__(self, name: str) -> Column:
        """Enable df.column_name syntax for column access (PySpark compatibility)."""
        return DataFrameAttributeHandler.handle_getattr(self, name)

    def _validate_operation_types(self, col: "ColumnOperation", operation: str) -> None:
        """Validate type requirements for specific operations.

        Args:
            col: The ColumnOperation to validate
            operation: The operation name for error messages

        Raises:
            TypeError: If operation type requirements are not met
        """
        from sparkless.spark_types import StringType, DateType

        # Check if this is a to_timestamp or to_date operation
        if hasattr(col, "function_name") and col.function_name in (
            "to_timestamp",
            "to_date",
        ):
            # Get the input column from the operation
            input_col = getattr(col, "column", None)
            if input_col is None:
                return

            # Get column name
            col_name = getattr(input_col, "name", None)
            if col_name is None:
                return

            # Look up column type from DataFrame schema
            schema_field = next(
                (f for f in self.schema.fields if f.name == col_name), None
            )
            if schema_field is not None:
                input_type = schema_field.dataType
                func_name = col.function_name

                # Check if there's a cast operation that converts to string
                # For to_timestamp, if the column is cast to string, that's acceptable
                actual_input_type = input_type
                if (
                    hasattr(col, "column")
                    and hasattr(col.column, "operation")
                    and col.column.operation == "cast"
                ):
                    # Check if cast target is string
                    cast_target = col.column.value
                    if (
                        isinstance(cast_target, str)
                        and cast_target.lower() in ["string", "varchar"]
                    ) or (
                        hasattr(cast_target, "__name__")
                        and cast_target.__name__ == "StringType"
                    ):
                        actual_input_type = StringType()

                # Import types needed for validation
                from sparkless.spark_types import (
                    IntegerType,
                    DoubleType,
                )

                # to_timestamp accepts multiple input types (PySpark compatibility):
                # - StringType (with format parameter)
                # - TimestampType (pass-through)
                # - IntegerType/LongType (Unix timestamp in seconds)
                # - DateType (convert Date to Timestamp)
                # - DoubleType (Unix timestamp with decimal seconds)
                if func_name == "to_timestamp":
                    if not isinstance(
                        actual_input_type,
                        (
                            StringType,
                            TimestampType,
                            IntegerType,
                            LongType,
                            DateType,
                            DoubleType,
                        ),
                    ):
                        raise TypeError(
                            f"{func_name}() requires StringType, TimestampType, "
                            f"IntegerType, LongType, DateType, or DoubleType input, "
                            f"got {input_type}."
                        )
                # to_date accepts StringType, TimestampType, or DateType
                elif func_name == "to_date" and not isinstance(
                    input_type, (StringType, TimestampType, DateType)
                ):
                    raise TypeError(
                        f"{func_name}() requires StringType, TimestampType, or DateType input, got {input_type}. "
                        f"Cast the column to string first: F.col('{col_name}').cast('string')"
                    )

    def _validate_column_exists(
        self,
        column_name: str,
        operation: str,
        allow_ambiguous: bool = False,
    ) -> None:
        """Validate that a column exists in the DataFrame and is materialized.

        Raises:
            SparkColumnNotFoundError: If column doesn't exist or isn't materialized
        """
        from ...core.exceptions.operation import SparkColumnNotFoundError

        # Get available (materialized) columns
        available_columns = self._get_available_columns()

        if column_name not in available_columns:
            # Check if column exists in schema (logical plan) but isn't materialized
            schema_columns = [f.name for f in self.schema.fields]
            if column_name in schema_columns:
                # Column exists in logical plan but isn't materialized
                raise SparkColumnNotFoundError(
                    column_name,
                    available_columns,
                    f"Column '{column_name}' exists in logical plan but is not yet materialized. "
                    f"Materialize the DataFrame first using .cache(), .collect(), or by executing an action. "
                    f"Operation: {operation}",
                )
            else:
                # Column doesn't exist at all
                self._get_validation_handler().validate_column_exists(
                    self.schema, column_name, operation
                )

    def _validate_columns_exist(self, column_names: List[str], operation: str) -> None:
        """Validate that multiple columns exist in the DataFrame."""
        self._get_validation_handler().validate_columns_exist(
            self.schema, column_names, operation
        )

    def _validate_filter_expression(
        self,
        condition: "ColumnExpression",
        operation: str,
        has_pending_joins: bool = False,
    ) -> None:
        """Validate filter expression before execution."""
        if not has_pending_joins:
            # Check if there are pending joins (columns might come from other DF)
            has_pending_joins = any(op[0] == "join" for op in self._operations_queue)

        # For lazy DataFrames with operations queued, allow column references from original context
        # PySpark allows filtering on columns from original DataFrame even after select
        in_lazy_context = len(self._operations_queue) > 0

        self._get_validation_handler().validate_filter_expression(
            self.schema, condition, operation, has_pending_joins
        )

        # Also validate expression columns with lazy materialization flag for proper column resolution
        if in_lazy_context:
            self._get_validation_handler().validate_expression_columns(
                self.schema, condition, operation, in_lazy_materialization=True
            )

    def _validate_expression_columns(
        self,
        expression: "ColumnExpression",
        operation: str,
        in_lazy_materialization: bool = False,
    ) -> None:
        """Validate column references in complex expressions."""
        if not in_lazy_materialization:
            # Check if we're in lazy materialization mode by looking at the call stack
            import inspect

            frame = inspect.currentframe()
            try:
                # Walk up the call stack to see if we're in lazy materialization
                # Start from the caller (skip current frame)
                if frame is not None:
                    frame = frame.f_back
                while frame:
                    # Check if we're in _materialize_manual
                    if frame.f_code.co_name == "_materialize_manual":
                        in_lazy_materialization = True
                        break
                    # Also check the filename to be more specific
                    if (
                        hasattr(frame.f_code, "co_filename")
                        and "_materialize_manual" in frame.f_code.co_filename
                        and "_materialize_manual" in str(frame.f_code.co_name)
                    ):
                        # Check if the function name matches (in case of name mangling)
                        in_lazy_materialization = True
                        break
                    frame = frame.f_back
            finally:
                del frame

        # When in lazy materialization, use _schema directly instead of self.schema
        # because self.schema projects the final schema (after all operations including select),
        # but we need the schema at the time the operation was queued (which _materialize_manual sets)
        # We rely on call stack detection to determine if we're in lazy materialization.
        # During normal withColumn calls (not in materialization), we should use self.schema
        # (the projected schema) which includes columns from queued operations.
        validation_schema = self._schema if in_lazy_materialization else self.schema
        self._get_validation_handler().validate_expression_columns(
            validation_schema, expression, operation, in_lazy_materialization
        )

    def _project_schema_with_operations(self) -> StructType:
        """Compute schema after applying queued lazy operations.

        Delegates to SchemaManager for schema projection logic.
        Preserves base schema fields even when data is empty.
        """
        from .schema.schema_manager import SchemaManager

        # Use _schema directly to avoid recursion (schema property calls this method)
        case_sensitive = self._is_case_sensitive()
        return SchemaManager.project_schema_with_operations(
            self._schema, self._operations_queue, case_sensitive
        )

    @property
    def rdd(self) -> "MockRDD":
        """Get RDD representation."""
        return MockRDD(self.data)

    def registerTempTable(self, name: str) -> None:
        """Register as temporary table."""
        # Store in storage
        # Create table with schema first
        self.storage.create_table("default", name, self.schema.fields)
        # Then insert data
        dict_data = [
            row.asDict() if hasattr(row, "asDict") else row for row in self.data
        ]
        self.storage.insert_data("default", name, dict_data)

    def createTempView(self, name: str) -> None:
        """Create temporary view."""
        self.registerTempTable(name)

    def _apply_condition(
        self, data: List[Dict[str, Any]], condition: ColumnOperation
    ) -> List[Dict[str, Any]]:
        """Apply condition to filter data."""
        return self._get_condition_handler().apply_condition(data, condition)

    def _evaluate_condition(
        self, row: Dict[str, Any], condition: Union[ColumnOperation, Column]
    ) -> bool:
        """Evaluate condition for a single row.

        Delegates to ConditionHandler for consistency.
        """
        return self._get_condition_handler().evaluate_condition(row, condition)

    def _evaluate_column_expression(
        self,
        row: Dict[str, Any],
        column_expression: "ColumnExpression",
    ) -> Any:
        """Evaluate a column expression for a single row.

        Args:
            row: Dictionary representing a single row.
            column_expression: Column expression to evaluate (Column, ColumnOperation, or literal).

        Returns:
            Evaluated value of the expression.
        """
        result = self._get_condition_handler().evaluate_column_expression(
            row, column_expression
        )
        return cast("Any", result)

    def _evaluate_window_functions(
        self, data: List[Dict[str, Any]], window_functions: List[Tuple[Any, ...]]
    ) -> List[Dict[str, Any]]:
        """Evaluate window functions across all rows."""
        return self._get_window_handler().evaluate_window_functions(
            data, window_functions
        )

    def _evaluate_lag_lead(
        self, data: List[Dict[str, Any]], window_func: Any, col_name: str, is_lead: bool
    ) -> None:
        """Evaluate lag or lead window function."""
        self._get_window_handler()._evaluate_lag_lead(
            data, window_func, col_name, is_lead
        )

    def _apply_ordering_to_indices(
        self,
        data: List[Dict[str, Any]],
        indices: List[int],
        order_by_cols: List[Union[Column, ColumnOperation]],
    ) -> List[int]:
        """Apply ordering to a list of indices based on order by columns."""
        return self._get_window_handler()._apply_ordering_to_indices(
            data, indices, order_by_cols
        )

    def _apply_lag_lead_to_partition(
        self,
        data: List[Dict[str, Any]],
        indices: List[int],
        source_col: str,
        target_col: str,
        offset: int,
        default_value: Any,
        is_lead: bool,
    ) -> None:
        """Apply lag or lead to a specific partition."""
        self._get_window_handler()._apply_lag_lead_to_partition(
            data, indices, source_col, target_col, offset, default_value, is_lead
        )

    def _evaluate_rank_functions(
        self, data: List[Dict[str, Any]], window_func: Any, col_name: str
    ) -> None:
        """Evaluate rank or dense_rank window function."""
        self._get_window_handler()._evaluate_rank_functions(data, window_func, col_name)

    def _apply_rank_to_partition(
        self,
        data: List[Dict[str, Any]],
        indices: List[int],
        order_by_cols: List[Any],
        col_name: str,
        is_dense: bool,
    ) -> None:
        """Apply rank or dense_rank to a specific partition."""
        self._get_window_handler()._apply_rank_to_partition(
            data, indices, order_by_cols, col_name, is_dense
        )

    def _evaluate_aggregate_window_functions(
        self, data: List[Dict[str, Any]], window_func: Any, col_name: str
    ) -> None:
        """Evaluate aggregate window functions like avg, sum, count, etc."""
        self._get_window_handler()._evaluate_aggregate_window_functions(
            data, window_func, col_name
        )

    def _apply_aggregate_to_partition(
        self,
        data: List[Dict[str, Any]],
        indices: List[int],
        window_func: Any,
        col_name: str,
    ) -> None:
        """Apply aggregate function to a specific partition."""
        self._get_window_handler()._apply_aggregate_to_partition(
            data, indices, window_func, col_name
        )

    def _evaluate_case_when(self, row: Dict[str, Any], case_when_obj: Any) -> Any:
        """Evaluate CASE WHEN expression for a row."""
        return self._get_condition_handler().evaluate_case_when(row, case_when_obj)

    def _evaluate_case_when_condition(
        self, row: Dict[str, Any], condition: Any
    ) -> bool:
        """Evaluate a CASE WHEN condition for a row."""
        return self._get_condition_handler()._evaluate_case_when_condition(
            row, condition
        )

    def createOrReplaceTempView(self, name: str) -> None:
        """Create or replace a temporary view of this DataFrame."""
        # Store the DataFrame as a temporary view in the storage manager
        self.storage.create_temp_view(name, self)

    def createGlobalTempView(self, name: str) -> None:
        """Create a global temporary view (session-independent)."""
        # Use the global_temp schema to mimic Spark's behavior
        if not self.storage.schema_exists("global_temp"):
            self.storage.create_schema("global_temp")
        # Create/overwrite the table in global_temp
        data = self.data
        schema_obj = self.schema
        self.storage.create_table("global_temp", name, schema_obj.fields)
        self.storage.insert_data("global_temp", name, list(data))

    def createOrReplaceGlobalTempView(self, name: str) -> None:
        """Create or replace a global temporary view (all PySpark versions).

        Unlike createGlobalTempView, this method does not raise an error if the view already exists.

        Args:
            name: Name of the global temp view

        Example:
            >>> df.createOrReplaceGlobalTempView("my_global_view")
            >>> spark.sql("SELECT * FROM global_temp.my_global_view")
        """
        # Use the global_temp schema to mimic Spark's behavior
        if not self.storage.schema_exists("global_temp"):
            self.storage.create_schema("global_temp")

        # Check if table exists and drop it first
        if self.storage.table_exists("global_temp", name):
            self.storage.drop_table("global_temp", name)

        # Create the table in global_temp
        data = self.data
        schema_obj = self.schema
        self.storage.create_table("global_temp", name, schema_obj.fields)
        self.storage.insert_data("global_temp", name, list(data))

    def checkpoint(self, eager: bool = False) -> "SupportsDataFrameOps":
        """Checkpoint the DataFrame (no-op in mock; returns self)."""
        return cast("SupportsDataFrameOps", self)

    # Old mixin method implementations removed - now handled by delegation methods above
    # Keeping only DataFrame-specific methods that aren't in mixins below

    def observe(self, name: str, *exprs: "Column") -> "SupportsDataFrameOps":
        """Define observation metrics (PySpark 3.3+).

        Args:
            name: Name of the observation
            *exprs: Column expressions to observe

        Returns:
            Same DataFrame with observation registered

        Example:
            >>> df.observe("metrics", F.count(F.lit(1)).alias("count"))
        """
        # In mock implementation, observations don't affect behavior
        # Preserve existing observations and add new ones
        new_df = DataFrame(
            data=self.data,
            schema=self.schema,
            storage=self.storage,
            operations=self._operations_queue,
        )
        new_df._observations = dict(self._observations)
        new_df._observations[name] = exprs
        return cast("SupportsDataFrameOps", new_df)

    @property
    def write(self) -> "DataFrameWriter":
        """Get DataFrame writer (PySpark-compatible property)."""
        return DataFrameWriter(self, self.storage)

    def _parse_cast_type_string(self, type_str: str) -> DataType:
        """Parse a cast type string to DataType."""
        from ..spark_types import (
            BooleanType,
            TimestampType,
            DecimalType,
        )

        type_str = type_str.strip().lower()

        # Primitive types
        if type_str in ["int", "integer"]:
            return IntegerType()
        elif type_str in ["long", "bigint"]:
            return LongType()
        elif type_str in ["double", "float"]:
            return DoubleType()
        elif type_str in ["string", "varchar"]:
            return StringType()
        elif type_str in ["boolean", "bool"]:
            return BooleanType()
        elif type_str == "date":
            return DateType()
        elif type_str == "timestamp":
            return TimestampType()
        elif type_str.startswith("decimal"):
            import re

            match = re.match(r"decimal\((\d+),(\d+)\)", type_str)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
                return DecimalType(precision, scale)
            return DecimalType(10, 2)
        elif type_str.startswith("array<"):
            element_type_str = type_str[6:-1]
            return ArrayType(self._parse_cast_type_string(element_type_str))
        elif type_str.startswith("map<"):
            types = type_str[4:-1].split(",", 1)
            key_type = self._parse_cast_type_string(types[0].strip())
            value_type = self._parse_cast_type_string(types[1].strip())
            return MapType(key_type, value_type)
        else:
            return StringType()  # Default fallback

    # Old duplicate method implementations removed - now handled by delegation methods above
    # All methods are now delegated to service classes via the delegation methods defined earlier
