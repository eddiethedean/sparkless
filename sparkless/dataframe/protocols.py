"""
Protocol interfaces for DataFrame handlers.

This module defines type-safe protocols for the specialized handler classes
that implement the Single Responsibility Principle for DataFrame operations.
"""

from __future__ import annotations

from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Protocol,
    TYPE_CHECKING,
    Tuple,
    Union,
)

try:  # Python <3.11 compatibility
    from typing import Self  # type: ignore[attr-defined,unused-ignore]
except (ImportError, AttributeError):  # pragma: no cover - fallback for older versions
    from typing_extensions import Self

if TYPE_CHECKING:
    from ..spark_types import Row, StructType
    from ..core.protocols import ColumnExpression


class HasSchema(Protocol):
    """Objects that expose a schema and can project pending operations."""

    _schema: StructType

    @property
    def schema(self) -> StructType: ...

    @schema.setter
    def schema(self, value: StructType) -> None: ...

    def _project_schema_with_operations(self) -> StructType: ...


class HasData(Protocol):
    """Objects that carry in-memory row data."""

    data: List[Dict[str, Any]]


class HasStorage(Protocol):
    """Objects that reference a storage backend."""

    storage: Any


class OperationQueueAware(Protocol):
    """Objects that queue operations prior to materialisation."""

    _operations_queue: List[Tuple[str, Any]]

    def _queue_op(self, operation: str, payload: Any) -> Self: ...


class LazyMaterializable(Protocol):
    """Objects that can defer execution until explicitly materialised."""

    _cached_count: Optional[int]

    def _materialize_if_lazy(self) -> Self: ...


class ColumnAware(Protocol):
    """Objects that expose column metadata."""

    @property
    def columns(self) -> List[str]: ...


class HandlerProvider(Protocol):
    """Objects that vend auxiliary handlers used by mixins."""

    def _get_collection_handler(self) -> CollectionHandler: ...

    def _get_validation_handler(self) -> ValidationHandler: ...

    def _get_condition_handler(self) -> ConditionHandler: ...

    def _get_lazy_engine(self) -> Any: ...


class CollectionSupport(Protocol):
    """Objects exposing collection-style helpers."""

    def collect(self) -> List[Any]: ...

    def orderBy(self, *columns: ColumnExpression, **kwargs: Any) -> Self: ...

    def count(self) -> int: ...


class LogicalOpSupport(Protocol):
    """Objects that provide logical dataframe operations used by mixins."""

    def select(self, *columns: ColumnExpression, **kwargs: Any) -> Self: ...

    def filter(self, condition: ColumnExpression) -> Self: ...

    def distinct(self) -> Self: ...

    def dropDuplicates(self, subset: Optional[List[str]] = None) -> Self: ...

    def drop_duplicates(self, subset: Optional[List[str]] = None) -> Self: ...

    def union(self, other: Any) -> Self: ...

    def groupBy(self, *columns: ColumnExpression, **kwargs: Any) -> Any: ...


class MutationSupport(Protocol):
    """Objects that provide mutation and advanced DataFrame operations."""

    def withColumn(self, col_name: str, col: ColumnExpression) -> Self: ...

    def withColumns(self, cols_map: Dict[str, Any]) -> Self: ...

    def withColumnRenamed(self, existing: str, new: str) -> Self: ...

    def withColumnsRenamed(self, cols_map: Dict[str, str]) -> Self: ...

    def drop(self, *cols: str) -> Self: ...

    def replace(
        self, to_replace: Any, value: Any = ..., subset: Optional[List[str]] = None
    ) -> Self: ...

    def sample(
        self,
        fraction: float,
        seed: Optional[int] = None,
        withReplacement: bool = False,
    ) -> Self: ...

    def sampleBy(
        self, col: str, fractions: Dict[Any, float], seed: Optional[int] = None
    ) -> Self: ...

    def randomSplit(
        self, weights: List[float], seed: Optional[int] = None
    ) -> List[Self]: ...

    def describe(self, *cols: str) -> Self: ...

    def summary(self, *stats: str) -> Self: ...

    def crosstab(self, col1: str, col2: str) -> Self: ...

    def freqItems(self, cols: List[str], support: Optional[float] = None) -> Self: ...

    def approxQuantile(
        self, col: ColumnExpression, probabilities: List[float], relativeError: float
    ) -> List[float]: ...

    def cov(self, col1: str, col2: str) -> float: ...

    def transform(self, func: Any) -> Self: ...

    def mapPartitions(self, func: Any, preservesPartitioning: bool = False) -> Self: ...

    def mapInPandas(self, func: Any, schema: Any) -> Self: ...

    def unpivot(
        self,
        ids: Union[str, List[str]],
        values: Union[str, List[str]],
        variableColumnName: str = "variable",
        valueColumnName: str = "value",
    ) -> Self: ...

    def melt(
        self,
        ids: Optional[List[str]] = None,
        values: Optional[List[str]] = None,
        variableColumnName: str = "variable",
        valueColumnName: str = "value",
    ) -> Self: ...

    def toDF(self, *cols: str) -> Self: ...

    def alias(self, alias: str) -> Self: ...

    def hint(self, name: str, *parameters: Any) -> Self: ...

    def withWatermark(self, eventTime: str, delayThreshold: str) -> Self: ...

    def toJSON(self) -> Self: ...

    def checkpoint(self, eager: bool = False) -> Self: ...

    def localCheckpoint(self, eager: bool = True) -> Self: ...

    def limit(self, n: int) -> Self: ...

    def offset(self, n: int) -> Self: ...

    def repartition(self, numPartitions: int, *cols: ColumnExpression) -> Self: ...

    def coalesce(self, numPartitions: int) -> Self: ...

    def repartitionByRange(
        self, numPartitions: int, *cols: ColumnExpression
    ) -> Self: ...

    def sortWithinPartitions(self, *cols: ColumnExpression, **kwargs: Any) -> Self: ...

    def toLocalIterator(self, prefetchPartitions: bool = False) -> Iterator[Row]: ...

    def foreach(self, f: Any) -> None: ...

    def foreachPartition(self, f: Any) -> None: ...

    def writeTo(self, table: str) -> Any: ...

    def sameSemantics(self, other: Any) -> bool: ...

    def semanticHash(self) -> int: ...

    def inputFiles(self) -> List[str]: ...

    def isLocal(self) -> bool: ...

    @property
    def isStreaming(self) -> bool: ...


class WatermarkAware(Protocol):
    """Objects that keep watermark metadata for streaming semantics."""

    _watermark_col: Optional[str]
    _watermark_delay: Optional[str]


class SupportsDataFrameOps(
    HasSchema,
    HasData,
    HasStorage,
    OperationQueueAware,
    LazyMaterializable,
    ColumnAware,
    HandlerProvider,
    CollectionSupport,
    LogicalOpSupport,
    MutationSupport,
    WatermarkAware,
    Protocol,
):
    """Composite protocol satisfied by `sparkless.dataframe.DataFrame`."""

    def _validate_expression_columns(
        self,
        expression: ColumnExpression,
        operation: str,
        in_lazy_materialization: bool = False,
    ) -> None: ...

    def _validate_filter_expression(
        self,
        condition: ColumnExpression,
        operation: str,
        has_pending_joins: bool = False,
    ) -> None: ...

    def _validate_column_exists(
        self, column_name: str, operation: str, allow_ambiguous: bool = False
    ) -> None: ...

    def _evaluate_column_expression(
        self,
        row: Dict[str, Any],
        column_expression: ColumnExpression,
    ) -> Any: ...


class WindowFunctionHandler(Protocol):
    """Protocol for window function evaluation."""

    def evaluate_window_functions(
        self, data: List[Dict[str, Any]], window_functions: List[Tuple[Any, ...]]
    ) -> List[Dict[str, Any]]:
        """Evaluate window functions across all rows."""
        ...

    def _evaluate_lag_lead(
        self,
        result_data: List[Dict[str, Any]],
        window_func: Any,
        col_name: str,
        is_lead: bool,
    ) -> None:
        """Evaluate LAG/LEAD functions."""
        ...

    def _apply_ordering_to_indices(
        self, data: List[Dict[str, Any]], indices: List[int], order_by_cols: List[Any]
    ) -> List[int]:
        """Apply ordering to row indices."""
        ...

    def _apply_lag_lead_to_partition(
        self,
        result_data: List[Dict[str, Any]],
        partition_indices: List[int],
        window_func: Any,
        col_name: str,
        is_lead: bool,
    ) -> None:
        """Apply LAG/LEAD to a partition."""
        ...

    def _evaluate_rank_functions(
        self, result_data: List[Dict[str, Any]], window_func: Any, col_name: str
    ) -> None:
        """Evaluate RANK/DENSE_RANK functions."""
        ...

    def _apply_rank_to_partition(
        self,
        result_data: List[Dict[str, Any]],
        partition_indices: List[int],
        window_func: Any,
        col_name: str,
    ) -> None:
        """Apply ranking to a partition."""
        ...

    def _evaluate_aggregate_window_functions(
        self, result_data: List[Dict[str, Any]], window_func: Any, col_name: str
    ) -> None:
        """Evaluate aggregate window functions (SUM, AVG, etc.)."""
        ...

    def _apply_aggregate_to_partition(
        self,
        result_data: List[Dict[str, Any]],
        partition_indices: List[int],
        window_func: Any,
        col_name: str,
    ) -> None:
        """Apply aggregate functions to a partition."""
        ...


class CollectionHandler(Protocol):
    """Protocol for collection operations."""

    def collect(self, data: List[Dict[str, Any]], schema: StructType) -> List[Row]:
        """Convert data to Row objects."""
        ...

    def take(self, data: List[Dict[str, Any]], schema: StructType, n: int) -> List[Row]:
        """Take first n rows."""
        ...

    def head(
        self, data: List[Dict[str, Any]], schema: StructType, n: int = 1
    ) -> List[Any]:
        """Get first row(s)."""
        ...

    def tail(self, data: List[Dict[str, Any]], schema: StructType, n: int = 1) -> Any:
        """Get last n rows."""
        ...

    def to_local_iterator(
        self,
        data: List[Dict[str, Any]],
        schema: StructType,
        prefetch: bool = False,
    ) -> Iterator[Row]:
        """Return iterator over rows."""
        ...


class ValidationHandler(Protocol):
    """Protocol for data validation."""

    def validate_column_exists(
        self, schema: StructType, column_name: str, operation: str
    ) -> None:
        """Validate single column exists."""
        ...

    def validate_columns_exist(
        self, schema: StructType, column_names: List[str], operation: str
    ) -> None:
        """Validate multiple columns exist."""
        ...

    def validate_filter_expression(
        self,
        schema: StructType,
        condition: ColumnExpression,
        operation: str,
        has_pending_joins: bool = False,
    ) -> None:
        """Validate filter expression."""
        ...

    def validate_expression_columns(
        self,
        schema: StructType,
        expression: ColumnExpression,
        operation: str,
        in_lazy_materialization: bool = False,
    ) -> None:
        """Validate columns in expression exist."""
        ...


class ConditionHandler(Protocol):
    """Protocol for condition evaluation."""

    def apply_condition(
        self, data: List[Dict[str, Any]], condition: ColumnExpression
    ) -> List[Dict[str, Any]]:
        """Filter data based on condition."""
        ...

    def evaluate_condition(
        self, row: Dict[str, Any], condition: ColumnExpression
    ) -> bool:
        """Evaluate condition for a single row."""
        ...

    def evaluate_column_expression(
        self, row: Dict[str, Any], column_expression: ColumnExpression
    ) -> Any:
        """Evaluate column expression."""
        ...

    def evaluate_case_when(self, row: Dict[str, Any], case_when_obj: Any) -> Any:
        """Evaluate CASE WHEN expression."""
        ...

    def _evaluate_case_when_condition(
        self, row: Dict[str, Any], condition: ColumnExpression
    ) -> bool:
        """Helper for case when condition evaluation."""
        ...
