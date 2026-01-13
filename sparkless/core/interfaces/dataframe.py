"""
DataFrame interface definitions.

This module defines the abstract interfaces for DataFrame operations,
ensuring consistent behavior across all DataFrame implementations.
"""

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Tuple, Union
from ..types.schema import ISchema


class IDataFrame(ABC):
    """Abstract interface for DataFrame operations."""

    @property
    @abstractmethod
    def schema(self) -> ISchema:
        """Get the DataFrame schema."""
        pass

    @property
    @abstractmethod
    def columns(self) -> List[str]:
        """Get column names."""
        pass

    @abstractmethod
    def select(self, *columns: Union[str, Any]) -> "IDataFrame":
        """Select columns from DataFrame."""
        pass

    @abstractmethod
    def filter(self, condition: Any) -> "IDataFrame":
        """Filter DataFrame rows."""
        pass

    @abstractmethod
    def groupBy(self, *columns: Union[str, Any]) -> "IGroupedData":
        """Group DataFrame by columns."""
        pass

    @abstractmethod
    def orderBy(self, *columns: Union[str, Any]) -> "IDataFrame":
        """Order DataFrame by columns."""
        pass

    @abstractmethod
    def limit(self, n: int) -> "IDataFrame":
        """Limit number of rows."""
        pass

    @abstractmethod
    def collect(self) -> List[Any]:
        """Collect all rows."""
        pass

    @abstractmethod
    def show(self, n: int = 20, truncate: bool = True) -> None:
        """Show DataFrame rows."""
        pass

    @abstractmethod
    def count(self) -> int:
        """Count number of rows."""
        pass

    @abstractmethod
    def distinct(self) -> "IDataFrame":
        """Get distinct rows."""
        pass

    @abstractmethod
    def drop(self, *columns: str) -> "IDataFrame":
        """Drop columns."""
        pass

    @abstractmethod
    def rename(self, *mapping: Tuple[str, str]) -> "IDataFrame":
        """Rename columns."""
        pass

    @abstractmethod
    def withColumn(self, col_name: str, col: Any) -> "IDataFrame":
        """Add or replace column."""
        pass

    @abstractmethod
    def withColumnRenamed(self, existing: str, new: str) -> "IDataFrame":
        """Rename a column."""
        pass

    @abstractmethod
    def join(
        self, other: "IDataFrame", on: Union[str, List[str]], how: str = "inner"
    ) -> "IDataFrame":
        """Join with another DataFrame."""
        pass

    @abstractmethod
    def union(self, other: "IDataFrame") -> "IDataFrame":
        """Union with another DataFrame."""
        pass

    @abstractmethod
    def unionByName(
        self, other: "IDataFrame", allowMissingColumns: bool = False
    ) -> "IDataFrame":
        """Union by column names."""
        pass

    @abstractmethod
    def agg(self, *exprs: Any) -> "IDataFrame":
        """Aggregate DataFrame."""
        pass

    @abstractmethod
    def toPandas(self) -> Any:
        """Convert to Pandas DataFrame."""
        pass

    @abstractmethod
    def printSchema(self) -> None:
        """Print schema."""
        pass

    @abstractmethod
    def explain(self, extended: bool = False) -> None:
        """Explain execution plan."""
        pass

    @property
    @abstractmethod
    def write(self) -> "IDataFrameWriter":
        """Get DataFrame writer."""
        pass

    @property
    @abstractmethod
    def rdd(self) -> "IRDD":
        """Get RDD representation."""
        pass


class IDataFrameWriter(ABC):
    """Abstract interface for DataFrame writing operations."""

    @abstractmethod
    def mode(self, save_mode: str) -> "IDataFrameWriter":
        """Set save mode."""
        pass

    @abstractmethod
    def format(self, source: str) -> "IDataFrameWriter":
        """Set output format."""
        pass

    @abstractmethod
    def option(self, key: str, value: Any) -> "IDataFrameWriter":
        """Set option."""
        pass

    @abstractmethod
    def options(self, **options: Any) -> "IDataFrameWriter":
        """Set multiple options."""
        pass

    @abstractmethod
    def saveAsTable(self, table_name: str) -> None:
        """Save as table."""
        pass

    @abstractmethod
    def save(self, path: str) -> None:
        """Save to path."""
        pass

    @abstractmethod
    def insertInto(self, table_name: str, overwrite: bool = False) -> None:
        """Insert into table."""
        pass


class IDataFrameReader(ABC):
    """Abstract interface for DataFrame reading operations."""

    @abstractmethod
    def format(self, source: str) -> "IDataFrameReader":
        """Set input format."""
        pass

    @abstractmethod
    def option(self, key: str, value: Any) -> "IDataFrameReader":
        """Set option."""
        pass

    @abstractmethod
    def options(self, **options: Any) -> "IDataFrameReader":
        """Set multiple options."""
        pass

    @abstractmethod
    def schema(self, schema: Union[ISchema, str]) -> "IDataFrameReader":
        """Set schema."""
        pass

    @abstractmethod
    def load(
        self, path: Optional[str] = None, format: Optional[str] = None, **options: Any
    ) -> IDataFrame:
        """Load data."""
        pass

    @abstractmethod
    def table(self, table_name: str) -> IDataFrame:
        """Load table."""
        pass

    @abstractmethod
    def json(self, path: str, **options: Any) -> IDataFrame:
        """Load JSON data."""
        pass

    @abstractmethod
    def csv(self, path: str, **options: Any) -> IDataFrame:
        """Load CSV data."""
        pass

    @abstractmethod
    def parquet(self, path: str, **options: Any) -> IDataFrame:
        """Load Parquet data."""
        pass


class IGroupedData(ABC):
    """Abstract interface for grouped data operations."""

    @abstractmethod
    def agg(self, *exprs: Any) -> IDataFrame:
        """Aggregate grouped data."""
        pass

    @abstractmethod
    def count(self) -> IDataFrame:
        """Count grouped data."""
        pass

    @abstractmethod
    def sum(self, *columns: Union[str, Any]) -> IDataFrame:
        """Sum grouped data."""
        pass

    @abstractmethod
    def avg(self, *columns: Union[str, Any]) -> IDataFrame:
        """Average grouped data."""
        pass

    @abstractmethod
    def max(self, *columns: Union[str, Any]) -> IDataFrame:
        """Max grouped data."""
        pass

    @abstractmethod
    def min(self, *columns: Union[str, Any]) -> IDataFrame:
        """Min grouped data."""
        pass


class IRDD(ABC):
    """Abstract interface for RDD operations."""

    @abstractmethod
    def collect(self) -> List[Any]:
        """Collect RDD elements."""
        pass

    @abstractmethod
    def count(self) -> int:
        """Count RDD elements."""
        pass

    @abstractmethod
    def first(self) -> Any:
        """Get first element."""
        pass

    @abstractmethod
    def take(self, n: int) -> List[Any]:
        """Take first n elements."""
        pass

    @abstractmethod
    def map(self, func: Any) -> "IRDD":
        """Map transformation."""
        pass

    @abstractmethod
    def filter(self, func: Any) -> "IRDD":
        """Filter transformation."""
        pass

    @abstractmethod
    def flatMap(self, func: Any) -> "IRDD":
        """FlatMap transformation."""
        pass

    @abstractmethod
    def reduce(self, func: Any) -> Any:
        """Reduce operation."""
        pass

    @abstractmethod
    def foreach(self, func: Any) -> None:
        """Foreach action."""
        pass

    @abstractmethod
    def toDF(self, schema: Optional[Union[ISchema, str]] = None) -> IDataFrame:
        """Convert to DataFrame."""
        pass
