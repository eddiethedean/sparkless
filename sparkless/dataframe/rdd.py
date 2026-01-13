"""
Mock RDD implementation for DataFrame compatibility.

This module provides a simplified RDD implementation that maintains
compatibility with PySpark's RDD interface while working with mock data.
"""

from __future__ import annotations

from typing import Iterator
from typing import Any, Callable, Dict, List


class MockRDD:
    """Mock RDD for DataFrame compatibility.

    Provides a simplified implementation of PySpark's RDD interface
    that works with mock data structures.
    """

    def __init__(self, data: List[Dict[str, Any]]):
        """Initialize MockRDD.

        Args:
            data: List of dictionaries representing the RDD data.
        """
        self.data = data

    def collect(self) -> List[Any]:
        """Collect RDD data.

        Returns:
            List of all elements in the RDD.
        """
        return self.data

    def count(self) -> int:
        """Count RDD elements.

        Returns:
            Number of elements in the RDD.
        """
        return len(self.data)

    def take(self, num: int) -> List[Any]:
        """Take the first num elements.

        Args:
            num: Number of elements to take.

        Returns:
            List of first num elements.
        """
        return self.data[:num]

    def first(self) -> Any:
        """Get the first element.

        Returns:
            First element in the RDD, or None if empty.
        """
        return self.data[0] if self.data else None

    def foreach(self, func: Callable[[Any], None]) -> None:
        """Apply function to each element.

        Args:
            func: Function to apply to each element.
        """
        for item in self.data:
            func(item)

    def map(self, func: Callable[[Any], Any]) -> MockRDD:
        """Map function over RDD elements.

        Args:
            func: Function to apply to each element.

        Returns:
            New MockRDD with transformed elements.
        """
        return MockRDD([func(item) for item in self.data])

    def filter(self, func: Callable[[Any], bool]) -> MockRDD:
        """Filter RDD elements.

        Args:
            func: Predicate function to filter elements.

        Returns:
            New MockRDD with filtered elements.
        """
        return MockRDD([item for item in self.data if func(item)])

    def reduce(self, func: Callable[[Any, Any], Any]) -> Any:
        """Reduce RDD elements.

        Args:
            func: Reduction function.

        Returns:
            Reduced value.
        """
        if not self.data:
            from ..core.exceptions import PySparkValueError

            raise PySparkValueError("Cannot reduce empty RDD")

        result = self.data[0]
        for item in self.data[1:]:
            result = func(result, item)
        return result

    def groupBy(self, func: Callable[[Any], Any]) -> MockGroupedRDD:
        """Group RDD elements by key.

        Args:
            func: Function to extract grouping key.

        Returns:
            MockGroupedRDD for grouped operations.
        """
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        for item in self.data:
            key = func(item)
            if key not in groups:
                groups[key] = []
            groups[key].append(item)

        return MockGroupedRDD(groups)

    def toDF(self, schema: Any = None) -> Any:
        """Convert RDD to DataFrame.

        Args:
            schema: Optional schema for the DataFrame.

        Returns:
            DataFrame representation.
        """
        # Import here to avoid circular imports
        from ..dataframe import DataFrame
        from ..spark_types import StructType
        from ..core.schema_inference import SchemaInferenceEngine

        # If schema is None, infer it from data
        if schema is None:
            if not self.data:
                schema = StructType([])
            else:
                schema, _ = SchemaInferenceEngine.infer_from_data(self.data)

        return DataFrame(self.data, schema)

    def cache(self) -> MockRDD:
        """Cache the RDD.

        Returns:
            Self (caching is a no-op in mock implementation).
        """
        return self

    def persist(self, storage_level: Any = None) -> MockRDD:
        """Persist the RDD.

        Args:
            storage_level: Storage level (ignored in mock).

        Returns:
            Self (persistence is a no-op in mock implementation).
        """
        return self

    def unpersist(self) -> MockRDD:
        """Unpersist the RDD.

        Returns:
            Self (unpersistence is a no-op in mock implementation).
        """
        return self

    def __iter__(self) -> Iterator[Any]:
        """Make RDD iterable.

        Returns:
            Iterator over RDD elements.
        """
        return iter(self.data)

    def __len__(self) -> int:
        """Get RDD length.

        Returns:
            Number of elements in the RDD.
        """
        return len(self.data)

    def __repr__(self) -> str:
        """String representation of RDD.

        Returns:
            String representation.
        """
        return f"MockRDD({len(self.data)} elements)"


class MockGroupedRDD:
    """Mock grouped RDD for groupBy operations."""

    def __init__(self, groups: Dict[Any, List[Any]]):
        """Initialize MockGroupedRDD.

        Args:
            groups: Dictionary mapping keys to lists of values.
        """
        self.groups = groups

    def mapValues(self, func: Callable[[List[Any]], Any]) -> MockRDD:
        """Map function over grouped values.

        Args:
            func: Function to apply to each group of values.

        Returns:
            New MockRDD with transformed grouped values.
        """
        result = []
        for key, values in self.groups.items():
            result.append({"key": key, "value": func(values)})
        return MockRDD(result)

    def reduceByKey(self, func: Callable[[Any, Any], Any]) -> MockRDD:
        """Reduce grouped values by key.

        Args:
            func: Reduction function.

        Returns:
            New MockRDD with reduced values.
        """
        result = []
        for key, values in self.groups.items():
            if values:
                reduced = values[0]
                for value in values[1:]:
                    reduced = func(reduced, value)
                result.append({"key": key, "value": reduced})
        return MockRDD(result)

    def countByKey(self) -> Dict[Any, int]:
        """Count values by key.

        Returns:
            Dictionary mapping keys to counts.
        """
        return {key: len(values) for key, values in self.groups.items()}

    def collect(self) -> List[Any]:
        """Collect grouped data.

        Returns:
            List of (key, values) tuples.
        """
        return [(key, values) for key, values in self.groups.items()]
