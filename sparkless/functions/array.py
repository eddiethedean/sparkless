"""
Array functions for Sparkless.

This module provides comprehensive array manipulation functions that match PySpark's
array function API. Includes array operations like distinct, intersect, union, except,
and element operations for working with array columns in DataFrames.

Key Features:
    - Complete PySpark array function API compatibility
    - Array set operations (distinct, intersect, union, except)
    - Element operations (position, remove)
    - Type-safe operations with proper return types
    - Support for both column references and array literals

Example:
    >>> from sparkless.sql import SparkSession, functions as F
    >>> spark = SparkSession("test")
    >>> data = [{"tags": ["a", "b", "c", "a"]}, {"tags": ["d", "e", "f"]}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(F.array_distinct(F.col("tags"))).show()
    DataFrame[2 rows, 1 columns]
    array_distinct(tags)
    ['a', 'c', 'b']
    ['e', 'f', 'd']
"""

from typing import Any, Callable, List, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from sparkless.functions.base import AggregateFunction

from sparkless.functions.base import (
    Column,
    ColumnOperation,
    MockLambdaExpression,
)


class ArrayFunctions:
    """Collection of array manipulation functions."""

    @staticmethod
    def array_distinct(column: Union[Column, str]) -> ColumnOperation:
        """Remove duplicate elements from an array.

        Args:
            column: The array column to process.

        Returns:
            ColumnOperation representing the array_distinct function.

        Example:
            >>> df.select(F.array_distinct(F.col("tags")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "array_distinct", name=f"array_distinct({column.name})"
        )

    @staticmethod
    def array_intersect(
        column1: Union[Column, str], column2: Union[Column, str]
    ) -> ColumnOperation:
        """Return the intersection of two arrays.

        Args:
            column1: First array column.
            column2: Second array column.

        Returns:
            ColumnOperation representing the array_intersect function.

        Example:
            >>> df.select(F.array_intersect(F.col("tags1"), F.col("tags2")))
        """
        if isinstance(column1, str):
            column1 = Column(column1)
        if isinstance(column2, str):
            column2 = Column(column2)

        return ColumnOperation(
            column1,
            "array_intersect",
            column2,
            name=f"array_intersect({column1.name}, {column2.name})",
        )

    @staticmethod
    def array_union(
        column1: Union[Column, str], column2: Union[Column, str]
    ) -> ColumnOperation:
        """Return the union of two arrays (with duplicates removed).

        Args:
            column1: First array column.
            column2: Second array column.

        Returns:
            ColumnOperation representing the array_union function.

        Example:
            >>> df.select(F.array_union(F.col("tags1"), F.col("tags2")))
        """
        if isinstance(column1, str):
            column1 = Column(column1)
        if isinstance(column2, str):
            column2 = Column(column2)

        return ColumnOperation(
            column1,
            "array_union",
            column2,
            name=f"array_union({column1.name}, {column2.name})",
        )

    @staticmethod
    def array_except(
        column1: Union[Column, str], column2: Union[Column, str]
    ) -> ColumnOperation:
        """Return elements in first array but not in second.

        Args:
            column1: First array column.
            column2: Second array column.

        Returns:
            ColumnOperation representing the array_except function.

        Example:
            >>> df.select(F.array_except(F.col("tags1"), F.col("tags2")))
        """
        if isinstance(column1, str):
            column1 = Column(column1)
        if isinstance(column2, str):
            column2 = Column(column2)

        return ColumnOperation(
            column1,
            "array_except",
            column2,
            name=f"array_except({column1.name}, {column2.name})",
        )

    @staticmethod
    def array_position(column: Union[Column, str], value: Any) -> ColumnOperation:
        """Return the (1-based) index of the first occurrence of value in the array.

        Args:
            column: The array column.
            value: The value to find.

        Returns:
            ColumnOperation representing the array_position function.

        Example:
            >>> df.select(F.array_position(F.col("tags"), "target"))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column,
            "array_position",
            value,
            name=f"array_position({column.name}, {value!r})",
        )

    @staticmethod
    def array_remove(column: Union[Column, str], value: Any) -> ColumnOperation:
        """Remove all occurrences of a value from the array.

        Args:
            column: The array column.
            value: The value to remove.

        Returns:
            ColumnOperation representing the array_remove function.

        Example:
            >>> df.select(F.array_remove(F.col("tags"), "unwanted"))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column,
            "array_remove",
            value,
            name=f"array_remove({column.name}, {value!r})",
        )

    @staticmethod
    def transform(
        column: Union[Column, str], function: Callable[[Any], Any]
    ) -> ColumnOperation:
        """Apply a function to each element in the array.

        This is a higher-order function that transforms each element of an array
        using the provided lambda function.

        Args:
            column: The array column to transform.
            function: Lambda function to apply to each element.

        Returns:
            ColumnOperation representing the transform function.

        Example:
            >>> df.select(F.transform(F.col("numbers"), lambda x: x * 2))
        """
        if isinstance(column, str):
            column = Column(column)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        return ColumnOperation(
            column,
            "transform",
            lambda_expr,
            name=f"transform({column.name}, <lambda>)",
        )

    @staticmethod
    def filter(
        column: Union[Column, str], function: Callable[[Any], bool]
    ) -> ColumnOperation:
        """Filter array elements based on a predicate function.

        This is a higher-order function that filters array elements using
        the provided lambda function.

        Args:
            column: The array column to filter.
            function: Lambda function that returns True for elements to keep.

        Returns:
            ColumnOperation representing the filter function.

        Example:
            >>> df.select(F.filter(F.col("numbers"), lambda x: x > 10))
        """
        if isinstance(column, str):
            column = Column(column)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        return ColumnOperation(
            column,
            "filter",
            lambda_expr,
            name=f"filter({column.name}, <lambda>)",
        )

    @staticmethod
    def exists(
        column: Union[Column, str], function: Callable[[Any], bool]
    ) -> ColumnOperation:
        """Check if any element in the array satisfies the predicate.

        This is a higher-order function that returns True if at least one
        element matches the condition.

        Args:
            column: The array column to check.
            function: Lambda function predicate.

        Returns:
            ColumnOperation representing the exists function.

        Example:
            >>> df.select(F.exists(F.col("numbers"), lambda x: x > 100))
        """
        if isinstance(column, str):
            column = Column(column)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        return ColumnOperation(
            column,
            "exists",
            lambda_expr,
            name=f"exists({column.name}, <lambda>)",
        )

    @staticmethod
    def forall(
        column: Union[Column, str], function: Callable[[Any], bool]
    ) -> ColumnOperation:
        """Check if all elements in the array satisfy the predicate.

        This is a higher-order function that returns True only if all
        elements match the condition.

        Args:
            column: The array column to check.
            function: Lambda function predicate.

        Returns:
            ColumnOperation representing the forall function.

        Example:
            >>> df.select(F.forall(F.col("numbers"), lambda x: x > 0))
        """
        if isinstance(column, str):
            column = Column(column)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        return ColumnOperation(
            column,
            "forall",
            lambda_expr,
            name=f"forall({column.name}, <lambda>)",
        )

    @staticmethod
    def aggregate(
        column: Union[Column, str],
        initial_value: Any,
        merge: Callable[[Any, Any], Any],
        finish: Optional[Callable[[Any], Any]] = None,
    ) -> ColumnOperation:
        """Reduce array elements to a single value.

        This is a higher-order function that aggregates array elements
        using an accumulator pattern.

        Args:
            column: The array column to aggregate.
            initial_value: Starting value for the accumulator.
            merge: Lambda function (acc, x) -> acc that combines accumulator and element.
            finish: Optional lambda to transform final accumulator value.

        Returns:
            ColumnOperation representing the aggregate function.

        Example:
            >>> df.select(F.aggregate(F.col("nums"), F.lit(0), lambda acc, x: acc + x))
        """
        if isinstance(column, str):
            column = Column(column)

        # Wrap the lambda function
        merge_expr = MockLambdaExpression(merge)

        # Store initial value and lambda data as tuple in value
        lambda_data = {"merge": merge_expr, "finish": finish}
        value_tuple = (initial_value, lambda_data)

        return ColumnOperation(
            column,
            "aggregate",
            value=value_tuple,
            name=f"aggregate({column.name}, <init>, <lambda>)",
        )

    @staticmethod
    def zip_with(
        left: Union[Column, str],
        right: Union[Column, str],
        function: Callable[[Any, Any], Any],
    ) -> ColumnOperation:
        """Merge two arrays element-wise using a function.

        This is a higher-order function that combines elements from two arrays
        using the provided lambda function.

        Args:
            left: First array column.
            right: Second array column.
            function: Lambda function (x, y) -> result for combining elements.

        Returns:
            ColumnOperation representing the zip_with function.

        Example:
            >>> df.select(F.zip_with(F.col("arr1"), F.col("arr2"), lambda x, y: x + y))
        """
        if isinstance(left, str):
            left = Column(left)
        if isinstance(right, str):
            right = Column(right)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        # Store right array and lambda as tuple in value
        value_tuple = (right, lambda_expr)

        return ColumnOperation(
            left,
            "zip_with",
            value=value_tuple,
            name=f"zip_with({left.name}, {right.name}, <lambda>)",
        )

    # Basic Array Functions (PySpark 3.2+)

    @staticmethod
    def array_compact(column: Union[Column, str]) -> ColumnOperation:
        """Remove null values from an array.

        Args:
            column: The array column to compact.

        Returns:
            ColumnOperation representing the array_compact function.

        Example:
            >>> df.select(F.array_compact(F.col("nums")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "array_compact", name=f"array_compact({column.name})"
        )

    @staticmethod
    def slice(column: Union[Column, str], start: int, length: int) -> ColumnOperation:
        """Extract array slice starting at position for given length.

        Args:
            column: The array column.
            start: Starting position (1-based).
            length: Number of elements to extract.

        Returns:
            ColumnOperation representing the slice function.

        Example:
            >>> df.select(F.slice(F.col("nums"), 2, 3))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column,
            "slice",
            (start, length),
            name=f"slice({column.name}, {start}, {length})",
        )

    @staticmethod
    def element_at(column: Union[Column, str], index: int) -> ColumnOperation:
        """Get element at index (1-based, negative for reverse indexing).

        Args:
            column: The array column.
            index: Position to extract (1-based, negative counts from end).

        Returns:
            ColumnOperation representing the element_at function.

        Example:
            >>> df.select(F.element_at(F.col("nums"), 1))  # First element
            >>> df.select(F.element_at(F.col("nums"), -1))  # Last element
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "element_at", index, name=f"element_at({column.name}, {index})"
        )

    @staticmethod
    def array_append(column: Union[Column, str], element: Any) -> ColumnOperation:
        """Append element to end of array.

        Args:
            column: The array column.
            element: Element to append.

        Returns:
            ColumnOperation representing the array_append function.

        Example:
            >>> df.select(F.array_append(F.col("nums"), 10))
        """
        if isinstance(column, str):
            column = Column(column)

        # PySpark's array_append is implemented as array_union(array, array(element))
        return ColumnOperation(
            column,
            "array_append",
            element,
            name=f"array_union({column.name}, array({element}))",
        )

    @staticmethod
    def array_prepend(column: Union[Column, str], element: Any) -> ColumnOperation:
        """Prepend element to start of array.

        Args:
            column: The array column.
            element: Element to prepend.

        Returns:
            ColumnOperation representing the array_prepend function.

        Example:
            >>> df.select(F.array_prepend(F.col("nums"), 0))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column,
            "array_prepend",
            element,
            name=f"array_prepend({column.name}, {element})",
        )

    @staticmethod
    def array_insert(
        column: Union[Column, str], pos: int, value: Any
    ) -> ColumnOperation:
        """Insert element at position in array.

        Args:
            column: The array column.
            pos: Position to insert at (1-based).
            value: Value to insert.

        Returns:
            ColumnOperation representing the array_insert function.

        Example:
            >>> df.select(F.array_insert(F.col("nums"), 2, 99))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column,
            "array_insert",
            (pos, value),
            name=f"array_insert({column.name}, {pos}, {value})",
        )

    @staticmethod
    def array_size(column: Union[Column, str]) -> ColumnOperation:
        """Get array length.

        Args:
            column: The array column.

        Returns:
            ColumnOperation representing the array_size function.

        Example:
            >>> df.select(F.array_size(F.col("nums")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "array_size", name=f"array_size({column.name})")

    @staticmethod
    def array_sort(column: Union[Column, str]) -> ColumnOperation:
        """Sort array elements in ascending order.

        Args:
            column: The array column to sort.

        Returns:
            ColumnOperation representing the array_sort function.

        Example:
            >>> df.select(F.array_sort(F.col("nums")))
        """
        if isinstance(column, str):
            column = Column(column)

        # PySpark includes the lambda function in the column name for array_sort
        # This matches PySpark's behavior exactly
        pyspark_lambda = "lambdafunction((IF(((namedlambdavariable() IS NULL) AND (namedlambdavariable() IS NULL)), 0, (IF((namedlambdavariable() IS NULL), 1, (IF((namedlambdavariable() IS NULL), -1, (IF((namedlambdavariable() < namedlambdavariable()), -1, (IF((namedlambdavariable() > namedlambdavariable()), 1, 0)))))))))), namedlambdavariable(), namedlambdavariable())"
        return ColumnOperation(
            column, "array_sort", name=f"array_sort({column.name}, {pyspark_lambda})"
        )

    @staticmethod
    def array_contains(column: Union[Column, str], value: Any) -> ColumnOperation:
        """Check if array contains a specific value.

        Args:
            column: The array column to search.
            value: The value to search for.

        Returns:
            ColumnOperation representing the array_contains function.

        Example:
            >>> df.select(F.array_contains(F.col("tags"), "spark"))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column,
            "array_contains",
            value=value,
            name=f"array_contains({column.name}, {value})",
        )

    @staticmethod
    def array_max(column: Union[Column, str]) -> ColumnOperation:
        """Return maximum value from array.

        Args:
            column: The array column.

        Returns:
            ColumnOperation representing the array_max function.

        Example:
            >>> df.select(F.array_max(F.col("nums")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "array_max", name=f"array_max({column.name})")

    @staticmethod
    def array_min(column: Union[Column, str]) -> ColumnOperation:
        """Return minimum value from array.

        Args:
            column: The array column.

        Returns:
            ColumnOperation representing the array_min function.

        Example:
            >>> df.select(F.array_min(F.col("nums")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "array_min", name=f"array_min({column.name})")

    @staticmethod
    def explode(column: Union[Column, str]) -> ColumnOperation:
        """Returns a new row for each element in the given array or map.

        Args:
            column: The array or map column.

        Returns:
            ColumnOperation representing the explode function.

        Example:
            >>> df.select(F.explode(F.col("tags")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "explode", name=f"explode({column.name})")

    @staticmethod
    def size(column: Union[Column, str]) -> ColumnOperation:
        """Return the size (length) of an array or map.

        Args:
            column: The array or map column.

        Returns:
            ColumnOperation representing the size function.

        Example:
            >>> df.select(F.size(F.col("tags")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "size", name=f"size({column.name})")

    @staticmethod
    def flatten(column: Union[Column, str]) -> ColumnOperation:
        """Flatten array of arrays into a single array.

        Args:
            column: The array column containing nested arrays.

        Returns:
            ColumnOperation representing the flatten function.

        Example:
            >>> df.select(F.flatten(F.col("nested_arrays")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "flatten", name=f"flatten({column.name})")

    @staticmethod
    def reverse(column: Union[Column, str]) -> ColumnOperation:
        """Reverse the elements of an array.

        Args:
            column: The array column.

        Returns:
            ColumnOperation representing the reverse function.

        Example:
            >>> df.select(F.reverse(F.col("nums")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "reverse", name=f"reverse({column.name})")

    @staticmethod
    def arrays_overlap(
        column1: Union[Column, str], column2: Union[Column, str]
    ) -> ColumnOperation:
        """Check if two arrays have any common elements.

        Args:
            column1: First array column.
            column2: Second array column.

        Returns:
            ColumnOperation representing the arrays_overlap function.

        Example:
            >>> df.select(F.arrays_overlap(F.col("arr1"), F.col("arr2")))
        """
        if isinstance(column1, str):
            column1 = Column(column1)
        if isinstance(column2, str):
            column2 = Column(column2)

        return ColumnOperation(
            column1,
            "arrays_overlap",
            column2,
            name=f"arrays_overlap({column1.name}, {column2.name})",
        )

    @staticmethod
    def explode_outer(column: Union[Column, str]) -> ColumnOperation:
        """Returns a new row for each element, including rows with null/empty arrays.

        Args:
            column: The array or map column.

        Returns:
            ColumnOperation representing the explode_outer function.

        Example:
            >>> df.select(F.explode_outer(F.col("tags")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "explode_outer", name=f"explode_outer({column.name})"
        )

    @staticmethod
    def posexplode(column: Union[Column, str]) -> ColumnOperation:
        """Returns a new row for each element with position in array.

        Args:
            column: The array column.

        Returns:
            ColumnOperation representing the posexplode function.
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "posexplode", name=f"posexplode({column.name})")

    @staticmethod
    def posexplode_outer(column: Union[Column, str]) -> ColumnOperation:
        """Returns a new row for each element with position, including null/empty arrays.

        Args:
            column: The array column.

        Returns:
            ColumnOperation representing the posexplode_outer function.
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "posexplode_outer", name=f"posexplode_outer({column.name})"
        )

    @staticmethod
    def arrays_zip(*columns: Union[Column, str]) -> ColumnOperation:
        """Merge arrays into array of structs (alias for array_zip).

        Args:
            *columns: Array columns to zip together.

        Returns:
            ColumnOperation representing the arrays_zip function.
        """
        cols = []
        for col in columns:
            if isinstance(col, str):
                cols.append(Column(col))
            else:
                cols.append(col)

        # Generate proper name with column names
        if len(cols) == 0:
            name = "arrays_zip()"
        elif len(cols) == 1:
            col_name = cols[0].name if hasattr(cols[0], "name") else str(cols[0])
            name = f"arrays_zip({col_name})"
        else:
            col_names = ", ".join(
                c.name if hasattr(c, "name") else str(c) for c in cols
            )
            name = f"arrays_zip({col_names})"

        return ColumnOperation(
            cols[0] if cols else Column(""),
            "arrays_zip",
            value=cols[1:] if len(cols) > 1 else [],
            name=name,
        )

    @staticmethod
    def sequence(
        start: Union[Column, str, int],
        stop: Union[Column, str, int],
        step: Union[Column, str, int] = 1,
    ) -> ColumnOperation:
        """Generate array of integers from start to stop by step.

        Args:
            start: Starting value
            stop: Ending value
            step: Step size (default 1)

        Returns:
            ColumnOperation representing the sequence function.
        """
        if isinstance(start, str):
            start = Column(start)
        elif isinstance(start, int):
            from sparkless.functions.core.literals import Literal

            start = Literal(start)  # type: ignore[assignment]

        # Generate proper name with literal values
        start_str = (
            str(start.value)
            if hasattr(start, "value") and hasattr(start, "name")
            else (start.name if hasattr(start, "name") else str(start))
        )
        stop_str = (
            str(stop)
            if isinstance(stop, (int, float))
            else (stop.name if hasattr(stop, "name") else str(stop))
        )
        step_str = (
            str(step)
            if isinstance(step, (int, float))
            else (step.name if hasattr(step, "name") else str(step))
        )

        # Only include step if it's not the default value of 1
        if step_str == "1" or (isinstance(step, int) and step == 1):
            name = f"sequence({start_str}, {stop_str})"
        else:
            name = f"sequence({start_str}, {stop_str}, {step_str})"

        return ColumnOperation(
            start,
            "sequence",
            value=(stop, step),
            name=name,
        )

    @staticmethod
    def shuffle(column: Union[Column, str]) -> ColumnOperation:
        """Randomly shuffle array elements.

        Args:
            column: The array column.

        Returns:
            ColumnOperation representing the shuffle function.
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "shuffle", name=f"shuffle({column.name})")

    @staticmethod
    def array(*cols: Union[Column, str, List[Union[Column, str]]]) -> ColumnOperation:
        """Create array from multiple columns (PySpark 3.0+).

        Args:
            *cols: Variable number of columns to combine into array.
                   Supports multiple formats:
                   - F.array("Name", "Type") - string column names
                   - F.array(["Name", "Type"]) - list of string column names
                   - F.array(F.col("Name"), F.col("Type")) - Column objects
                   - F.array([F.col("Name"), F.col("Type")]) - list of Column objects

        Returns:
            ColumnOperation representing the array function.

        Example:
            >>> df.select(F.array(F.col("a"), F.col("b"), F.col("c")))
            >>> df.select(F.array(["a", "b", "c"]))  # List format
        """
        if not cols:
            raise ValueError("array requires at least one column")

        # Handle case where a single list is passed: F.array(["Name", "Type"])
        # Unpack the list if it's the only argument
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = tuple(cols[0])

        # Convert all columns
        converted_cols = []
        for c in cols:
            if isinstance(c, str):
                converted_cols.append(Column(c))
            elif isinstance(c, list):
                # This shouldn't happen after unpacking, but handle it for type safety
                raise ValueError("Nested lists are not supported in array()")
            else:
                # c is a Column object
                converted_cols.append(c)

        # First column is the main column, rest are in value as tuple
        first_col = converted_cols[0]
        rest_cols = tuple(converted_cols[1:]) if len(converted_cols) > 1 else ()

        col_names = ", ".join(
            c.name if hasattr(c, "name") else str(c) for c in converted_cols
        )
        return ColumnOperation(
            first_col,
            "array",
            value=rest_cols if rest_cols else None,
            name=f"array({col_names})",
        )

    @staticmethod
    def array_repeat(col: Union[Column, str], count: int) -> ColumnOperation:
        """Create array by repeating value N times (PySpark 3.0+).

        Args:
            col: Value to repeat
            count: Number of repetitions

        Returns:
            ColumnOperation representing the array_repeat function.

        Example:
            >>> df.select(F.array_repeat(F.col("value"), 3))
        """
        if isinstance(col, str):
            col = Column(col)

        return ColumnOperation(
            col, "array_repeat", value=count, name=f"array_repeat({col.name}, {count})"
        )

    @staticmethod
    def sort_array(col: Union[Column, str], asc: bool = True) -> ColumnOperation:
        """Sort array elements (PySpark 3.0+).

        Args:
            col: Array column to sort
            asc: Sort ascending if True, descending if False

        Returns:
            ColumnOperation representing the sort_array function.

        Example:
            >>> df.select(F.sort_array(F.col("values"), asc=False))
        """
        if isinstance(col, str):
            col = Column(col)

        # Use array_sort as internal function name (reuse existing handlers)
        return ColumnOperation(
            col, "array_sort", value=asc, name=f"sort_array({col.name}, {asc})"
        )

    # Priority 2: Additional Array Functions
    @staticmethod
    def array_agg(col: Union[Column, str]) -> "AggregateFunction":
        """Aggregate function to collect values into an array (PySpark 3.5+).

        Args:
            col: Column to aggregate into an array

        Returns:
            AggregateFunction representing the array_agg function.

        Example:
            >>> df.groupBy("dept").agg(F.array_agg("name"))
        """
        from sparkless.functions.base import AggregateFunction
        from sparkless.spark_types import ArrayType, StringType

        return AggregateFunction(col, "array_agg", ArrayType(StringType()))

    @staticmethod
    def cardinality(col: Union[Column, str]) -> ColumnOperation:
        """Return the size of an array or map (PySpark 3.5+).

        Args:
            col: Array or map column

        Returns:
            ColumnOperation representing the cardinality function.

        Example:
            >>> df.select(F.cardinality(F.col("array_col")))
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "size", name=f"cardinality({column.name})")

    @staticmethod
    def get(
        col: Union[Column, str], key: Union[Column, str, int, Any]
    ) -> ColumnOperation:
        """Get element from array by index or map by key.

        Args:
            col: Array or map column.
            key: Index (for arrays) or key (for maps).

        Returns:
            ColumnOperation representing the get function.
        """
        from sparkless.functions.base import Column

        column = Column(col) if isinstance(col, str) else col
        if isinstance(key, (str, int)):
            key = Column(str(key)) if isinstance(key, int) else Column(key)

        operation = ColumnOperation(
            column,
            "get",
            key,
            name=f"get({column.name}, {key.name if hasattr(key, 'name') else key})",
        )
        return operation

    @staticmethod
    def inline(col: Union[Column, str]) -> ColumnOperation:
        """Explode array of structs into rows.

        Args:
            col: Array of structs column.

        Returns:
            ColumnOperation representing the inline function.
        """
        column = Column(col) if isinstance(col, str) else col
        operation = ColumnOperation(column, "inline", name=f"inline({column.name})")
        return operation

    @staticmethod
    def inline_outer(col: Union[Column, str]) -> ColumnOperation:
        """Explode array of structs into rows (outer join style - preserves nulls).

        Args:
            col: Array of structs column.

        Returns:
            ColumnOperation representing the inline_outer function.
        """
        column = Column(col) if isinstance(col, str) else col
        operation = ColumnOperation(
            column, "inline_outer", name=f"inline_outer({column.name})"
        )
        return operation
