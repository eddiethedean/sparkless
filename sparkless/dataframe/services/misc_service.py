"""
Miscellaneous service for DataFrame operations.

This service provides various miscellaneous operations using composition instead of mixin inheritance.
"""

from __future__ import annotations
from typing import Iterator
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple, Union, cast

from ...spark_types import (
    ArrayType,
    BooleanType,
    DataType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

if TYPE_CHECKING:
    from ...functions import Column
    from ..dataframe import DataFrame
    from ..protocols import SupportsDataFrameOps

from ...core.exceptions import IllegalArgumentException
from ...core.exceptions.analysis import AnalysisException, ColumnNotFoundException


class MiscService:
    """Service providing miscellaneous operations for DataFrame."""

    def __init__(self, df: DataFrame):
        """Initialize misc service with DataFrame instance."""
        self._df = df

    @staticmethod
    def _is_value_compatible_with_type(value: Any, column_type: DataType) -> bool:
        """Check if a value is compatible with a column's data type.

        PySpark silently ignores type mismatches when filling nulls.
        This method checks if the fill value type matches the column type.

        Args:
            value: The value to check
            column_type: The column's data type

        Returns:
            True if value is compatible with column type, False otherwise
        """
        # None/null values are always compatible (they're what we're filling)
        if value is None:
            return True

        # Check type compatibility based on column type
        if isinstance(column_type, StringType):
            return isinstance(value, str)
        elif isinstance(column_type, (IntegerType, LongType)):
            return isinstance(value, int)
        elif isinstance(column_type, (FloatType, DoubleType)):
            return isinstance(value, (int, float))
        elif isinstance(column_type, BooleanType):
            return isinstance(value, bool)
        elif isinstance(column_type, ArrayType):
            # For arrays, check if value is a list
            return isinstance(value, list)
        else:
            # For other types (MapType, StructType, etc.), be permissive
            # PySpark is generally permissive with complex types
            return True

    # Data Cleaning Operations
    def dropna(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[List[str]] = None,
    ) -> SupportsDataFrameOps:
        """Drop rows with null values."""
        # Resolve subset column names case-insensitively if provided
        resolved_subset = None
        if subset:
            from ...core.column_resolver import ColumnResolver

            available_cols = self._df.columns
            case_sensitive = self._df._is_case_sensitive()
            resolved_subset = []
            for col in subset:
                resolved_col = ColumnResolver.resolve_column_name(
                    col, available_cols, case_sensitive
                )
                if resolved_col is None:
                    from ...core.exceptions.analysis import ColumnNotFoundException

                    raise ColumnNotFoundException(col)
                resolved_subset.append(resolved_col)

        filtered_data = []
        for row in self._df.data:
            if resolved_subset:
                # Check only specified columns
                null_count = sum(1 for col in resolved_subset if row.get(col) is None)
            else:
                # Check all columns
                null_count = sum(1 for v in row.values() if v is None)

            if (
                how == "any"
                and null_count == 0
                or how == "all"
                and null_count < len(row)
                or thresh is not None
                and null_count <= len(row) - thresh
            ):
                filtered_data.append(row)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(filtered_data, self._df.schema, self._df.storage),
        )

    def fillna(
        self,
        value: Union[Any, Dict[str, Any]],
        subset: Optional[Union[str, List[str], Tuple[str, ...]]] = None,
    ) -> SupportsDataFrameOps:
        """Fill null values.

        Materializes lazy DataFrames before processing to ensure all columns
        are present after operations like joins. This prevents missing columns
        from being incorrectly filled as None.

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

        Note:
            This method automatically materializes lazy DataFrames to ensure
            all columns from previous operations (like joins) are available
            before filling null values. This matches PySpark behavior.
        """
        # Normalize subset to a list
        subset_cols: Optional[List[str]] = None
        if subset is not None:
            if isinstance(subset, str):
                subset_cols = [subset]
            elif isinstance(subset, (list, tuple)):
                subset_cols = list(subset)
            else:
                raise IllegalArgumentException(
                    f"subset must be a string, list, or tuple, got {type(subset).__name__}"
                )

        # Validate columns exist if subset is provided and resolve case-insensitively
        if subset_cols is not None:
            available_cols = [field.name for field in self._df.schema.fields]
            from ...core.column_resolver import ColumnResolver

            case_sensitive = self._df._is_case_sensitive()

            resolved_subset = []
            for col in subset_cols:
                resolved_col = ColumnResolver.resolve_column_name(
                    col, available_cols, case_sensitive
                )
                if resolved_col is None:
                    raise ColumnNotFoundException(col)
                resolved_subset.append(resolved_col)
            subset_cols = resolved_subset

        # Materialize lazy operations first to ensure all columns are present
        # This is especially important after joins where columns might be missing
        # until materialization
        if self._df._operations_queue:
            materialized_df = self._df._materialize_if_lazy()
            # Use materialized DataFrame for processing
            df_to_process = materialized_df
        else:
            # Cast to SupportsDataFrameOps for type compatibility
            # (DataFrame implements SupportsDataFrameOps, but mypy needs explicit cast)
            df_to_process = cast("SupportsDataFrameOps", self._df)

        # Get column types for type checking
        column_types = {
            field.name: field.dataType for field in df_to_process.schema.fields
        }

        # Validate dict keys exist before processing (PySpark behavior) and resolve case-insensitively
        if isinstance(value, dict):
            available_cols = [field.name for field in df_to_process.schema.fields]
            from ...core.column_resolver import ColumnResolver

            case_sensitive = df_to_process._is_case_sensitive()  # type: ignore[attr-defined]

            # Resolve all dict keys case-insensitively
            resolved_value_dict = {}
            for col in value:
                resolved_col = ColumnResolver.resolve_column_name(
                    col, available_cols, case_sensitive
                )
                if resolved_col is None:
                    raise ColumnNotFoundException(col)
                resolved_value_dict[resolved_col] = value[col]
            value = resolved_value_dict

        new_data = []
        for row in df_to_process.data:
            new_row = row.copy()
            if isinstance(value, dict):
                # When value is a dict, subset is ignored (PySpark behavior)
                for col, fill_value in value.items():
                    if new_row.get(col) is None:
                        # Check type compatibility (PySpark silently ignores mismatches)
                        col_type = column_types.get(col)
                        if col_type and self._is_value_compatible_with_type(
                            fill_value, col_type
                        ):
                            new_row[col] = fill_value
                        # If not compatible, leave as None (PySpark behavior)
            else:
                # When value is not a dict, use subset if provided
                if subset_cols is not None:
                    # Only fill nulls in specified columns
                    for col in subset_cols:
                        if new_row.get(col) is None:
                            # Check type compatibility (PySpark silently ignores mismatches)
                            col_type = column_types.get(col)
                            if col_type and self._is_value_compatible_with_type(
                                value, col_type
                            ):
                                new_row[col] = value
                            # If not compatible, leave as None (PySpark behavior)
                else:
                    # Fill nulls in all columns
                    # Iterate over all columns in schema to handle joins properly
                    # (after lazy join materialization, None columns might be missing from row dict)
                    for col in column_types:
                        # Check if column exists in row dict
                        if col not in new_row:
                            # Column is missing - assume it's None and fill it
                            # (happens after lazy join materialization where None columns might be omitted)
                            col_type = column_types.get(col)
                            if col_type and self._is_value_compatible_with_type(
                                value, col_type
                            ):
                                new_row[col] = value
                        elif new_row[col] is None:
                            # Column exists and is None - fill it
                            col_type = column_types.get(col)
                            if col_type and self._is_value_compatible_with_type(
                                value, col_type
                            ):
                                new_row[col] = value
                            # If not compatible, leave as None (PySpark behavior)
                        # If column exists and has a non-None value, don't fill it (preserve existing value)
            new_data.append(new_row)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(new_data, df_to_process.schema, df_to_process.storage),
        )

    # Sampling Operations
    def sample(
        self,
        fraction: float,
        seed: Optional[int] = None,
        withReplacement: bool = False,
    ) -> SupportsDataFrameOps:
        """Sample rows from DataFrame.

        Args:
            fraction: Fraction of rows to sample (0.0 to 1.0).
            seed: Random seed for reproducible sampling.
            withReplacement: Whether to sample with replacement.

        Returns:
            New DataFrame with sampled rows.
        """
        import random

        if not withReplacement and not (0.0 <= fraction <= 1.0):
            raise IllegalArgumentException(
                f"Fraction must be between 0.0 and 1.0 when without replacement, got {fraction}"
            )
        if withReplacement and fraction < 0.0:
            raise IllegalArgumentException(
                f"Fraction must be non-negative when with replacement, got {fraction}"
            )

        if seed is not None:
            random.seed(seed)

        if fraction == 0.0:
            from ..dataframe import DataFrame

            return cast(
                "SupportsDataFrameOps",
                DataFrame([], self._df.schema, self._df.storage),
            )
        elif fraction == 1.0:
            from ..dataframe import DataFrame

            return cast(
                "SupportsDataFrameOps",
                DataFrame(self._df.data.copy(), self._df.schema, self._df.storage),
            )

        # Calculate number of rows to sample
        total_rows = len(self._df.data)
        num_rows = int(total_rows * fraction)

        if withReplacement:
            # Sample with replacement
            sampled_indices = [
                random.randint(0, total_rows - 1) for _ in range(num_rows)
            ]
            sampled_data = [self._df.data[i] for i in sampled_indices]
        else:
            # Sample without replacement
            if num_rows > total_rows:
                num_rows = total_rows
            sampled_indices = random.sample(range(total_rows), num_rows)
            sampled_data = [self._df.data[i] for i in sampled_indices]

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(sampled_data, self._df.schema, self._df.storage),
        )

    def sampleBy(
        self,
        col: str,
        fractions: Dict[Any, float],
        seed: Optional[int] = None,
    ) -> SupportsDataFrameOps:
        """Stratified sampling (all PySpark versions).

        Args:
            col: Column to stratify by
            fractions: Dict mapping stratum values to sampling fractions
            seed: Random seed

        Returns:
            Sampled DataFrame
        """
        import random

        if seed is not None:
            random.seed(seed)

        # Resolve column name case-insensitively
        from ...core.column_resolver import ColumnResolver

        available_cols = self._df.columns
        case_sensitive = self._df._is_case_sensitive()
        resolved_col = ColumnResolver.resolve_column_name(
            col, available_cols, case_sensitive
        )
        if resolved_col is None:
            from ...core.exceptions.analysis import ColumnNotFoundException

            raise ColumnNotFoundException(col)

        result_data = []
        for row in self._df.data:
            stratum_value = row.get(resolved_col)
            fraction = fractions.get(stratum_value, 0.0)
            if random.random() < fraction:
                result_data.append(row)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(result_data, self._df.schema, self._df.storage),
        )

    def randomSplit(
        self, weights: List[float], seed: Optional[int] = None
    ) -> List[SupportsDataFrameOps]:
        """Randomly split DataFrame into multiple DataFrames.

        Args:
            weights: List of weights for each split (must sum to 1.0).
            seed: Random seed for reproducible splitting.

        Returns:
            List of DataFrames split according to weights.
        """
        import random

        if not weights or len(weights) < 2:
            raise IllegalArgumentException("Weights must have at least 2 elements")

        if abs(sum(weights) - 1.0) > 1e-6:
            raise IllegalArgumentException(
                f"Weights must sum to 1.0, got {sum(weights)}"
            )

        if any(w < 0 for w in weights):
            raise IllegalArgumentException("All weights must be non-negative")

        if seed is not None:
            random.seed(seed)

        # Create a list of (index, random_value) pairs
        indexed_data = [(i, random.random()) for i in range(len(self._df.data))]

        # Sort by random value to ensure random distribution
        indexed_data.sort(key=lambda x: x[1])

        # Calculate split points
        cumulative_weight = 0.0
        split_points: List[int] = []
        for weight in weights:
            cumulative_weight += weight
            split_points.append(int(len(self._df.data) * cumulative_weight))

        # Create splits
        splits: List[List[Dict[str, Any]]] = []
        start_idx = 0

        for end_idx in split_points:
            split_indices = [idx for idx, _ in indexed_data[start_idx:end_idx]]
            split_data = [self._df.data[idx] for idx in split_indices]
            splits.append(split_data)
            start_idx = end_idx

        from ..dataframe import DataFrame

        return [
            cast(
                "SupportsDataFrameOps",
                DataFrame(data, self._df.schema, self._df.storage),
            )
            for data in splits
        ]

    # Statistics Operations
    def describe(self, *cols: str) -> SupportsDataFrameOps:
        """Compute basic statistics for numeric columns.

        Args:
            *cols: Column names to describe. If empty, describes all numeric columns.

        Returns:
            DataFrame with statistics (count, mean, stddev, min, max).
        """
        import statistics

        # Determine which columns to describe
        if not cols:
            # Describe all numeric columns
            numeric_cols = []
            for field in self._df.schema.fields:
                field_type = field.dataType.typeName()
                if field_type in [
                    "long",
                    "int",
                    "integer",
                    "bigint",
                    "double",
                    "float",
                ]:
                    numeric_cols.append(field.name)
        else:
            numeric_cols = list(cols)
            # Validate that columns exist
            available_cols = [field.name for field in self._df.schema.fields]
            for col in numeric_cols:
                if col not in available_cols:
                    raise ColumnNotFoundException(col)

        if not numeric_cols:
            # No numeric columns found
            from ..dataframe import DataFrame

            return cast(
                "SupportsDataFrameOps",
                DataFrame([], self._df.schema, self._df.storage),
            )

        # Calculate statistics for each column
        result_data = []

        for col in numeric_cols:
            # Extract values for this column
            values = []
            for row in self._df.data:
                value = row.get(col)
                if value is not None and isinstance(value, (int, float)):
                    values.append(value)

            if not values:
                # No valid numeric values
                stats_row = {
                    "summary": col,
                    "count": "0",
                    "mean": "NaN",
                    "stddev": "NaN",
                    "min": "NaN",
                    "max": "NaN",
                }
            else:
                stats_row = {
                    "summary": col,
                    "count": str(len(values)),
                    "mean": str(round(statistics.mean(values), 4)),
                    "stddev": str(
                        round(statistics.stdev(values) if len(values) > 1 else 0.0, 4)
                    ),
                    "min": str(min(values)),
                    "max": str(max(values)),
                }

            result_data.append(stats_row)

        # Create result schema

        result_schema = StructType(
            [
                StructField("summary", StringType()),
                StructField("count", StringType()),
                StructField("mean", StringType()),
                StructField("stddev", StringType()),
                StructField("min", StringType()),
                StructField("max", StringType()),
            ]
        )

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(result_data, result_schema, self._df.storage),
        )

    def summary(self, *stats: str) -> SupportsDataFrameOps:
        """Compute extended statistics for numeric columns.

        Args:
            *stats: Statistics to compute. Default: ["count", "mean", "stddev", "min", "25%", "50%", "75%", "max"].

        Returns:
            DataFrame with extended statistics.
        """
        import statistics

        # Default statistics if none provided
        if not stats:
            stats = ("count", "mean", "stddev", "min", "25%", "50%", "75%", "max")

        # Find numeric columns
        numeric_cols = []
        for field in self._df.schema.fields:
            field_type = field.dataType.typeName()
            if field_type in ["long", "int", "integer", "bigint", "double", "float"]:
                numeric_cols.append(field.name)

        if not numeric_cols:
            # No numeric columns found
            from ..dataframe import DataFrame

            return cast(
                "SupportsDataFrameOps",
                DataFrame([], self._df.schema, self._df.storage),
            )

        # Calculate statistics for each column
        result_data = []

        for col in numeric_cols:
            # Extract values for this column
            values = []
            for row in self._df.data:
                value = row.get(col)
                if value is not None and isinstance(value, (int, float)):
                    values.append(value)

            if not values:
                # No valid numeric values
                stats_row = {"summary": col}
                for stat in stats:
                    stats_row[stat] = "NaN"
            else:
                stats_row = {"summary": col}
                values_sorted = sorted(values)
                n = len(values)

                for stat in stats:
                    if stat == "count":
                        stats_row[stat] = str(n)
                    elif stat == "mean":
                        stats_row[stat] = str(round(statistics.mean(values), 4))
                    elif stat == "stddev":
                        stats_row[stat] = str(
                            round(statistics.stdev(values) if n > 1 else 0.0, 4)
                        )
                    elif stat == "min":
                        stats_row[stat] = str(values_sorted[0])
                    elif stat == "max":
                        stats_row[stat] = str(values_sorted[-1])
                    elif stat == "25%":
                        q1_idx = int(0.25 * (n - 1))
                        stats_row[stat] = str(values_sorted[q1_idx])
                    elif stat == "50%":
                        q2_idx = int(0.5 * (n - 1))
                        stats_row[stat] = str(values_sorted[q2_idx])
                    elif stat == "75%":
                        q3_idx = int(0.75 * (n - 1))
                        stats_row[stat] = str(values_sorted[q3_idx])
                    else:
                        stats_row[stat] = "NaN"

            result_data.append(stats_row)

        # Create result schema

        result_fields = [StructField("summary", StringType())]
        for stat in stats:
            result_fields.append(StructField(stat, StringType()))

        result_schema = StructType(result_fields)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(result_data, result_schema, self._df.storage),
        )

    def crosstab(self, col1: str, col2: str) -> SupportsDataFrameOps:
        """Calculate cross-tabulation (all PySpark versions).

        Args:
            col1: First column name (rows)
            col2: Second column name (columns)

        Returns:
            DataFrame with cross-tabulation
        """
        from collections import defaultdict

        # Resolve column names case-insensitively
        from ...core.column_resolver import ColumnResolver

        available_cols = self._df.columns
        case_sensitive = self._df._is_case_sensitive()
        resolved_col1 = ColumnResolver.resolve_column_name(
            col1, available_cols, case_sensitive
        )
        resolved_col2 = ColumnResolver.resolve_column_name(
            col2, available_cols, case_sensitive
        )
        if resolved_col1 is None:
            from ...core.exceptions.analysis import ColumnNotFoundException

            raise ColumnNotFoundException(col1)
        if resolved_col2 is None:
            from ...core.exceptions.analysis import ColumnNotFoundException

            raise ColumnNotFoundException(col2)

        # Build cross-tab structure
        crosstab_data: Dict[Any, Dict[Any, int]] = defaultdict(lambda: defaultdict(int))
        col2_values = set()

        for row in self._df.data:
            val1 = row.get(resolved_col1)
            val2 = row.get(resolved_col2)
            crosstab_data[val1][val2] += 1
            col2_values.add(val2)

        # Convert to list of dicts
        # Filter out None values before sorting to avoid comparison issues
        col2_sorted = sorted([v for v in col2_values if v is not None])
        result_data = []
        for val1 in sorted([k for k in crosstab_data if k is not None]):
            result_row = {f"{col1}_{col2}": val1}
            for val2 in col2_sorted:
                result_row[str(val2)] = crosstab_data[val1].get(val2, 0)
            result_data.append(result_row)

        # Build schema

        fields = [StructField(f"{col1}_{col2}", StringType())]
        for val2 in col2_sorted:
            fields.append(StructField(str(val2), LongType()))
        result_schema = StructType(fields)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(result_data, result_schema, self._df.storage),
        )

    def freqItems(
        self, cols: List[str], support: Optional[float] = None
    ) -> SupportsDataFrameOps:
        """Find frequent items (all PySpark versions).

        Args:
            cols: List of column names
            support: Minimum support threshold (default 0.01)

        Returns:
            DataFrame with frequent items for each column
        """
        from collections import Counter

        if support is None:
            support = 0.01

        # Resolve column names case-insensitively
        from ...core.column_resolver import ColumnResolver

        available_cols = self._df.columns
        case_sensitive = self._df._is_case_sensitive()
        resolved_cols = []
        for col in cols:
            resolved_col = ColumnResolver.resolve_column_name(
                col, available_cols, case_sensitive
            )
            if resolved_col is None:
                from ...core.exceptions.analysis import ColumnNotFoundException

                raise ColumnNotFoundException(col)
            resolved_cols.append(resolved_col)

        min_count = int(len(self._df.data) * support)
        result_row = {}

        for col, resolved_col in zip(cols, resolved_cols):
            values = [
                row.get(resolved_col)
                for row in self._df.data
                if row.get(resolved_col) is not None
            ]
            counter = Counter(values)
            freq_items = [item for item, count in counter.items() if count >= min_count]
            result_row[f"{col}_freqItems"] = freq_items

        # Build schema

        fields = [
            StructField(f"{col}_freqItems", ArrayType(StringType())) for col in cols
        ]
        result_schema = StructType(fields)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame([result_row], result_schema, self._df.storage),
        )

    def approxQuantile(
        self,
        col: Union[str, List[str]],
        probabilities: List[float],
        relativeError: float,
    ) -> Union[List[float], List[List[float]]]:
        """Calculate approximate quantiles (all PySpark versions).

        Args:
            col: Column name or list of column names
            probabilities: List of quantile probabilities (0.0 to 1.0)
            relativeError: Relative error for approximation (0.0 for exact)

        Returns:
            List of quantile values, or list of lists if multiple columns
        """

        def calc_quantiles(column_name: str) -> List[float]:
            values_list: List[float] = []
            for row in self._df.data:
                val = row.get(column_name)
                if val is not None:
                    values_list.append(float(val))
            if not values_list:
                return [float("nan")] * len(probabilities)
            from sparkless.utils.statistics import percentile

            return [float(percentile(values_list, p * 100)) for p in probabilities]

        if isinstance(col, str):
            return calc_quantiles(col)
        else:
            return [calc_quantiles(c) for c in col]

    def cov(self, col1: str, col2: str) -> float:
        """Calculate covariance between two columns (all PySpark versions).

        Args:
            col1: First column name
            col2: Second column name

        Returns:
            Covariance value
        """
        # Filter rows where both values are not None and extract numeric values
        pairs = [
            (row.get(col1), row.get(col2))
            for row in self._df.data
            if row.get(col1) is not None and row.get(col2) is not None
        ]

        if not pairs:
            return 0.0

        # Extract values, ensuring they're not None
        values1 = [float(p[0]) for p in pairs if p[0] is not None]
        values2 = [float(p[1]) for p in pairs if p[1] is not None]

        from sparkless.utils.statistics import covariance

        return float(covariance(values1, values2))

    # Transformation Operations
    def transform(self, func: Any) -> SupportsDataFrameOps:
        """Apply a function to transform a DataFrame.

        This enables functional programming style transformations on DataFrames.

        Args:
            func: Function that takes a DataFrame and returns a DataFrame.

        Returns:
            DataFrame: The result of applying the function to this DataFrame.

        Example:
            >>> def add_id(df):
            ...     return df.withColumn("id", F.monotonically_increasing_id())
            >>> df.transform(add_id)
        """
        result = func(self._df)
        if not isinstance(result, type(self._df)):
            from ...core.exceptions import PySparkTypeError

            raise PySparkTypeError(
                f"Function must return a DataFrame, got {type(result).__name__}"
            )
        return cast("SupportsDataFrameOps", result)

    def mapPartitions(
        self, func: Any, preservesPartitioning: bool = False
    ) -> SupportsDataFrameOps:
        """Apply a function to each partition of the DataFrame.

        For sparkless, we treat the entire DataFrame as a single partition.
        The function receives an iterator of Row objects and should return
        an iterator of Row objects.

        Args:
            func: A function that takes an iterator of Rows and returns an iterator of Rows.
            preservesPartitioning: Whether the function preserves partitioning (unused in sparkless).

        Returns:
            DataFrame: Result of applying the function.

        Example:
            >>> def add_index(iterator):
            ...     for i, row in enumerate(iterator):
            ...         yield Row(id=row.id, name=row.name, index=i)
            >>> df.mapPartitions(add_index)
        """
        # Materialize if lazy
        materialized = self._df._materialize_if_lazy()

        # Convert data to Row objects
        from ...spark_types import Row

        def row_iterator() -> Iterator[Row]:
            for row_dict in materialized.data:
                yield Row(row_dict)

        # Apply the function
        result_iterator = func(row_iterator())

        # Collect results
        result_data = []
        for result_row in result_iterator:
            if isinstance(result_row, Row):
                result_data.append(result_row.asDict())
            elif isinstance(result_row, dict):
                result_data.append(result_row)
            else:
                # Try to convert to dict
                result_data.append(dict(result_row))

        # Infer schema from result data
        from ...core.schema_inference import infer_schema_from_data

        result_schema = (
            infer_schema_from_data(result_data) if result_data else self._df.schema
        )

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(result_data, result_schema, self._df.storage),
        )

    def mapInPandas(self, func: Any, schema: Any) -> SupportsDataFrameOps:
        """Map an iterator of pandas DataFrames to another iterator of pandas DataFrames.

        For sparkless, we treat the entire DataFrame as a single partition.
        The function receives an iterator yielding pandas DataFrames and should
        return an iterator yielding pandas DataFrames.

        Args:
            func: A function that takes an iterator of pandas DataFrames and returns
                  an iterator of pandas DataFrames.
            schema: The schema of the output DataFrame (StructType or DDL string).

        Returns:
            DataFrame: Result of applying the function.

        Example:
            >>> def multiply_by_two(iterator):
            ...     for pdf in iterator:
            ...         yield pdf * 2
            >>> df.mapInPandas(multiply_by_two, schema="value double")
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for mapInPandas. "
                "Install it with: pip install 'sparkless[pandas]'"
            )

        # Materialize if lazy
        materialized = self._df._materialize_if_lazy()

        # Convert to pandas DataFrame
        input_pdf = pd.DataFrame(materialized.data)

        def input_iterator() -> Iterator[Any]:
            yield input_pdf

        # Apply the function
        result_iterator = func(input_iterator())

        # Collect results from the iterator
        result_pdfs = []
        for result_pdf in result_iterator:
            if not isinstance(result_pdf, pd.DataFrame):
                from ...core.exceptions import PySparkTypeError

                raise PySparkTypeError(
                    f"Function must yield pandas DataFrames, got {type(result_pdf).__name__}"
                )
            result_pdfs.append(result_pdf)

        # Concatenate all results
        result_data: List[Dict[str, Any]] = []
        if result_pdfs:
            combined_pdf = pd.concat(result_pdfs, ignore_index=True)
            # Convert to records and ensure string keys
            result_data = [
                {str(k): v for k, v in row.items()}
                for row in combined_pdf.to_dict("records")
            ]

        # Parse schema
        from ...core.schema_inference import infer_schema_from_data

        result_schema: StructType
        if isinstance(schema, str):
            # For DDL string, use schema inference from result data
            # (DDL parsing is complex, so we rely on inference for now)
            result_schema = (
                infer_schema_from_data(result_data) if result_data else self._df.schema
            )
        elif isinstance(schema, StructType):
            result_schema = schema
        else:
            # Try to infer schema from result data
            result_schema = (
                infer_schema_from_data(result_data) if result_data else self._df.schema
            )

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(result_data, result_schema, self._df.storage),
        )

    def unpivot(
        self,
        ids: Union[str, List[str]],
        values: Union[str, List[str]],
        variableColumnName: str = "variable",
        valueColumnName: str = "value",
    ) -> SupportsDataFrameOps:
        """Unpivot columns into rows (opposite of pivot).

        Args:
            ids: Column(s) to keep as identifiers (not unpivoted).
            values: Column(s) to unpivot into rows.
            variableColumnName: Name for the column containing variable names.
            valueColumnName: Name for the column containing values.

        Returns:
            DataFrame: Unpivoted DataFrame.

        Example:
            >>> df.unpivot(
            ...     ids=["id", "name"],
            ...     values=["Q1", "Q2", "Q3", "Q4"],
            ...     variableColumnName="quarter",
            ...     valueColumnName="sales"
            ... )
        """
        # Materialize if lazy
        materialized = self._df._materialize_if_lazy()

        # Normalize inputs
        id_cols = [ids] if isinstance(ids, str) else ids
        value_cols = [values] if isinstance(values, str) else values

        # Validate columns exist and resolve case-insensitively
        from ...core.column_resolver import ColumnResolver

        available_cols = materialized.columns
        case_sensitive = materialized._is_case_sensitive()  # type: ignore[attr-defined]

        resolved_id_cols = []
        for col in id_cols:
            resolved_col = ColumnResolver.resolve_column_name(
                col, available_cols, case_sensitive
            )
            if resolved_col is None:
                raise AnalysisException(
                    f"Cannot resolve column name '{col}' among ({', '.join(materialized.columns)})"
                )
            resolved_id_cols.append(resolved_col)

        resolved_value_cols = []
        for col in value_cols:
            resolved_col = ColumnResolver.resolve_column_name(
                col, available_cols, case_sensitive
            )
            if resolved_col is None:
                raise AnalysisException(
                    f"Cannot resolve column name '{col}' among ({', '.join(materialized.columns)})"
                )
            resolved_value_cols.append(resolved_col)

        # Use resolved column names
        id_cols = resolved_id_cols
        value_cols = resolved_value_cols

        # Create unpivoted data
        unpivoted_data = []
        for row in materialized.data:
            # For each row, create multiple rows (one per value column)
            for value_col in value_cols:
                new_row = {}
                # Add id columns
                for id_col in id_cols:
                    new_row[id_col] = row.get(id_col)
                # Add variable and value
                new_row[variableColumnName] = value_col
                new_row[valueColumnName] = row.get(value_col)
                unpivoted_data.append(new_row)

        # Infer schema for unpivoted DataFrame
        # ID columns keep their types, variable is string, value type is inferred

        fields = []
        # Add id column fields
        for id_col in id_cols:
            for field in materialized.schema.fields:
                if field.name == id_col:
                    fields.append(StructField(id_col, field.dataType, field.nullable))
                    break

        # Add variable column (always string)
        fields.append(StructField(variableColumnName, StringType(), False))

        # Add value column (infer from first value column's type)
        value_type: DataType = StringType()  # Default to string
        for field in materialized.schema.fields:
            if field.name == value_cols[0]:
                value_type = field.dataType
                break
        fields.append(StructField(valueColumnName, value_type, True))

        unpivoted_schema = StructType(fields)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(unpivoted_data, unpivoted_schema, self._df.storage),
        )

    def melt(
        self,
        ids: Optional[List[str]] = None,
        values: Optional[List[str]] = None,
        variableColumnName: str = "variable",
        valueColumnName: str = "value",
    ) -> SupportsDataFrameOps:
        """Unpivot DataFrame from wide to long format (PySpark 3.4+).

        Args:
            ids: List of column names to use as identifier columns
            values: List of column names to unpivot (None = all non-id columns)
            variableColumnName: Name for the variable column
            valueColumnName: Name for the value column

        Returns:
            New DataFrame in long format

        Example:
            >>> df = spark.createDataFrame([{"id": 1, "A": 10, "B": 20}])
            >>> df.melt(ids=["id"], values=["A", "B"]).show()
        """
        id_cols = ids or []
        # Access columns via schema
        all_columns = [field.name for field in self._df.schema.fields]
        value_cols = values or [c for c in all_columns if c not in id_cols]

        result_data = []
        for row in self._df.data:
            for val_col in value_cols:
                new_row = {col: row[col] for col in id_cols}
                new_row[variableColumnName] = val_col
                new_row[valueColumnName] = row.get(val_col)
                result_data.append(new_row)

        # Build new schema - find fields by name

        fields = []
        for col in id_cols:
            field = [f for f in self._df.schema.fields if f.name == col][0]
            fields.append(StructField(col, field.dataType))

        fields.append(StructField(variableColumnName, StringType()))

        # Use first value column's type for value column (or StringType as fallback)
        if value_cols:
            first_value_field = [
                f for f in self._df.schema.fields if f.name == value_cols[0]
            ][0]
            value_type = first_value_field.dataType
        else:
            value_type = StringType()

        fields.append(StructField(valueColumnName, value_type))

        new_schema = StructType(fields)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(result_data, new_schema, self._df.storage),
        )

    # Utility Operations
    def explain(
        self,
        extended: bool = False,
        codegen: bool = False,
        cost: bool = False,
        formatted: bool = False,
        mode: Optional[str] = None,
    ) -> Optional[str]:
        """Explain the execution plan.

        In sparkless, this provides a simplified execution plan showing
        DataFrame operations in a readable format.

        Args:
            extended: If True, show extended plan with more details (default: False)
            codegen: If True, show code generation info (default: False, not implemented)
            cost: If True, show cost estimates (default: False, not implemented)
            formatted: If True, return formatted string (default: False)
            mode: Optional mode for output. If "graph", returns graphviz DOT format.
                  If None, uses default text format.

        Returns:
            If formatted=True or mode="graph", returns the plan as a string.
            Otherwise, prints the plan and returns None.

        Example:
            >>> df.explain()
            >>> df.explain(extended=True)
            >>> dot_graph = df.explain(mode="graph")  # Returns DOT format
        """
        # Handle graph mode
        if mode == "graph":
            return self._generate_graphviz_dot()

        # Build execution plan
        plan_lines = []
        plan_lines.append("== Physical Plan ==")

        # Check if there are pending operations (lazy evaluation)
        if hasattr(self._df, "_operations_queue") and self._df._operations_queue:
            plan_lines.append("")
            plan_lines.append("Pending Operations:")
            for i, (op_name, op_args) in enumerate(self._df._operations_queue, 1):
                args_str = self._format_operation_args(op_args)
                plan_lines.append(f"  {i}. {op_name}({args_str})")
            plan_lines.append("")
            plan_lines.append(
                "Note: Operations are queued and will execute on action (collect, show, etc.)"
            )
        else:
            plan_lines.append("")
            plan_lines.append("Source Operations:")
            plan_lines.append("  MockDataSource")
            plan_lines.append(f"    Rows: {len(self._df.data)}")
            # Access columns via schema
            column_names = [field.name for field in self._df.schema.fields]
            plan_lines.append(f"    Columns: {', '.join(column_names)}")

        if extended:
            plan_lines.append("")
            plan_lines.append("Schema:")
            for field in self._df.schema.fields:
                nullable = "nullable" if field.nullable else "not nullable"
                plan_lines.append(
                    f"  {field.name}: {field.dataType.__class__.__name__} ({nullable})"
                )

        if codegen:
            plan_lines.append("")
            plan_lines.append("Code Generation: Not implemented in sparkless")

        if cost:
            plan_lines.append("")
            plan_lines.append("Cost Estimates: Not implemented in sparkless")

        plan_str = "\n".join(plan_lines)

        if formatted or mode == "graph":
            return plan_str
        else:
            print(plan_str)
            return None

    def _generate_graphviz_dot(self) -> str:
        """Generate graphviz DOT format for query plan visualization.

        Returns:
            Graphviz DOT format string representing the execution plan.
        """
        dot_lines = []
        dot_lines.append("digraph ExecutionPlan {")
        dot_lines.append("  rankdir=TB;")
        dot_lines.append("  node [shape=box, style=rounded];")
        dot_lines.append("")

        # Add source node
        dot_lines.append(
            f'  source [label="MockDataSource\\nRows: {len(self._df.data)}"];'
        )

        # Add operation nodes if there are pending operations
        if hasattr(self._df, "_operations_queue") and self._df._operations_queue:
            prev_node = "source"
            for i, (op_name, op_args) in enumerate(self._df._operations_queue):
                node_id = f"op_{i}"
                args_str = self._format_operation_args(op_args)
                label = f"{op_name}({args_str})"
                # Escape special characters for DOT
                label = label.replace('"', '\\"').replace("\n", "\\n")
                dot_lines.append(f'  {node_id} [label="{label}"];')
                dot_lines.append(f"  {prev_node} -> {node_id};")
                prev_node = node_id

            # Add output node
            dot_lines.append('  output [label="Output"];')
            dot_lines.append(f"  {prev_node} -> output;")
        else:
            # Direct connection from source to output
            dot_lines.append('  output [label="Output"];')
            dot_lines.append("  source -> output;")

        dot_lines.append("}")
        return "\n".join(dot_lines)

    def _format_operation_args(self, args: Any) -> str:
        """Format operation arguments for display in explain plan."""
        if isinstance(args, (list, tuple)):
            if len(args) == 0:
                return ""
            # Format first few args
            formatted = []
            for arg in args[:3]:  # Show first 3 args
                if isinstance(arg, str):
                    formatted.append(f'"{arg}"')
                elif hasattr(arg, "name"):  # Column object
                    formatted.append(f"col({arg.name})")
                else:
                    formatted.append(str(arg)[:20])  # Truncate long strings
            if len(args) > 3:
                formatted.append(f"... ({len(args) - 3} more)")
            return ", ".join(formatted)
        elif isinstance(args, str):
            return f'"{args}"'
        else:
            return str(args)[:50]  # Truncate long strings

    def toDF(self, *cols: str) -> SupportsDataFrameOps:
        """Rename columns of DataFrame (all PySpark versions).

        Args:
            *cols: New column names

        Returns:
            DataFrame with renamed columns

        Raises:
            ValueError: If number of columns doesn't match
        """
        if len(cols) != len(self._df.schema.fields):
            from ...core.exceptions import PySparkValueError

            raise PySparkValueError(
                f"Number of column names ({len(cols)}) must match "
                f"number of columns in DataFrame ({len(self._df.schema.fields)})"
            )

        # Create new schema with renamed columns

        new_fields = [
            StructField(new_name, field.dataType, field.nullable)
            for new_name, field in zip(cols, self._df.schema.fields)
        ]
        new_schema = StructType(new_fields)

        # Rename columns in data
        old_names = [field.name for field in self._df.schema.fields]
        new_data = []
        for row in self._df.data:
            new_row = {
                new_name: row[old_name] for new_name, old_name in zip(cols, old_names)
            }
            new_data.append(new_row)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(new_data, new_schema, self._df.storage),
        )

    def alias(self, alias: str) -> SupportsDataFrameOps:
        """Give DataFrame an alias for join operations (all PySpark versions).

        Args:
            alias: Alias name

        Returns:
            DataFrame with alias set
        """
        # Store alias in a special attribute
        from ..dataframe import DataFrame

        result = DataFrame(self._df.data, self._df.schema, self._df.storage)
        # Store alias dynamically (not in type definition)
        setattr(result, "_alias", alias)
        return cast(
            "SupportsDataFrameOps",
            result,
        )

    def hint(self, name: str, *parameters: Any) -> SupportsDataFrameOps:
        """Provide query optimization hints (all PySpark versions).

        This is a no-op in sparkless as there's no query optimizer.

        Args:
            name: Hint name
            *parameters: Hint parameters

        Returns:
            Same DataFrame (no-op)
        """
        # No-op for mock implementation
        return cast("SupportsDataFrameOps", self._df)

    def withWatermark(
        self, eventTime: str, delayThreshold: str
    ) -> SupportsDataFrameOps:
        """Define watermark for streaming (all PySpark versions).

        Args:
            eventTime: Column name for event time
            delayThreshold: Delay threshold (e.g., "1 hour")

        Returns:
            DataFrame with watermark defined (mock: returns self unchanged)
        """
        # In mock implementation, watermarks don't affect behavior
        # Store for potential future use (dynamically, not in type definition)
        setattr(self._df, "_watermark_col", eventTime)
        setattr(self._df, "_watermark_delay", delayThreshold)
        return cast("SupportsDataFrameOps", self._df)

    def sameSemantics(self, other: DataFrame) -> bool:
        """Check if this DataFrame has the same semantics as another (PySpark 3.1+).

        Simplified implementation that checks schema and data equality.

        Args:
            other: Another DataFrame to compare

        Returns:
            True if semantically equivalent, False otherwise
        """
        # Simplified: check if schemas match
        if len(self._df.schema.fields) != len(other.schema.fields):
            return False

        for f1, f2 in zip(self._df.schema.fields, other.schema.fields):
            if f1.name != f2.name or f1.dataType != f2.dataType:
                return False

        return True

    def semanticHash(self) -> int:
        """Return semantic hash of this DataFrame (PySpark 3.1+).

        Simplified implementation based on schema.

        Returns:
            Hash value representing DataFrame semantics
        """
        # Create hash from schema
        schema_str = ",".join(
            [f"{f.name}:{f.dataType}" for f in self._df.schema.fields]
        )
        return hash(schema_str)

    def inputFiles(self) -> List[str]:
        """Return list of input files for this DataFrame (PySpark 3.1+).

        Returns:
            Empty list (mock DataFrames don't have file inputs)
        """
        # Mock DataFrames are in-memory, so no input files
        return []

    # Partition/Streaming Operations
    def repartitionByRange(
        self,
        numPartitions: Union[int, str, Column],
        *cols: Union[str, Column],
    ) -> SupportsDataFrameOps:
        """Repartition by range of column values (all PySpark versions).

        Args:
            numPartitions: Number of partitions or first column if string/Column
            *cols: Columns to partition by

        Returns:
            New DataFrame repartitioned by range (mock: sorted)
        """
        # For mock purposes, sort by columns to simulate range partitioning
        if isinstance(numPartitions, int):
            return self._df._transformations.orderBy(*cols)
        else:
            # numPartitions is actually the first column
            return self._df._transformations.orderBy(numPartitions, *cols)

    def sortWithinPartitions(
        self, *cols: Union[str, Column], **kwargs: Any
    ) -> SupportsDataFrameOps:
        """Sort within partitions (all PySpark versions).

        Args:
            *cols: Columns to sort by
            **kwargs: Additional arguments (ascending, etc.)

        Returns:
            New DataFrame sorted within partitions (mock: equivalent to orderBy)
        """
        # For mock purposes, treat as regular sort since we have single partition
        return self._df._transformations.orderBy(*cols, **kwargs)

    def toLocalIterator(self, prefetchPartitions: bool = False) -> Any:
        """Return iterator over rows (all PySpark versions).

        Args:
            prefetchPartitions: Whether to prefetch partitions (ignored in mock)

        Returns:
            Iterator over Row objects
        """
        if self._df._operations_queue:
            materialized = self._df._materialize_if_lazy()
            return self._df._get_collection_handler().to_local_iterator(
                materialized.data, materialized.schema, prefetchPartitions
            )
        return self._df._get_collection_handler().to_local_iterator(
            self._df.data, self._df.schema, prefetchPartitions
        )

    def checkpoint(self, eager: bool = False) -> SupportsDataFrameOps:
        """Checkpoint the DataFrame (no-op in mock; returns self)."""
        return cast("SupportsDataFrameOps", self._df)

    def localCheckpoint(self, eager: bool = True) -> SupportsDataFrameOps:
        """Local checkpoint to truncate lineage (all PySpark versions).

        Args:
            eager: Whether to checkpoint eagerly

        Returns:
            Same DataFrame with truncated lineage
        """
        if eager:
            # Force materialization
            _ = len(self._df.data)
        return cast("SupportsDataFrameOps", self._df)

    def isLocal(self) -> bool:
        """Check if running in local mode (all PySpark versions).

        Returns:
            True if running in local mode (mock: always True)
        """
        return True

    @property
    def isStreaming(self) -> bool:
        """Whether this DataFrame is streaming (always False in mock)."""
        return False

    # RDD Operations
    def foreach(self, f: Any) -> None:
        """Apply function to each row (action, all PySpark versions).

        Args:
            f: Function to apply to each Row
        """
        for row in self._df._display.collect():
            f(row)

    def foreachPartition(self, f: Any) -> None:
        """Apply function to each partition (action, all PySpark versions).

        Args:
            f: Function to apply to each partition Iterator[Row]
        """
        # Mock implementation: treat entire dataset as single partition
        f(iter(self._df._display.collect()))

    def writeTo(self, table: str) -> Any:
        """Write DataFrame to a table using the Table API (PySpark 3.1+).

        Args:
            table: Name of the table to write to.

        Returns:
            DataFrameWriter configured for table write operation.

        Example:
            >>> df.writeTo("my_table").create()
        """
        from ..writer import DataFrameWriter
        from ...storage import MemoryStorageManager

        # Get storage manager from session if available
        storage = getattr(self._df, "storage", None)
        if storage is None:
            # Create a default storage manager if not available
            storage = MemoryStorageManager()

        writer = DataFrameWriter(self._df, storage)
        # Set table name for writeTo operation
        writer._table_name = table
        return writer
