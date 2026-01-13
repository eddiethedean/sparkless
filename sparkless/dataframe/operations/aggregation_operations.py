"""Aggregation operations for DataFrame."""

from typing import Any, Dict, List, Tuple, Union
import statistics
from ...spark_types import StructType, StructField, StringType


class AggregationOperationsStatic:
    """Static utility methods for aggregation operations (legacy - use AggregationOperations mixin instead)."""

    @staticmethod
    def describe(
        data: List[Dict[str, Any]], schema: StructType, cols: List[str]
    ) -> Tuple[List[Dict[str, Any]], StructType]:
        """Compute basic statistics for numeric columns.

        Args:
            data: DataFrame data
            schema: DataFrame schema
            cols: Column names to describe

        Returns:
            Tuple of (result_data, result_schema)
        """
        # Determine which columns to describe
        if not cols:
            # Describe all numeric columns
            numeric_cols = []
            for field in schema.fields:
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
            available_cols = [field.name for field in schema.fields]
            for col in numeric_cols:
                if col not in available_cols:
                    from ...core.exceptions.analysis import ColumnNotFoundException

                    raise ColumnNotFoundException(col)

        if not numeric_cols:
            # No numeric columns found
            return [], schema

        # Calculate statistics for each column
        result_data = []

        for col in numeric_cols:
            # Extract values for this column
            values = []
            for row in data:
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

        return result_data, result_schema

    @staticmethod
    def summary(
        data: List[Dict[str, Any]], schema: StructType, stats: List[str]
    ) -> Tuple[List[Dict[str, Any]], StructType]:
        """Compute extended statistics for numeric columns.

        Args:
            data: DataFrame data
            schema: DataFrame schema
            stats: Statistics to compute

        Returns:
            Tuple of (result_data, result_schema)
        """
        # Default statistics if none provided
        if not stats:
            stats = ["count", "mean", "stddev", "min", "25%", "50%", "75%", "max"]

        # Find numeric columns
        numeric_cols = []
        for field in schema.fields:
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

        if not numeric_cols:
            # No numeric columns found
            return [], schema

        # Calculate statistics for each column
        result_data = []

        for col in numeric_cols:
            # Extract values for this column
            values = []
            for row in data:
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

                # Calculate requested statistics
                for stat in stats:
                    if stat == "count":
                        stats_row[stat] = str(len(values))
                    elif stat == "mean":
                        stats_row[stat] = str(round(statistics.mean(values), 4))
                    elif stat == "stddev":
                        stats_row[stat] = str(
                            round(
                                statistics.stdev(values) if len(values) > 1 else 0.0, 4
                            )
                        )
                    elif stat == "min":
                        stats_row[stat] = str(min(values))
                    elif stat == "max":
                        stats_row[stat] = str(max(values))
                    elif stat == "25%":
                        stats_row[stat] = str(
                            round(statistics.quantiles(values, n=4)[0], 4)
                        )
                    elif stat == "50%":
                        stats_row[stat] = str(round(statistics.median(values), 4))
                    elif stat == "75%":
                        stats_row[stat] = str(
                            round(statistics.quantiles(values, n=4)[2], 4)
                        )
                    else:
                        stats_row[stat] = "NaN"

            result_data.append(stats_row)

        # Create result schema
        schema_fields = [StructField("summary", StringType())]
        for stat in stats:
            schema_fields.append(StructField(stat, StringType()))

        result_schema = StructType(schema_fields)

        return result_data, result_schema

    @staticmethod
    def compute_basic_stats(values: List[Union[int, float]]) -> Dict[str, str]:
        """Compute basic statistics for a list of numeric values.

        Args:
            values: List of numeric values

        Returns:
            Dictionary with basic statistics
        """
        if not values:
            return {
                "count": "0",
                "mean": "NaN",
                "stddev": "NaN",
                "min": "NaN",
                "max": "NaN",
            }

        return {
            "count": str(len(values)),
            "mean": str(round(statistics.mean(values), 4)),
            "stddev": str(
                round(statistics.stdev(values) if len(values) > 1 else 0.0, 4)
            ),
            "min": str(min(values)),
            "max": str(max(values)),
        }

    @staticmethod
    def compute_extended_stats(
        values: List[Union[int, float]], stats: List[str]
    ) -> Dict[str, str]:
        """Compute extended statistics for a list of numeric values.

        Args:
            values: List of numeric values
            stats: List of statistics to compute

        Returns:
            Dictionary with requested statistics
        """
        if not values:
            result = {}
            for stat in stats:
                result[stat] = "NaN"
            return result

        result = {}
        for stat in stats:
            if stat == "count":
                result[stat] = str(len(values))
            elif stat == "mean":
                result[stat] = str(round(statistics.mean(values), 4))
            elif stat == "stddev":
                result[stat] = str(
                    round(statistics.stdev(values) if len(values) > 1 else 0.0, 4)
                )
            elif stat == "min":
                result[stat] = str(min(values))
            elif stat == "max":
                result[stat] = str(max(values))
            elif stat == "25%":
                result[stat] = str(round(statistics.quantiles(values, n=4)[0], 4))
            elif stat == "50%":
                result[stat] = str(round(statistics.median(values), 4))
            elif stat == "75%":
                result[stat] = str(round(statistics.quantiles(values, n=4)[2], 4))
            else:
                result[stat] = "NaN"

        return result

    @staticmethod
    def get_numeric_columns(schema: StructType) -> List[str]:
        """Get list of numeric column names from schema.

        Args:
            schema: DataFrame schema

        Returns:
            List of numeric column names
        """
        numeric_cols = []
        for field in schema.fields:
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
        return numeric_cols

    @staticmethod
    def extract_numeric_values(
        data: List[Dict[str, Any]], column: str
    ) -> List[Union[int, float]]:
        """Extract numeric values from a specific column.

        Args:
            data: DataFrame data
            column: Column name

        Returns:
            List of numeric values
        """
        values = []
        for row in data:
            value = row.get(column)
            if value is not None and isinstance(value, (int, float)):
                values.append(value)
        return values
