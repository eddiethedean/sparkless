"""
Base grouped data implementation for Sparkless.

This module provides the core GroupedData class for DataFrame aggregation
operations, maintaining compatibility with PySpark's GroupedData interface.
"""

from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING, Tuple, Union, cast
import statistics

from ...functions import (
    Column,
    ColumnOperation,
    AggregateFunction,
)
from ...core.exceptions.analysis import AnalysisException
from ...core.type_utils import (
    is_literal as is_literal_type,
    is_column,
    is_column_operation,
    get_expression_name,
    get_expression_value,
)

from ..protocols import SupportsDataFrameOps

if TYPE_CHECKING:
    from ..dataframe import DataFrame
    from .rollup import RollupGroupedData
    from .cube import CubeGroupedData
    from .pivot import PivotGroupedData


class GroupedData:
    """Mock grouped data for aggregation operations.

    Provides grouped data functionality for DataFrame aggregation operations,
    maintaining compatibility with PySpark's GroupedData interface.
    """

    def __init__(self, df: SupportsDataFrameOps, group_columns: List[str]):
        """Initialize GroupedData.

        Args:
            df: The DataFrame being grouped.
            group_columns: List of column names to group by.
        """
        self.df: SupportsDataFrameOps = df
        self.group_columns = group_columns

    def agg(
        self,
        *exprs: Union[str, Column, ColumnOperation, AggregateFunction, Dict[str, str]],
    ) -> "DataFrame":
        """Aggregate grouped data.

        Args:
            *exprs: Aggregation expressions or dictionary mapping column names to aggregation functions.

        Returns:
            New DataFrame with aggregated results.
        """
        from ...functions import F

        # Track expression processing order to preserve column ordering
        # For dict syntax, PySpark preserves dict order; otherwise we sort alphabetically
        expression_order: List[str] = []
        is_dict_syntax = len(exprs) == 1 and isinstance(exprs[0], dict)

        # PySpark-style strict validation: all expressions must be Column or ColumnOperation.
        # Skip this for dict syntax (handled separately below).
        # NOTE (BUG-022): PySpark also accepts AggregateFunction objects in many contexts
        # (e.g., F.first, F.last, F.collect_list). We therefore allow AggregateFunction
        # instances through validation and handle them explicitly later in this method
        # instead of rejecting them up-front.
        if not is_dict_syntax:
            for i, expr in enumerate(exprs):
                # Allow strings for backward compatibility
                if isinstance(expr, str):
                    continue
                # Allow AggregateFunction instances - they are handled explicitly
                # later in this method (see the AggregateFunction branch below).
                if isinstance(expr, AggregateFunction):
                    continue
                if not (is_column(expr) or is_column_operation(expr)):
                    raise AssertionError(
                        f"all exprs should be Column, got {type(expr).__name__} at argument {i}"
                    )

        # Handle dictionary syntax: {"col": "agg_func"}
        if is_dict_syntax:
            agg_dict = exprs[0]
            if not isinstance(agg_dict, dict):
                raise TypeError(
                    f"Expected dict for dict syntax aggregation, got {type(agg_dict)}"
                )
            converted_exprs: List[
                Union[str, Column, ColumnOperation, AggregateFunction]
            ] = []
            for col_name, agg_func in agg_dict.items():
                if agg_func == "sum":
                    expr = F.sum(col_name)
                    converted_exprs.append(expr)
                    expression_order.append(f"sum({col_name})")
                elif agg_func == "avg" or agg_func == "mean":
                    expr = F.avg(col_name)
                    converted_exprs.append(expr)
                    expression_order.append(f"avg({col_name})")
                elif agg_func == "max":
                    expr = F.max(col_name)
                    converted_exprs.append(expr)
                    expression_order.append(f"max({col_name})")
                elif agg_func == "min":
                    expr = F.min(col_name)
                    converted_exprs.append(expr)
                    expression_order.append(f"min({col_name})")
                elif agg_func == "count":
                    # For dict syntax, PySpark names it "count(column)" not "count"
                    count_expr = F.count(col_name)
                    converted_exprs.append(count_expr)
                    expression_order.append(
                        f"count({col_name})"
                    )  # Track the actual column name PySpark uses
                elif agg_func == "stddev":
                    expr = F.stddev(col_name)
                    converted_exprs.append(expr)
                    expression_order.append(f"stddev({col_name})")
                elif agg_func == "variance":
                    expr = F.variance(col_name)
                    converted_exprs.append(expr)
                    expression_order.append(f"variance({col_name})")
                else:
                    # Fallback to string expression
                    converted_exprs.append(f"{agg_func}({col_name})")
                    expression_order.append(f"{agg_func}({col_name})")
            exprs = tuple(converted_exprs)
        else:
            # For non-dict syntax, track expression order (will sort alphabetically later)
            expression_order = []

        # Materialize the DataFrame if it has queued operations
        if self.df._operations_queue:
            self.df = self.df._materialize_if_lazy()

        # Group data by group columns
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        for row in self.df.data:
            group_key = tuple(row.get(col) for col in self.group_columns)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row)

        # Track which result keys are from count/rank functions (non-nullable)
        non_nullable_keys = set()

        # Track result key order for non-dict syntax (PySpark preserves expression order)
        result_key_order: List[str] = []

        # Apply aggregations
        result_data = []
        for group_key, group_rows in groups.items():
            result_row = dict(zip(self.group_columns, group_key))

            for expr in exprs:
                if isinstance(expr, str):
                    # Handle string expressions like "sum(age)"
                    result_key, result_value = self._evaluate_string_expression(
                        expr, group_rows
                    )
                    # Check if this is a count function
                    if expr.startswith("count("):
                        non_nullable_keys.add(result_key)
                    # Track result key order (same for all groups)
                    if result_key not in result_key_order:
                        result_key_order.append(result_key)
                    result_row[result_key] = result_value
                elif isinstance(expr, dict):
                    # Handle dict expressions (for pivot operations)
                    result_row.update(expr)
                elif is_literal_type(expr):
                    # For literals in aggregation, just use their value
                    # Note: Literal may be passed at runtime even if not in type annotation
                    result_key = get_expression_name(expr)
                    # Track result key order (same for all groups)
                    if result_key not in result_key_order:
                        result_key_order.append(result_key)
                    result_row[result_key] = get_expression_value(expr)
                elif is_column_operation(expr):
                    # Handle ColumnOperation first (before AggregateFunction check)
                    # ColumnOperation has function_name but should be handled differently
                    # Check if this is a cast operation wrapping an aggregate function
                    if (
                        isinstance(expr, ColumnOperation)
                        and hasattr(expr, "operation")
                        and expr.operation == "cast"
                    ):
                        # Check if the column being cast is an AggregateFunction or wraps one
                        cast_agg_func: Optional[AggregateFunction] = None
                        if isinstance(expr.column, AggregateFunction):
                            cast_agg_func = expr.column
                        elif (
                            isinstance(expr.column, ColumnOperation)
                            and hasattr(expr.column, "_aggregate_function")
                            and expr.column._aggregate_function is not None
                        ):
                            # ColumnOperation wrapping an AggregateFunction (e.g., F.sum().cast())
                            cast_agg_func = expr.column._aggregate_function

                        if cast_agg_func is not None:
                            # Evaluate the aggregate function first
                            _, agg_result = self._evaluate_aggregate_function(
                                cast_agg_func, group_rows
                            )

                            # Apply cast to the result
                            from ...dataframe.casting.type_converter import (
                                TypeConverter,
                            )
                            from ...spark_types import (
                                DataType,
                                StringType,
                                IntegerType,
                                LongType,
                                DoubleType,
                                FloatType,
                                BooleanType,
                            )

                            # Handle string type names (e.g., "string", "int")
                            # expr.value is the cast target type (ColumnOperation has value attribute)
                            cast_type: Optional[DataType] = None
                            if isinstance(expr, ColumnOperation) and hasattr(
                                expr, "value"
                            ):
                                cast_type_value = expr.value
                                if isinstance(cast_type_value, str):
                                    type_name_map: Dict[str, DataType] = {
                                        "string": StringType(),
                                        "str": StringType(),
                                        "int": IntegerType(),
                                        "integer": IntegerType(),
                                        "long": LongType(),
                                        "bigint": LongType(),
                                        "double": DoubleType(),
                                        "float": FloatType(),
                                        "boolean": BooleanType(),
                                        "bool": BooleanType(),
                                    }
                                    cast_type = type_name_map.get(
                                        cast_type_value.lower()
                                    )
                                elif isinstance(cast_type_value, DataType):
                                    cast_type = cast_type_value

                            # Apply cast transformation
                            if cast_type is not None:
                                cast_result = TypeConverter.cast_to_type(
                                    agg_result, cast_type
                                )
                            else:
                                # Fallback to string conversion if type not recognized
                                cast_result = (
                                    str(agg_result) if agg_result is not None else None
                                )

                            # Check for alias first - if alias is set, use it instead of CAST expression
                            # This fixes issue #332: cast+alias+select should use alias name
                            if hasattr(expr, "_alias_name") and expr._alias_name:
                                result_key = expr._alias_name
                            elif hasattr(expr, "name") and expr.name:
                                # Check if expr.name is an alias (not the column name and not a CAST expression)
                                # For cast operations without alias, expr.name returns the column name
                                # For cast operations with alias, expr.name returns the alias
                                # We need to distinguish between these cases
                                expr_name = expr.name
                                column_name = (
                                    cast_agg_func.name
                                    if hasattr(cast_agg_func, "name")
                                    else str(cast_agg_func)
                                )
                                # If name is different from column name and doesn't start with "CAST(",
                                # it's likely an alias
                                if (
                                    expr_name != column_name
                                    and not expr_name.startswith("CAST(")
                                ):
                                    result_key = expr_name
                                else:
                                    # Generate proper column name for cast operation (PySpark format)
                                    # Format: CAST(avg(value) AS STRING)
                                    if cast_type is not None:
                                        type_name = str(cast_type).upper()
                                    elif isinstance(expr, ColumnOperation) and hasattr(
                                        expr, "value"
                                    ):
                                        type_name = str(expr.value).upper()
                                    else:
                                        type_name = "STRING"
                                    result_key = (
                                        f"CAST({cast_agg_func.name} AS {type_name})"
                                    )
                            else:
                                # Generate proper column name for cast operation (PySpark format)
                                # Format: CAST(avg(value) AS STRING)
                                if cast_type is not None:
                                    type_name = str(cast_type).upper()
                                elif isinstance(expr, ColumnOperation) and hasattr(
                                    expr, "value"
                                ):
                                    type_name = str(expr.value).upper()
                                else:
                                    type_name = "STRING"
                                result_key = (
                                    f"CAST({cast_agg_func.name} AS {type_name})"
                                )
                            result_value = cast_result
                        else:
                            # Regular cast operation (not on aggregate)
                            result_key, result_value = self._evaluate_column_expression(
                                cast("Union[Column, ColumnOperation]", expr), group_rows
                            )
                    # Check if this ColumnOperation wraps an aggregate function (PySpark-style)
                    elif (
                        hasattr(expr, "_aggregate_function")
                        and expr._aggregate_function is not None
                    ):
                        # This is a ColumnOperation wrapping an AggregateFunction (e.g., corr, covar_samp)
                        result_key, result_value = self._evaluate_aggregate_function(
                            expr._aggregate_function, group_rows
                        )
                        # Use the alias from ColumnOperation if set
                        if hasattr(expr, "_alias_name") and expr._alias_name:
                            result_key = expr._alias_name
                        elif hasattr(expr, "name"):
                            result_key = expr.name
                    else:
                        # Regular ColumnOperation
                        result_key, result_value = self._evaluate_column_expression(
                            cast("Union[Column, ColumnOperation]", expr), group_rows
                        )
                    # Check if this is a count function
                    if hasattr(expr, "operation") and expr.operation == "count":
                        non_nullable_keys.add(result_key)
                    # Track result key order (same for all groups)
                    if result_key not in result_key_order:
                        result_key_order.append(result_key)
                    result_row[result_key] = result_value
                elif hasattr(expr, "function_name") and not is_column_operation(expr):
                    # Handle AggregateFunction (but not ColumnOperation)
                    if not isinstance(expr, AggregateFunction):
                        raise TypeError(f"Expected AggregateFunction, got {type(expr)}")
                    # isinstance check above ensures expr is AggregateFunction at this point
                    result_key, result_value = self._evaluate_aggregate_function(
                        expr, group_rows
                    )
                    # Check if this is a count function
                    if expr.function_name == "count":
                        non_nullable_keys.add(result_key)
                    # Track result key order (same for all groups)
                    if result_key not in result_key_order:
                        result_key_order.append(result_key)
                    result_row[result_key] = result_value
                elif is_column(expr):
                    # Handle Column (but not ColumnOperation, which is handled above)
                    # is_column narrows to Column, but _evaluate_column_expression accepts Union
                    # Cast to help mypy understand the type in Python 3.9
                    result_key, result_value = self._evaluate_column_expression(
                        cast("Union[Column, ColumnOperation]", expr),
                        group_rows,
                    )
                    # Track result key order (same for all groups)
                    if result_key not in result_key_order:
                        result_key_order.append(result_key)
                    result_row[result_key] = result_value
                elif isinstance(expr, dict):  # type: ignore[unreachable,unused-ignore]
                    # Skip dict expressions - should have been converted already
                    # This branch handles dict expressions that weren't converted
                    pass
                    # Type system doesn't allow expr to be both Column and dict after other checks
                    pass

            # Reorder result_row to match PySpark's column ordering:
            # Group columns first (in their original order), then aggregation columns
            group_cols_dict = {col: result_row[col] for col in self.group_columns}
            agg_cols_dict = {
                col: result_row[col]
                for col in result_row
                if col not in self.group_columns
            }

            # PySpark behavior for column ordering:
            # - For dict syntax: sorts aggregation columns by the column name being aggregated first,
            #   then by function name (not by full column name like "avg(salary)")
            # - For non-dict syntax: preserves expression order
            if is_dict_syntax and expression_order:
                # For dict syntax, PySpark sorts by the column name being aggregated first
                def sort_key(col_name: str) -> Tuple[str, str]:
                    """Extract (column_name, function_name) for sorting."""
                    # Column names are like "avg(salary)", "count(id)", etc.
                    import re

                    match = re.match(r"(\w+)\((\w+)\)", col_name)
                    if match:
                        func_name, agg_col_name = match.groups()
                        return (
                            agg_col_name,
                            func_name,
                        )  # Sort by column name first, then function
                    # Fallback: use full string
                    return (col_name, "")

                # Sort by the column name being aggregated, then by function name
                ordered_agg_cols = dict(
                    sorted(
                        agg_cols_dict.items(),
                        key=lambda x: sort_key(
                            x[0] if isinstance(x[0], str) else str(x[0])
                        ),
                    )
                )
            elif result_key_order:
                # For non-dict syntax, preserve expression order (PySpark behavior)
                ordered_agg_cols = {}
                for key in result_key_order:
                    if key in agg_cols_dict:
                        ordered_agg_cols[key] = agg_cols_dict[key]
                # Add any keys that weren't in the tracked order (shouldn't happen)
                for key in agg_cols_dict:
                    if key not in ordered_agg_cols:
                        ordered_agg_cols[key] = agg_cols_dict[key]
            else:
                # Fallback: sort aggregation columns alphabetically (shouldn't happen)
                ordered_agg_cols = dict(sorted(agg_cols_dict.items()))

            # Combine: group cols first, then ordered agg cols
            result_row_ordered = {**group_cols_dict, **ordered_agg_cols}
            result_data.append(result_row_ordered)

        # Create result DataFrame with proper schema
        from ..dataframe import DataFrame
        from ...spark_types import (
            StructType,
            StructField,
            StringType,
            LongType,
            DoubleType,
            BooleanType,
            DataType,
        )

        # Track which expressions are literals for proper nullable inference
        # (used in both branches)
        literal_keys: Set[str] = set()
        for expr in exprs:
            if is_literal_type(expr):
                lit_key = get_expression_name(expr)
                literal_keys.add(lit_key)

        # Create schema based on the first result row and expression types
        if result_data:
            fields = []

            for key, value in result_data[0].items():
                if key in self.group_columns:
                    # Use existing schema for group columns
                    for field in self.df.schema.fields:
                        if field.name == key:
                            fields.append(field)
                            break
                else:
                    # Determine if this is a literal value
                    is_literal = key in literal_keys

                    # Count functions, window ranking functions, and boolean functions are non-nullable in PySpark
                    # Other aggregations and literals are non-nullable
                    is_count_function = key in non_nullable_keys or any(
                        key.startswith(func)
                        for func in [
                            "count(",
                            "count(1)",
                            "count(DISTINCT",
                            "count_if",
                            "row_number",
                            "rank",
                            "dense_rank",
                            "row_num",
                            "dept_row_num",
                            "global_row",
                            "dept_row",
                            "dept_rank",
                        ]
                    )
                    is_boolean_function = any(
                        key.startswith(func)
                        for func in ["coalesced_", "is_null_", "is_nan_"]
                    )
                    nullable = not (
                        is_literal or is_count_function or is_boolean_function
                    )

                    if isinstance(value, bool):
                        data_type = BooleanType(nullable=nullable)
                        fields.append(StructField(key, data_type, nullable=nullable))
                    elif isinstance(value, str):
                        str_data_type: DataType = StringType(nullable=nullable)
                        fields.append(
                            StructField(key, str_data_type, nullable=nullable)
                        )
                    elif isinstance(value, float):
                        float_data_type: DataType = DoubleType(nullable=nullable)
                        fields.append(
                            StructField(key, float_data_type, nullable=nullable)
                        )
                    else:
                        long_data_type: DataType = LongType(nullable=nullable)
                        fields.append(
                            StructField(key, long_data_type, nullable=nullable)
                        )
            schema = StructType(fields)
            return DataFrame(result_data, schema, self.df.storage)
        else:
            # Empty result - but we still need to preserve schema
            # Build schema from group columns and aggregation expressions
            fields = []

            # Add group columns from original DataFrame schema
            for group_col in self.group_columns:
                for field in self.df.schema.fields:
                    if field.name == group_col:
                        fields.append(field)
                        break

            # Infer schema from aggregation expressions
            # (literal_keys already defined above)
            for expr in exprs:
                if isinstance(expr, str):
                    # Handle string expressions like "sum(age)"
                    result_key = expr  # Use expression as key
                    # Check if this is a count function
                    is_count_function = (
                        result_key.startswith("count(")
                        or result_key.startswith("count(1)")
                        or result_key.startswith("count(DISTINCT")
                    )
                    is_boolean_function = any(
                        result_key.startswith(func)
                        for func in ["coalesced_", "is_null_", "is_nan_"]
                    )
                    is_literal = result_key in literal_keys
                    nullable = not (
                        is_literal or is_count_function or is_boolean_function
                    )

                    # Infer type from expression name
                    if (
                        "sum(" in result_key
                        or "avg(" in result_key
                        or "mean(" in result_key
                    ):
                        fields.append(
                            StructField(
                                result_key,
                                DoubleType(nullable=nullable),
                                nullable=nullable,
                            )
                        )
                    elif "count(" in result_key:
                        fields.append(
                            StructField(
                                result_key, LongType(nullable=False), nullable=False
                            )
                        )
                    elif "min(" in result_key or "max(" in result_key:
                        # For min/max, we'd need to check the column type, default to StringType
                        fields.append(
                            StructField(
                                result_key,
                                StringType(nullable=nullable),
                                nullable=nullable,
                            )
                        )
                    else:
                        # Default to StringType for unknown expressions
                        fields.append(
                            StructField(
                                result_key,
                                StringType(nullable=nullable),
                                nullable=nullable,
                            )
                        )
                elif is_literal_type(expr):
                    # Handle literals
                    lit_key = get_expression_name(expr)
                    lit_value = get_expression_value(expr)
                    if isinstance(lit_value, bool):
                        fields.append(
                            StructField(
                                lit_key, BooleanType(nullable=False), nullable=False
                            )
                        )
                    elif isinstance(lit_value, (int, float)):
                        fields.append(
                            StructField(
                                lit_key, DoubleType(nullable=False), nullable=False
                            )
                        )
                    else:
                        fields.append(
                            StructField(
                                lit_key, StringType(nullable=False), nullable=False
                            )
                        )
                elif hasattr(expr, "name"):
                    # Handle Column or ColumnOperation with aggregation
                    result_key = expr.name
                    is_count_function = result_key in non_nullable_keys or any(
                        result_key.startswith(func)
                        for func in ["count(", "count(1)", "count(DISTINCT", "count_if"]
                    )
                    is_literal = result_key in literal_keys
                    nullable = not (is_literal or is_count_function)

                    # For column operations, try to infer type from the operation
                    if hasattr(expr, "operation") and expr.operation:
                        if expr.operation in ["sum", "avg", "mean"]:
                            fields.append(
                                StructField(
                                    result_key,
                                    DoubleType(nullable=nullable),
                                    nullable=nullable,
                                )
                            )
                        elif expr.operation == "count":
                            fields.append(
                                StructField(
                                    result_key, LongType(nullable=False), nullable=False
                                )
                            )
                        else:
                            # Default to StringType
                            fields.append(
                                StructField(
                                    result_key,
                                    StringType(nullable=nullable),
                                    nullable=nullable,
                                )
                            )
                    else:
                        # Default to StringType for unknown expressions
                        fields.append(
                            StructField(
                                result_key,
                                StringType(nullable=nullable),
                                nullable=nullable,
                            )
                        )
                elif hasattr(expr, "function_name"):
                    # Handle AggregateFunction
                    result_key = getattr(expr, "name", expr.function_name)
                    if expr.function_name == "count":
                        fields.append(
                            StructField(
                                result_key, LongType(nullable=False), nullable=False
                            )
                        )
                    elif expr.function_name in ["sum", "avg", "mean"]:
                        fields.append(
                            StructField(
                                result_key, DoubleType(nullable=True), nullable=True
                            )
                        )
                    else:
                        fields.append(
                            StructField(
                                result_key, StringType(nullable=True), nullable=True
                            )
                        )

            schema = StructType(fields)
            return DataFrame(result_data, schema, self.df.storage)

    def _evaluate_string_expression(
        self, expr: str, group_rows: List[Dict[str, Any]]
    ) -> Tuple[str, Any]:
        """Evaluate string aggregation expression.

        Args:
            expr: String expression to evaluate.
            group_rows: Rows in the group.

        Returns:
            Tuple of (result_key, result_value).
        """
        if expr.startswith("sum("):
            col_name = expr[4:-1]
            # Validate column exists using ValidationHandler
            from ...dataframe.validation_handler import ValidationHandler

            validator = ValidationHandler()
            validator.validate_column_exists(self.df.schema, col_name, "aggregation")
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            return expr, sum(values) if values else 0
        elif expr.startswith("avg("):
            col_name = expr[4:-1]
            # Validate column exists using ValidationHandler
            from ...dataframe.validation_handler import ValidationHandler

            validator = ValidationHandler()
            validator.validate_column_exists(self.df.schema, col_name, "aggregation")
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            return expr, sum(values) / len(values) if values else 0
        elif expr.startswith("count("):
            return expr, len(group_rows)
        elif expr.startswith("max("):
            col_name = expr[4:-1]
            # Validate column exists using ValidationHandler
            from ...dataframe.validation_handler import ValidationHandler

            validator = ValidationHandler()
            validator.validate_column_exists(self.df.schema, col_name, "aggregation")
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr, max(values) if values else None
        elif expr.startswith("min("):
            col_name = expr[4:-1]
            # Validate column exists using ValidationHandler
            from ...dataframe.validation_handler import ValidationHandler

            validator = ValidationHandler()
            validator.validate_column_exists(self.df.schema, col_name, "aggregation")
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr, min(values) if values else None
        else:
            return expr, None

    def _evaluate_aggregate_function(
        self, expr: AggregateFunction, group_rows: List[Dict[str, Any]]
    ) -> Tuple[str, Any]:
        """Evaluate AggregateFunction.

        Args:
            expr: Aggregate function to evaluate.
            group_rows: Rows in the group.

        Returns:
            Tuple of (result_key, result_value).
        """
        func_name = expr.function_name
        col_name = (
            getattr(expr, "column_name", "") if hasattr(expr, "column_name") else ""
        )

        # Use the name from the aggregate function (already set correctly by _generate_name)
        # This handles both explicit aliases and the correct default names (e.g., count(dept) for countDistinct)
        alias_name = expr.name

        if func_name == "sum":
            # If the aggregate targets an expression (e.g., cast or arithmetic), evaluate per-row
            if hasattr(expr, "column") and hasattr(expr.column, "operation"):
                values = []
                for row_data in group_rows:
                    try:
                        from ...core.protocols import ColumnExpression  # noqa: TC001

                        expr_result = self.df._evaluate_column_expression(
                            row_data, cast("ColumnExpression", expr.column)
                        )
                        if expr_result is not None:
                            # Coerce booleans to ints to mirror Spark when user casts
                            if isinstance(expr_result, bool):
                                expr_result = 1 if expr_result else 0
                            # Convert numeric-looking strings
                            if isinstance(expr_result, str):
                                try:
                                    expr_result = (
                                        float(expr_result)
                                        if "." in expr_result
                                        else int(expr_result)
                                    )
                                except ValueError:
                                    continue
                            values.append(expr_result)
                    except (ValueError, TypeError, AttributeError):
                        pass
                result_key = alias_name if alias_name else f"sum({col_name})"
                return result_key, sum(values) if values else 0
            # Simple column: validate and sum (case-insensitive)
            if col_name and not any(
                op in col_name
                for op in [
                    "+",
                    "-",
                    "*",
                    "/",
                    "(",
                    ")",
                    "extract",
                    "TRY_CAST",
                    "AS",
                ]
            ):
                from ..validation.column_validator import ColumnValidator

                # Resolve column name using ColumnResolver
                case_sensitive = (
                    self.df._is_case_sensitive()
                    if hasattr(self.df, "_is_case_sensitive")
                    else True
                )
                actual_col_name = ColumnValidator._find_column(
                    self.df.schema, col_name, case_sensitive
                )
                if actual_col_name is None:
                    available_columns = [field.name for field in self.df.schema.fields]
                    from ...core.exceptions.operation import SparkColumnNotFoundError

                    raise SparkColumnNotFoundError(col_name, available_columns)
                col_name = actual_col_name
            values = []
            for row in group_rows:
                val = row.get(col_name)
                if val is not None:
                    if isinstance(val, bool):
                        val = 1 if val else 0
                    if isinstance(val, str):
                        try:
                            val = float(val) if "." in val else int(val)
                        except ValueError:
                            continue
                    values.append(val)
            result_key = alias_name if alias_name else f"sum({col_name})"
            return result_key, sum(values) if values else 0
        elif func_name == "avg":
            # Expression-aware avg
            if hasattr(expr, "column") and hasattr(expr.column, "operation"):
                values = []
                for row_data in group_rows:
                    try:
                        from ...core.protocols import ColumnExpression  # noqa: TC001

                        expr_result = self.df._evaluate_column_expression(
                            row_data, cast("ColumnExpression", expr.column)
                        )
                        if expr_result is not None:
                            if isinstance(expr_result, bool):
                                expr_result = 1 if expr_result else 0
                            if isinstance(expr_result, str):
                                try:
                                    expr_result = (
                                        float(expr_result)
                                        if "." in expr_result
                                        else int(expr_result)
                                    )
                                except ValueError:
                                    continue
                            values.append(expr_result)
                    except (ValueError, TypeError, AttributeError):
                        pass
                result_key = alias_name if alias_name else f"avg({col_name})"
                return result_key, (sum(values) / len(values)) if values else None
            # Simple column: validate and average (case-insensitive)
            if col_name and not any(
                op in col_name
                for op in [
                    "+",
                    "-",
                    "*",
                    "/",
                    "(",
                    ")",
                    "extract",
                    "TRY_CAST",
                    "AS",
                ]
            ):
                from ..validation.column_validator import ColumnValidator

                # Resolve column name using ColumnResolver
                case_sensitive = (
                    self.df._is_case_sensitive()
                    if hasattr(self.df, "_is_case_sensitive")
                    else True
                )
                actual_col_name = ColumnValidator._find_column(
                    self.df.schema, col_name, case_sensitive
                )
                if actual_col_name is None:
                    available_columns = [field.name for field in self.df.schema.fields]
                    from ...core.exceptions.operation import SparkColumnNotFoundError

                    raise SparkColumnNotFoundError(col_name, available_columns)
                col_name = actual_col_name
            values = []
            for row in group_rows:
                val = row.get(col_name)
                if val is not None:
                    if isinstance(val, bool):
                        val = 1 if val else 0
                    if isinstance(val, str):
                        try:
                            val = float(val) if "." in val else int(val)
                        except ValueError:
                            continue
                    values.append(val)
            result_key = alias_name if alias_name else f"avg({col_name})"
            return result_key, (sum(values) / len(values)) if values else None
        elif func_name == "count":
            if col_name == "*" or col_name == "":
                # For count(*), use alias if available, otherwise use the function's generated name
                result_key = alias_name if alias_name else expr._generate_name()
                return result_key, len(group_rows)
            else:
                # For count(column), PySpark names it "count" not "count(column)" in some contexts
                # But we'll use the alias if provided, otherwise use count(column) format
                result_key = alias_name if alias_name else f"count({col_name})"
                return result_key, len(group_rows)
        elif func_name == "max":
            # Check if this is a complex expression (ColumnOperation)
            if hasattr(expr, "column") and hasattr(expr.column, "operation"):
                # Evaluate the expression for each row
                values = []
                for row_data in group_rows:
                    try:
                        from ...core.protocols import ColumnExpression  # noqa: TC001

                        expr_result = self.df._evaluate_column_expression(
                            row_data, cast("ColumnExpression", expr.column)
                        )
                        if expr_result is not None:
                            values.append(expr_result)
                    except (ValueError, TypeError, AttributeError):
                        pass
                result_key = alias_name if alias_name else f"max({col_name})"
                return result_key, max(values) if values else None
            else:
                # Simple column reference
                values = [
                    row.get(col_name)
                    for row in group_rows
                    if row.get(col_name) is not None
                ]
                result_key = alias_name if alias_name else f"max({col_name})"
                return result_key, max(values) if values else None
        elif func_name == "min":
            # Check if this is a complex expression (ColumnOperation)
            if hasattr(expr, "column") and hasattr(expr.column, "operation"):
                # Evaluate the expression for each row
                values = []
                for row_data in group_rows:
                    try:
                        from ...core.protocols import ColumnExpression  # noqa: TC001

                        expr_result = self.df._evaluate_column_expression(
                            row_data, cast("ColumnExpression", expr.column)
                        )
                        if expr_result is not None:
                            values.append(expr_result)
                    except (ValueError, TypeError, AttributeError):
                        pass
                result_key = alias_name if alias_name else f"min({col_name})"
                return result_key, min(values) if values else None
            else:
                # Simple column reference
                values = [
                    row.get(col_name)
                    for row in group_rows
                    if row.get(col_name) is not None
                ]
                result_key = alias_name if alias_name else f"min({col_name})"
                return result_key, min(values) if values else None
        elif func_name == "collect_list":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"collect_list({col_name})"
            return result_key, values
        elif func_name == "collect_set":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"collect_set({col_name})"
            return result_key, list(set(values))
        elif func_name == "first":
            ignorenulls = getattr(expr, "ignorenulls", False)
            if ignorenulls:
                # Filter out None values and return first non-null value
                values = [
                    row.get(col_name)
                    for row in group_rows
                    if row.get(col_name) is not None
                ]
                result_key = alias_name if alias_name else f"first({col_name})"
                return result_key, values[0] if values else None
            else:
                # Return first value even if it's None (default behavior)
                result_key = alias_name if alias_name else f"first({col_name})"
                if group_rows:
                    return result_key, group_rows[0].get(col_name)
                else:
                    return result_key, None
        elif func_name == "last":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"last({col_name})"
            return result_key, values[-1] if values else None
        elif func_name == "stddev" or func_name == "std":
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None
                and isinstance(row.get(col_name), (int, float))
            ]
            result_key = alias_name if alias_name else f"stddev({col_name})"
            # PySpark returns None (not 0.0) when there's only one value
            if len(values) <= 1:
                return result_key, None
            return result_key, statistics.stdev(values)
        elif func_name == "product":
            # product(col) - multiply all values
            values = []
            for row in group_rows:
                val = row.get(col_name)
                if val is not None:
                    if isinstance(val, bool):
                        val = 1 if val else 0
                    if isinstance(val, str):
                        try:
                            val = float(val) if "." in val else int(val)
                        except ValueError:
                            continue
                    values.append(val)
            result_key = alias_name if alias_name else f"product({col_name})"
            if values:
                product_result = 1.0
                for val in values:
                    product_result *= val
                return result_key, product_result
            else:
                return result_key, 1.0  # Empty set returns 1.0
        elif func_name == "sum_distinct":
            # sum_distinct(col) - sum of distinct values
            values = []
            seen = set()
            for row in group_rows:
                val = row.get(col_name)
                if val is not None:
                    if isinstance(val, bool):
                        val = 1 if val else 0
                    if isinstance(val, str):
                        try:
                            val = float(val) if "." in val else int(val)
                        except ValueError:
                            continue
                    # Only add if not seen before
                    if val not in seen:
                        seen.add(val)
                        values.append(val)
            result_key = alias_name if alias_name else f"sum_distinct({col_name})"
            return result_key, sum(values) if values else 0
        elif func_name == "variance":
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None
                and isinstance(row.get(col_name), (int, float))
            ]
            result_key = alias_name if alias_name else f"variance({col_name})"
            # PySpark returns None (not 0.0) when there's only one value
            if len(values) <= 1:
                return result_key, None
            return result_key, statistics.variance(values)
        elif func_name == "skewness":
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None
                and isinstance(row.get(col_name), (int, float))
            ]
            result_key = alias_name if alias_name else f"skewness({col_name})"
            if values and len(values) > 2:
                mean_val = statistics.mean(values)
                std_val = statistics.stdev(values)
                if std_val > 0:
                    skewness = sum((x - mean_val) ** 3 for x in values) / (
                        len(values) * std_val**3
                    )
                    return result_key, skewness
                else:
                    return result_key, 0.0
            else:
                return result_key, None
        elif func_name == "kurtosis":
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None
                and isinstance(row.get(col_name), (int, float))
            ]
            result_key = alias_name if alias_name else f"kurtosis({col_name})"
            if values and len(values) > 3:
                mean_val = statistics.mean(values)
                std_val = statistics.stdev(values)
                if std_val > 0:
                    kurtosis = (
                        sum((x - mean_val) ** 4 for x in values)
                        / (len(values) * std_val**4)
                        - 3
                    )
                    return result_key, kurtosis
                else:
                    return result_key, 0.0
            else:
                return result_key, None
        elif func_name == "bool_and":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"bool_and({col_name})"
            return result_key, all(values) if values else None
        elif func_name == "bool_or":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"bool_or({col_name})"
            return result_key, any(values) if values else None
        elif func_name == "max_by":
            # max_by(col, ord) - return col value where ord is maximum
            if expr.ord_column is None:
                return alias_name if alias_name else f"max_by({col_name})", None
            ord_col_name = (
                expr.ord_column.name
                if hasattr(expr.ord_column, "name")
                else str(expr.ord_column)
            )
            if group_rows:
                max_row = max(
                    group_rows, key=lambda r: r.get(ord_col_name, float("-inf"))
                )
                result_key = alias_name if alias_name else f"max_by({col_name})"
                return result_key, max_row.get(col_name)
            return alias_name if alias_name else f"max_by({col_name})", None
        elif func_name == "min_by":
            # min_by(col, ord) - return col value where ord is minimum
            if expr.ord_column is None:
                return alias_name if alias_name else f"min_by({col_name})", None
            ord_col_name = (
                expr.ord_column.name
                if hasattr(expr.ord_column, "name")
                else str(expr.ord_column)
            )
            if group_rows:
                min_row = min(
                    group_rows, key=lambda r: r.get(ord_col_name, float("inf"))
                )
                result_key = alias_name if alias_name else f"min_by({col_name})"
                return result_key, min_row.get(col_name)
            return alias_name if alias_name else f"min_by({col_name})", None
        elif func_name == "count_if":
            # count_if(condition) - count where condition is true
            # The column might be a condition expression (e.g., col > 20)
            if expr.column is not None and hasattr(expr.column, "operation"):
                # This is a condition expression - evaluate it for each row
                true_count = 0
                for row in group_rows:
                    # Evaluate the condition expression
                    cond_expr = expr.column
                    if (
                        hasattr(cond_expr, "column")
                        and hasattr(cond_expr, "operation")
                        and hasattr(cond_expr, "value")
                    ):
                        col_val = row.get(
                            cond_expr.column.name
                            if hasattr(cond_expr.column, "name")
                            else cond_expr.column
                        )
                        comp_val = (
                            cond_expr.value.value
                            if hasattr(cond_expr.value, "value")
                            else cond_expr.value
                        )

                        # Evaluate the condition based on the operation
                        if cond_expr.operation == ">":
                            if col_val is not None and col_val > comp_val:
                                true_count += 1
                        elif cond_expr.operation == "<":
                            if col_val is not None and col_val < comp_val:
                                true_count += 1
                        elif cond_expr.operation == ">=":
                            if col_val is not None and col_val >= comp_val:
                                true_count += 1
                        elif cond_expr.operation == "<=":
                            if col_val is not None and col_val <= comp_val:
                                true_count += 1
                        elif (
                            cond_expr.operation == "=="
                            and col_val is not None
                            and col_val == comp_val
                        ):
                            true_count += 1
                result_key = alias_name if alias_name else "count_if"
                return result_key, true_count
            else:
                # Simple boolean column
                values = [
                    row.get(col_name)
                    for row in group_rows
                    if row.get(col_name) is not None
                ]
                true_count = sum(
                    1 for v in values if v is True or v == 1 or str(v).lower() == "true"
                )
                result_key = alias_name if alias_name else f"count_if({col_name})"
                return result_key, true_count
        elif func_name == "any_value":
            # any_value(col) - return any non-null value (non-deterministic)
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"any_value({col_name})"
            return result_key, values[0] if values else None
        elif func_name == "mean":
            # mean(col) - alias for avg
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = (
                alias_name  # Use the name from expr.name (already set correctly)
            )
            return result_key, statistics.mean(values) if values else None
        elif func_name == "approx_count_distinct":
            # approx_count_distinct(col) - approximate distinct count
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            distinct_count = len(set(values))
            result_key = (
                alias_name if alias_name else f"approx_count_distinct({col_name})"
            )
            return result_key, distinct_count
        elif func_name == "countDistinct":
            # countDistinct(col) - exact distinct count
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            distinct_count = len(set(values))
            result_key = (
                alias_name  # Use the name from expr.name (already set correctly)
            )
            return result_key, distinct_count
        elif func_name == "stddev_pop":
            # stddev_pop(col) - population standard deviation
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"stddev_pop({col_name})"
            return result_key, statistics.pstdev(values) if len(values) > 0 else None
        elif func_name == "stddev_samp":
            # stddev_samp(col) - sample standard deviation
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"stddev_samp({col_name})"
            return result_key, statistics.stdev(values) if len(values) > 1 else None
        elif func_name == "var_pop":
            # var_pop(col) - population variance
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"var_pop({col_name})"
            return result_key, statistics.pvariance(values) if len(values) > 0 else None
        elif func_name == "var_samp":
            # var_samp(col) - sample variance
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"var_samp({col_name})"
            return result_key, statistics.variance(values) if len(values) > 1 else None
        elif func_name == "covar_pop":
            # covar_pop(col1, col2) - population covariance
            # Get both columns
            if hasattr(expr, "ord_column") and expr.ord_column is not None:
                col2_name = (
                    expr.ord_column.name
                    if hasattr(expr.ord_column, "name")
                    else str(expr.ord_column)
                )
                values1 = [
                    row.get(col_name)
                    for row in group_rows
                    if row.get(col_name) is not None and row.get(col2_name) is not None
                ]
                values2 = [
                    row.get(col2_name)
                    for row in group_rows
                    if row.get(col_name) is not None and row.get(col2_name) is not None
                ]

                if len(values1) > 0 and len(values2) > 0:
                    # Mypy has limitations with statistics.mean and list comprehensions
                    mean1 = statistics.mean(values1)  # type: ignore[type-var]
                    mean2 = statistics.mean(values2)  # type: ignore[type-var]
                    if mean1 is not None and mean2 is not None:
                        covar = sum(
                            (x1 - mean1) * (x2 - mean2)
                            for x1, x2 in zip(values1, values2)
                        ) / len(values1)
                    else:
                        covar = 0.0
                    result_key = (
                        alias_name
                        if alias_name
                        else f"covar_pop({col_name}, {col2_name})"
                    )
                    return result_key, covar
                else:
                    result_key = alias_name if alias_name else f"covar_pop({col_name})"
                    return result_key, None
            else:
                result_key = alias_name if alias_name else f"covar_pop({col_name})"
                return result_key, None
        elif func_name == "covar_samp":
            # covar_samp(col1, col2) - sample covariance (divide by n-1 instead of n)
            # Get both columns
            if hasattr(expr, "ord_column") and expr.ord_column is not None:
                col2_name = (
                    expr.ord_column.name
                    if hasattr(expr.ord_column, "name")
                    else str(expr.ord_column)
                )
                values1 = [
                    row.get(col_name)
                    for row in group_rows
                    if row.get(col_name) is not None and row.get(col2_name) is not None
                ]
                values2 = [
                    row.get(col2_name)
                    for row in group_rows
                    if row.get(col_name) is not None and row.get(col2_name) is not None
                ]

                if (
                    len(values1) > 1 and len(values2) > 1
                ):  # Need at least 2 points for sample covariance
                    # Mypy has limitations with statistics.mean and list comprehensions
                    mean1 = statistics.mean(values1)  # type: ignore[type-var]
                    mean2 = statistics.mean(values2)  # type: ignore[type-var]
                    if mean1 is not None and mean2 is not None:
                        # Sample covariance: divide by (n-1) instead of n
                        covar = sum(
                            (x1 - mean1) * (x2 - mean2)
                            for x1, x2 in zip(values1, values2)
                        ) / (len(values1) - 1)
                    else:
                        covar = 0.0
                    result_key = (
                        alias_name
                        if alias_name
                        else f"covar_samp({col_name}, {col2_name})"
                    )
                    return result_key, covar
                else:
                    result_key = alias_name if alias_name else f"covar_samp({col_name})"
                    return result_key, None
            else:
                result_key = alias_name if alias_name else f"covar_samp({col_name})"
                return result_key, None
        elif func_name == "corr":
            # corr(col1, col2) - correlation coefficient
            # Get both columns
            if hasattr(expr, "ord_column") and expr.ord_column is not None:
                col2_name = (
                    expr.ord_column.name
                    if hasattr(expr.ord_column, "name")
                    else str(expr.ord_column)
                )
                values1 = [
                    row.get(col_name)
                    for row in group_rows
                    if row.get(col_name) is not None and row.get(col2_name) is not None
                ]
                values2 = [
                    row.get(col2_name)
                    for row in group_rows
                    if row.get(col_name) is not None and row.get(col2_name) is not None
                ]

                if (
                    len(values1) > 1 and len(values2) > 1
                ):  # Need at least 2 points for correlation
                    # Mypy has limitations with statistics.mean and list comprehensions
                    mean1 = statistics.mean(values1)  # type: ignore[type-var]
                    mean2 = statistics.mean(values2)  # type: ignore[type-var]
                    if mean1 is not None and mean2 is not None:
                        # Calculate covariance
                        covar = sum(
                            (x1 - mean1) * (x2 - mean2)
                            for x1, x2 in zip(values1, values2)
                        ) / (len(values1) - 1)

                        # Calculate standard deviations
                        var1 = sum((x1 - mean1) ** 2 for x1 in values1) / (
                            len(values1) - 1
                        )
                        var2 = sum((x2 - mean2) ** 2 for x2 in values2) / (
                            len(values2) - 1
                        )
                        std1 = var1**0.5 if var1 > 0 else 0.0
                        std2 = var2**0.5 if var2 > 0 else 0.0

                        # Correlation = covariance / (std1 * std2)
                        if std1 > 0 and std2 > 0:
                            corr = covar / (std1 * std2)
                        else:
                            corr = 0.0 if len(values1) > 0 else None
                    else:
                        corr = 0.0
                    result_key = (
                        alias_name if alias_name else f"corr({col_name}, {col2_name})"
                    )
                    return result_key, corr
                else:
                    result_key = alias_name if alias_name else f"corr({col_name})"
                    return result_key, None
            else:
                result_key = alias_name if alias_name else f"corr({col_name})"
                return result_key, None
        elif func_name in [
            "regr_avgx",
            "regr_avgy",
            "regr_count",
            "regr_intercept",
            "regr_r2",
            "regr_slope",
            "regr_sxx",
            "regr_sxy",
            "regr_syy",
        ]:
            # Linear regression functions - require two columns (y, x)
            # The expr.column is a ColumnOperation with y as base and x as value
            column_expr = getattr(expr, "column", None)
            column_operation = getattr(column_expr, "operation", None)
            if column_operation == func_name:
                # Extract y and x columns from the ColumnOperation
                y_col = getattr(column_expr, "column", None)
                x_col = getattr(column_expr, "value", None)
                if y_col is None or x_col is None:
                    result_key = (
                        alias_name if alias_name else f"{func_name}({col_name})"
                    )
                    return result_key, None

                y_col_name = y_col.name if hasattr(y_col, "name") else str(y_col)
                x_col_name = x_col.name if hasattr(x_col, "name") else str(x_col)

                # Get pairs of (y, x) values where both are not None
                cleaned_pairs: List[Tuple[float, float]] = []
                for row in group_rows:
                    y_raw = row.get(y_col_name)
                    x_raw = row.get(x_col_name)
                    if y_raw is None or x_raw is None:
                        continue
                    try:
                        cleaned_pairs.append((float(y_raw), float(x_raw)))
                    except (TypeError, ValueError):
                        continue

                if not cleaned_pairs:
                    result_key = (
                        alias_name
                        if alias_name
                        else f"{func_name}({y_col_name}, {x_col_name})"
                    )
                    return result_key, None

                y_values: List[float] = [pair[0] for pair in cleaned_pairs]
                x_values: List[float] = [pair[1] for pair in cleaned_pairs]
                n = len(cleaned_pairs)

                # Calculate basic statistics
                y_mean = statistics.mean(y_values) if y_values else 0.0
                x_mean = statistics.mean(x_values) if x_values else 0.0

                # Calculate regression statistics
                sxx = sum((x - x_mean) ** 2 for x in x_values)
                syy = sum((y - y_mean) ** 2 for y in y_values)
                sxy = sum(
                    (x - x_mean) * (y - y_mean) for x, y in zip(x_values, y_values)
                )

                result_key = (
                    alias_name
                    if alias_name
                    else f"{func_name}({y_col_name}, {x_col_name})"
                )

                if func_name == "regr_avgx":
                    return result_key, x_mean
                elif func_name == "regr_avgy":
                    return result_key, y_mean
                elif func_name == "regr_count":
                    return result_key, n
                elif func_name == "regr_sxx":
                    return result_key, sxx
                elif func_name == "regr_syy":
                    return result_key, syy
                elif func_name == "regr_sxy":
                    return result_key, sxy
                elif func_name == "regr_slope":
                    # slope = sxy / sxx
                    if sxx != 0:
                        return result_key, sxy / sxx
                    else:
                        return result_key, None
                elif func_name == "regr_intercept":
                    # intercept = y_mean - slope * x_mean
                    if sxx != 0:
                        slope = sxy / sxx
                        intercept = y_mean - slope * x_mean
                        return result_key, intercept
                    else:
                        return result_key, None
                elif func_name == "regr_r2":
                    # R-squared = (sxy^2) / (sxx * syy)
                    if sxx != 0 and syy != 0:
                        r2 = (sxy**2) / (sxx * syy)
                        return result_key, r2
                    else:
                        return result_key, None
            else:
                result_key = alias_name if alias_name else f"{func_name}({col_name})"
                return result_key, None
        elif func_name == "approx_percentile":
            # approx_percentile(col, percentage, accuracy)
            percentage = 0.5

            # Values of the column
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]

            if not values:
                result_key = (
                    alias_name if alias_name else f"approx_percentile({col_name})"
                )
                return result_key, None

            # Optional additional parameters from ColumnOperation
            column_expr = getattr(expr, "column", None)
            column_operation = getattr(column_expr, "operation", None)
            operation_value = getattr(column_operation, "value", None)
            if isinstance(operation_value, tuple) and len(operation_value) >= 1:
                first_arg = operation_value[0]
                if isinstance(first_arg, (int, float)):
                    percentage = float(first_arg)

            # Sort values for percentile calculation
            values.sort()
            n = len(values)
            # Calculate approximate percentile using linear interpolation
            index = percentage * (n - 1)
            lower_idx = int(index)
            upper_idx = min(lower_idx + 1, n - 1)
            fraction = index - lower_idx

            if lower_idx == upper_idx:
                percentile_value = values[lower_idx]
            else:
                percentile_value = (
                    values[lower_idx] * (1 - fraction) + values[upper_idx] * fraction
                )

            result_key = (
                alias_name
                if alias_name
                else f"approx_percentile({col_name}, {percentage})"
            )
            return result_key, percentile_value
        else:
            result_key = alias_name if alias_name else f"{func_name}({col_name})"
            return result_key, None

        return alias_name if alias_name else expr.name, None

    def _evaluate_column_expression(
        self,
        expr: Union[Column, ColumnOperation],
        group_rows: List[Dict[str, Any]],
    ) -> Tuple[str, Any]:
        """Evaluate Column or ColumnOperation.

        Args:
            expr: Column expression to evaluate.
            group_rows: Rows in the group.

        Returns:
            Tuple of (result_key, result_value).
        """
        # Check if it's a ColumnOperation with an operation
        if isinstance(expr, ColumnOperation) and hasattr(expr, "operation"):
            operation = expr.operation
            # Check if the column is an AggregateFunction or ColumnOperation wrapping one
            # (arithmetic on aggregate functions, e.g., F.countDistinct() - 1)
            agg_func = None
            is_reverse = False  # Track if this is a reverse operation (e.g., 10 - F.countDistinct())

            if isinstance(expr.column, AggregateFunction):
                agg_func = expr.column
            elif (
                isinstance(expr.column, ColumnOperation)
                and hasattr(expr.column, "_aggregate_function")
                and expr.column._aggregate_function is not None
            ):
                # ColumnOperation wrapping an AggregateFunction (e.g., F.count())
                agg_func = expr.column._aggregate_function
            elif isinstance(expr.value, AggregateFunction):
                # Reverse operation: literal - aggregate function (e.g., 10 - F.countDistinct())
                agg_func = expr.value
                is_reverse = True
            elif (
                isinstance(expr.value, ColumnOperation)
                and hasattr(expr.value, "_aggregate_function")
                and expr.value._aggregate_function is not None
            ):
                # Reverse operation: literal - ColumnOperation wrapping AggregateFunction
                agg_func = expr.value._aggregate_function
                is_reverse = True
            elif isinstance(expr.column, ColumnOperation) and operation in (
                "+",
                "-",
                "*",
                "/",
                "%",
            ):
                # Nested ColumnOperation (e.g., (F.countDistinct() - 1) * 2)
                # Recursively evaluate the nested expression first
                nested_key, nested_value = self._evaluate_column_expression(
                    expr.column, group_rows
                )
                if nested_value is not None:
                    # Get the right operand value
                    from ...functions.core.literals import Literal

                    if isinstance(expr.value, Literal) or hasattr(expr.value, "value"):
                        right_value = expr.value.value
                    else:
                        right_value = expr.value

                    # Apply the operation
                    if operation == "+":
                        result_value = nested_value + right_value
                    elif operation == "-":
                        result_value = nested_value - right_value
                    elif operation == "*":
                        result_value = nested_value * right_value
                    elif operation == "/":
                        result_value = (
                            nested_value / right_value if right_value != 0 else None
                        )
                    elif operation == "%":
                        result_value = (
                            nested_value % right_value if right_value != 0 else None
                        )
                    else:
                        result_value = None

                    result_key = (
                        expr.name
                        if hasattr(expr, "name")
                        else f"({nested_key} {operation} {right_value})"
                    )
                    return result_key, result_value

            if agg_func is not None and operation in ("+", "-", "*", "/", "%"):
                # Evaluate the aggregate function first
                _, agg_result = self._evaluate_aggregate_function(agg_func, group_rows)
                # Then apply the arithmetic operation
                from ...functions.core.literals import Literal

                # Get the other operand value (left for reverse, right for forward)
                if is_reverse:
                    # For reverse operations, the left operand is in expr.column
                    if isinstance(expr.column, Literal) or hasattr(
                        expr.column, "value"
                    ):
                        left_value = expr.column.value
                    else:
                        left_value = expr.column
                else:
                    # For forward operations, the right operand is in expr.value
                    if isinstance(expr.value, Literal) or hasattr(expr.value, "value"):
                        right_value = expr.value.value
                    else:
                        right_value = expr.value

                # Apply the operation
                if operation == "+":
                    if is_reverse:
                        result_value = (
                            left_value + agg_result if agg_result is not None else None
                        )
                    else:
                        result_value = (
                            agg_result + right_value if agg_result is not None else None
                        )
                elif operation == "-":
                    if is_reverse:
                        result_value = (
                            left_value - agg_result if agg_result is not None else None
                        )
                    else:
                        result_value = (
                            agg_result - right_value if agg_result is not None else None
                        )
                elif operation == "*":
                    if is_reverse:
                        result_value = (
                            left_value * agg_result if agg_result is not None else None
                        )
                    else:
                        result_value = (
                            agg_result * right_value if agg_result is not None else None
                        )
                elif operation == "/":
                    if is_reverse:
                        result_value = (
                            left_value / agg_result
                            if agg_result is not None and agg_result != 0
                            else None
                        )
                    else:
                        result_value = (
                            agg_result / right_value
                            if agg_result is not None and right_value != 0
                            else None
                        )
                elif operation == "%":
                    if is_reverse:
                        result_value = (
                            left_value % agg_result
                            if agg_result is not None and agg_result != 0
                            else None
                        )
                    else:
                        result_value = (
                            agg_result % right_value
                            if agg_result is not None and right_value != 0
                            else None
                        )
                else:
                    result_value = None

                # Generate result key name
                if is_reverse:
                    result_key = (
                        expr.name
                        if hasattr(expr, "name")
                        else f"({left_value} {operation} {agg_func.name})"
                    )
                else:
                    result_key = (
                        expr.name
                        if hasattr(expr, "name")
                        else f"({agg_func.name} {operation} {right_value})"
                    )
                return result_key, result_value
            elif operation == "count":
                # Count non-null values in the column
                col_name = (
                    expr.column.name
                    if hasattr(expr.column, "name")
                    else str(expr.column)
                )
                count_value = sum(
                    1 for row in group_rows if row.get(col_name) is not None
                )
                return expr.name, count_value
            elif operation == "sum":
                col_name = (
                    expr.column.name
                    if hasattr(expr.column, "name")
                    else str(expr.column)
                )
                values = [
                    row.get(col_name, 0)
                    for row in group_rows
                    if row.get(col_name) is not None
                ]
                return expr.name, sum(values) if values else 0
            elif operation == "avg":
                col_name = (
                    expr.column.name
                    if hasattr(expr.column, "name")
                    else str(expr.column)
                )
                values = [
                    row.get(col_name, 0)
                    for row in group_rows
                    if row.get(col_name) is not None
                ]
                return expr.name, sum(values) / len(values) if values else 0
            elif operation == "max":
                col_name = (
                    expr.column.name
                    if hasattr(expr.column, "name")
                    else str(expr.column)
                )
                values = [
                    row.get(col_name)
                    for row in group_rows
                    if row.get(col_name) is not None
                ]
                return expr.name, max(values) if values else None
            elif operation == "min":
                col_name = (
                    expr.column.name
                    if hasattr(expr.column, "name")
                    else str(expr.column)
                )
                values = [
                    row.get(col_name)
                    for row in group_rows
                    if row.get(col_name) is not None
                ]
                return expr.name, min(values) if values else None

        # Fallback to name-based parsing for string expressions
        expr_name = expr.name
        if expr_name.startswith("sum("):
            col_name = expr_name[4:-1]
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            return expr_name, sum(values) if values else 0
        elif expr_name.startswith("avg("):
            col_name = expr_name[4:-1]
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            return expr_name, sum(values) / len(values) if values else 0
        elif expr_name.startswith("count("):
            # Extract column name from count(column_name)
            col_name = expr_name[6:-1]  # Remove "count(" and ")"
            # Count non-null values in the specified column
            count_value = sum(1 for row in group_rows if row.get(col_name) is not None)
            return expr_name, count_value
        elif expr_name.startswith("max("):
            col_name = expr_name[4:-1]
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr_name, max(values) if values else None
        elif expr_name.startswith("min("):
            col_name = expr_name[4:-1]
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr_name, min(values) if values else None
        else:
            return expr_name, None

    def sum(self, *columns: Union[str, Column]) -> "DataFrame":
        """Sum grouped data.

        Args:
            *columns: Columns to sum.

        Returns:
            DataFrame with sum aggregations.
        """
        if not columns:
            return self.agg("sum(1)")

        exprs = [
            f"sum({col})" if isinstance(col, str) else f"sum({col.name})"
            for col in columns
        ]
        return self.agg(*exprs)

    def avg(self, *columns: Union[str, Column]) -> "DataFrame":
        """Average grouped data.

        Args:
            *columns: Columns to average.

        Returns:
            DataFrame with average aggregations.
        """
        if not columns:
            return self.agg("avg(1)")

        exprs = [
            f"avg({col})" if isinstance(col, str) else f"avg({col.name})"
            for col in columns
        ]
        return self.agg(*exprs)

    def mean(self, *columns: Union[str, Column]) -> "DataFrame":
        """Mean grouped data (alias for avg).

        Args:
            *columns: Columns to get mean of.

        Returns:
            DataFrame with mean aggregations.

        Example:
            >>> df.groupBy("Name").mean("Value")
        """
        # mean() is an alias for avg() in PySpark
        return self.avg(*columns)

    def count(self, *columns: Union[str, Column]) -> "DataFrame":
        """Count grouped data.

        Args:
            *columns: Columns to count.

        Returns:
            DataFrame with count aggregations.
        """
        if not columns:
            # AggregateFunctions.count() returns ColumnOperation (PySpark-compatible)
            # which wraps AggregateFunction internally
            from ...functions.aggregate import AggregateFunctions

            return self.agg(AggregateFunctions.count())

        exprs = [
            f"count({col})" if isinstance(col, str) else f"count({col.name})"
            for col in columns
        ]
        return self.agg(*exprs)

    def max(self, *columns: Union[str, Column]) -> "DataFrame":
        """Max grouped data.

        Args:
            *columns: Columns to get max of.

        Returns:
            DataFrame with max aggregations.
        """
        if not columns:
            return self.agg("max(1)")

        exprs = [
            f"max({col})" if isinstance(col, str) else f"max({col.name})"
            for col in columns
        ]
        return self.agg(*exprs)

    def min(self, *columns: Union[str, Column]) -> "DataFrame":
        """Min grouped data.

        Args:
            *columns: Columns to get min of.

        Returns:
            DataFrame with min aggregations.
        """
        if not columns:
            return self.agg("min(1)")

        exprs = [
            f"min({col})" if isinstance(col, str) else f"min({col.name})"
            for col in columns
        ]
        return self.agg(*exprs)

    def count_distinct(self, *columns: Union[str, Column]) -> "DataFrame":
        """Count distinct values in columns.

        Args:
            *columns: Columns to count distinct values for.

        Returns:
            DataFrame with count distinct results.
        """
        from ...functions import count_distinct

        exprs = []
        for col in columns:
            if isinstance(col, Column):
                exprs.append(count_distinct(col))
            else:
                exprs.append(count_distinct(col))

        return self.agg(*exprs)

    def collect_set(self, *columns: Union[str, Column]) -> "DataFrame":
        """Collect unique values into a set.

        Args:
            *columns: Columns to collect unique values for.

        Returns:
            DataFrame with collect_set results.
        """
        from ...functions import collect_set

        exprs = []
        for col in columns:
            if isinstance(col, Column):
                exprs.append(collect_set(col))
            else:
                exprs.append(collect_set(col))

        return self.agg(*exprs)

    def first(self, *columns: Union[str, Column]) -> "DataFrame":
        """Get first value in each group.

        Args:
            *columns: Columns to get first values for.

        Returns:
            DataFrame with first values.
        """
        from ...functions import first

        exprs = []
        for col in columns:
            if isinstance(col, Column):
                exprs.append(first(col))
            else:
                exprs.append(first(col))

        return self.agg(*exprs)

    def last(self, *columns: Union[str, Column]) -> "DataFrame":
        """Get last value in each group.

        Args:
            *columns: Columns to get last values for.

        Returns:
            DataFrame with last values.
        """
        from ...functions import last

        exprs = []
        for col in columns:
            if isinstance(col, Column):
                exprs.append(last(col))
            else:
                exprs.append(last(col))

        return self.agg(*exprs)

    def stddev(self, *columns: Union[str, Column]) -> "DataFrame":
        """Calculate standard deviation.

        Args:
            *columns: Columns to calculate standard deviation for.

        Returns:
            DataFrame with standard deviation results.
        """
        from ...functions import stddev

        exprs = []
        for col in columns:
            if isinstance(col, Column):
                exprs.append(stddev(col))
            else:
                exprs.append(stddev(col))

        return self.agg(*exprs)

    def variance(self, *columns: Union[str, Column]) -> "DataFrame":
        """Calculate variance.

        Args:
            *columns: Columns to calculate variance for.

        Returns:
            DataFrame with variance results.
        """
        from ...functions import variance

        exprs = []
        for col in columns:
            if isinstance(col, Column):
                exprs.append(variance(col))
            else:
                exprs.append(variance(col))

        return self.agg(*exprs)

    def rollup(self, *columns: Union[str, Column]) -> "RollupGroupedData":
        """Create rollup grouped data for hierarchical grouping.

        Args:
            *columns: Columns to rollup.

        Returns:
            RollupGroupedData for hierarchical grouping.
        """
        from .rollup import RollupGroupedData

        col_names = []
        for col in columns:
            if isinstance(col, Column):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist and resolve case-insensitively
        from ...core.column_resolver import ColumnResolver

        available_cols = [field.name for field in self.df.schema.fields]
        case_sensitive = (
            self.df._is_case_sensitive()
            if hasattr(self.df, "_is_case_sensitive")
            else True
        )
        resolved_col_names = []
        for col_name in col_names:
            resolved_col = ColumnResolver.resolve_column_name(
                col_name, available_cols, case_sensitive
            )
            if resolved_col is None:
                raise AnalysisException(f"Column '{col_name}' does not exist")
            resolved_col_names.append(resolved_col)

        return RollupGroupedData(self.df, resolved_col_names)

    def cube(self, *columns: Union[str, Column]) -> "CubeGroupedData":
        """Create cube grouped data for multi-dimensional grouping.

        Args:
            *columns: Columns to cube.

        Returns:
            CubeGroupedData for multi-dimensional grouping.
        """
        from .cube import CubeGroupedData

        col_names = []
        for col in columns:
            if isinstance(col, Column):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist and resolve case-insensitively
        from ...core.column_resolver import ColumnResolver

        available_cols = [field.name for field in self.df.schema.fields]
        case_sensitive = (
            self.df._is_case_sensitive()
            if hasattr(self.df, "_is_case_sensitive")
            else True
        )
        resolved_col_names = []
        for col_name in col_names:
            resolved_col = ColumnResolver.resolve_column_name(
                col_name, available_cols, case_sensitive
            )
            if resolved_col is None:
                raise AnalysisException(f"Column '{col_name}' does not exist")
            resolved_col_names.append(resolved_col)

        return CubeGroupedData(self.df, resolved_col_names)

    def pivot(
        self, pivot_col: str, values: Optional[List[Any]] = None
    ) -> "PivotGroupedData":
        """Create pivot grouped data.

        Args:
            pivot_col: Column to pivot on.
            values: Optional list of pivot values. If None, uses all unique values.

        Returns:
            PivotGroupedData for pivot operations.
        """
        from .pivot import PivotGroupedData

        # Validate that pivot column exists and resolve case-insensitively
        from ...core.column_resolver import ColumnResolver

        available_cols = [field.name for field in self.df.schema.fields]
        case_sensitive = (
            self.df._is_case_sensitive()
            if hasattr(self.df, "_is_case_sensitive")
            else True
        )
        resolved_pivot_col = ColumnResolver.resolve_column_name(
            pivot_col, available_cols, case_sensitive
        )
        if resolved_pivot_col is None:
            raise AnalysisException(f"Column '{pivot_col}' does not exist")

        # If values not provided, get unique values from pivot column
        if values is None:
            values = list(
                {
                    row.get(resolved_pivot_col)
                    for row in self.df.data
                    if row.get(resolved_pivot_col) is not None
                }
            )
            values.sort()  # Sort for consistent ordering

        return PivotGroupedData(self.df, self.group_columns, resolved_pivot_col, values)

    def applyInPandas(self, func: Any, schema: Any) -> "DataFrame":
        """Apply a Python native function to each group using pandas DataFrames.

        The function should take a pandas DataFrame and return a pandas DataFrame.
        For each group, the group data is passed as a pandas DataFrame to the function
        and the returned pandas DataFrame is used to construct the output rows.

        Args:
            func: A function that takes a pandas DataFrame and returns a pandas DataFrame.
            schema: The schema of the output DataFrame (StructType or DDL string).

        Returns:
            DataFrame: Result of applying the function to each group.

        Example:
            >>> def normalize(pdf):
            ...     pdf['normalized'] = (pdf['value'] - pdf['value'].mean()) / pdf['value'].std()
            ...     return pdf
            >>> df.groupBy("category").applyInPandas(normalize, schema="category string, value double, normalized double")
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for applyInPandas. "
                "Install it with: pip install 'sparkless[pandas]'"
            )

        # Materialize DataFrame if lazy
        df = self.df._materialize_if_lazy() if self.df._operations_queue else self.df

        # Group data by group columns
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        for row in df.data:
            group_key = tuple(row.get(col) for col in self.group_columns)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row)

        # Apply function to each group
        result_pdfs = []
        for group_rows in groups.values():
            # Convert group to pandas DataFrame
            group_pdf = pd.DataFrame(group_rows)

            # Apply function
            result_pdf = func(group_pdf)

            if not isinstance(result_pdf, pd.DataFrame):
                raise TypeError(
                    f"Function must return a pandas DataFrame, got {type(result_pdf).__name__}"
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
        from ...spark_types import StructType
        from ...core.schema_inference import infer_schema_from_data

        result_schema: StructType
        if isinstance(schema, str):
            # For DDL string, use schema inference from result data
            # (DDL parsing is complex, so we rely on inference for now)
            result_schema = (
                infer_schema_from_data(result_data) if result_data else self.df.schema
            )
        elif isinstance(schema, StructType):
            result_schema = schema
        else:
            # Try to infer schema from result data
            result_schema = (
                infer_schema_from_data(result_data) if result_data else self.df.schema
            )

        from ..dataframe import DataFrame as MDF

        storage: Any = getattr(self.df, "storage", None)
        return MDF(result_data, result_schema, storage)

    def transform(self, func: Any) -> "DataFrame":
        """Apply a function to each group and return a DataFrame with the same schema.

        This is similar to applyInPandas but preserves the original schema.
        The function should take a pandas DataFrame and return a pandas DataFrame
        with the same columns (though it may add computed columns).

        Args:
            func: A function that takes a pandas DataFrame and returns a pandas DataFrame.

        Returns:
            DataFrame: Result of applying the function to each group.

        Example:
            >>> def add_group_stats(pdf):
            ...     pdf['group_mean'] = pdf['value'].mean()
            ...     pdf['group_std'] = pdf['value'].std()
            ...     return pdf
            >>> df.groupBy("category").transform(add_group_stats)
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for transform. "
                "Install it with: pip install 'sparkless[pandas]'"
            )

        # Materialize DataFrame if lazy
        df = self.df._materialize_if_lazy() if self.df._operations_queue else self.df

        # Group data by group columns
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        group_indices: Dict[Any, List[int]] = {}  # Track original indices

        for idx, row in enumerate(df.data):
            group_key = tuple(row.get(col) for col in self.group_columns)
            if group_key not in groups:
                groups[group_key] = []
                group_indices[group_key] = []
            groups[group_key].append(row)
            group_indices[group_key].append(idx)

        # Apply function to each group and preserve order
        result_rows: List[Dict[str, Any]] = [{}] * len(df.data)

        for group_key, group_rows in groups.items():
            # Convert group to pandas DataFrame
            group_pdf = pd.DataFrame(group_rows)

            # Apply function
            transformed_pdf = func(group_pdf)

            if not isinstance(transformed_pdf, pd.DataFrame):
                raise TypeError(
                    f"Function must return a pandas DataFrame, got {type(transformed_pdf).__name__}"
                )

            # Put transformed rows back in their original positions
            transformed_rows = transformed_pdf.to_dict("records")
            for idx, transformed_row in zip(group_indices[group_key], transformed_rows):
                # Convert hashable keys to strings for type safety
                result_rows[idx] = {str(k): v for k, v in transformed_row.items()}

        # Use the same schema as the original DataFrame
        # (or extend it if new columns were added)
        from ...core.schema_inference import infer_schema_from_data

        result_schema = (
            infer_schema_from_data(result_rows) if result_rows else df.schema
        )

        from ..dataframe import DataFrame as MDF

        storage: Any = getattr(self.df, "storage", None)
        return MDF(result_rows, result_schema, storage)
