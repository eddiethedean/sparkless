"""
Simple Delta Lake support for Sparkless.

Provides minimal Delta Lake API compatibility by wrapping regular tables.
Good enough for basic Delta tests without requiring the delta-spark library.

Usage:
    # Create table normally
    df.write.saveAsTable("schema.table")

    # Access as Delta
    dt = DeltaTable.forName(spark, "schema.table")
    df = dt.toDF()

    # Mock operations (don't actually execute)
    dt.delete("id < 10")  # No-op
    dt.merge(source, "condition").execute()  # No-op

For real Delta operations (MERGE, time travel, etc.), use real PySpark + delta-spark.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING, Tuple, Union, cast

if TYPE_CHECKING:
    from .dataframe import DataFrame
    from .spark_types import StructType

from .functions import Column
from .functions.base import ColumnOperation
from .core.condition_evaluator import ConditionEvaluator
from .core.safe_evaluator import SafeExpressionEvaluator
from .spark_types import get_row_value

logger = logging.getLogger(__name__)

# Exceptions that indicate expression evaluation failed; fall back to literal.
_EVALUATION_FAILURE_EXCEPTIONS = (ValueError, SyntaxError, KeyError, TypeError)


def _normalize_boolean_expression(expression: str) -> str:
    """Convert SQL-style logical expression into Python-compatible syntax."""
    expr = expression
    expr = re.sub(r"\bAND\b", "and", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bOR\b", "or", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bNOT\b", "not", expr, flags=re.IGNORECASE)
    expr = re.sub(r"(?<![<>!=])=(?!=)", "==", expr)
    return expr


def _build_eval_context(
    target_row: Dict[str, Any],
    target_aliases: Set[str],
    source_row: Optional[Dict[str, Any]] = None,
    source_aliases: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """Create evaluation context with target/source aliases for expression eval."""
    context: Dict[str, Any] = dict(target_row)
    target_ns = SimpleNamespace(**target_row)
    for alias in target_aliases:
        context[alias] = target_ns

    if source_row is not None and source_aliases:
        source_ns = SimpleNamespace(**source_row)
        for alias in source_aliases:
            context[alias] = source_ns
        # Also expose raw source row values with alias prefix for clarity
        for key, value in source_row.items():
            context[f"{list(source_aliases)[0]}_{key}"] = value

    return context


class DeltaTable:
    """
    Simple DeltaTable wrapper for basic Delta Lake compatibility.

    Just wraps existing tables - doesn't implement real Delta features.
    Sufficient for tests that check Delta API exists and can be called.
    """

    def __init__(self, spark_session: Any, table_name: str):
        """Initialize DeltaTable wrapper."""
        self._spark = spark_session
        self._table_name = table_name

    @classmethod
    def forName(cls, spark_session: Any, table_name: str) -> DeltaTable:
        """
        Get DeltaTable for existing table.

        Usage:
            df.write.saveAsTable("schema.table")
            dt = DeltaTable.forName(spark, "schema.table")
        """
        # Import here to avoid circular imports
        from .core.exceptions.analysis import AnalysisException

        # Parse table name
        if "." in table_name:
            schema, table = table_name.split(".", 1)
        else:
            schema, table = "default", table_name

        # Check table exists (only for SparkSession)
        if hasattr(
            spark_session, "_storage"
        ) and not spark_session._storage.table_exists(schema, table):
            raise AnalysisException(f"Table or view not found: {table_name}")
        # For real SparkSession, we'll just assume the table exists
        # and let it fail naturally if it doesn't

        return cls(spark_session, table_name)

    @classmethod
    def forPath(cls, spark_session: Any, path: str) -> DeltaTable:
        """Get DeltaTable by path (mock - treats path as table name)."""
        table_name = path.split("/")[-1] if "/" in path else path
        return cls(spark_session, f"default.{table_name}")

    def toDF(self) -> DataFrame:
        """Get DataFrame from Delta table."""

        return cast("DataFrame", self._spark.table(self._table_name))

    def alias(self, alias: str) -> DeltaTable:
        """Alias table (returns self for chaining)."""
        self._alias = alias
        return self

    # Mock operations - don't actually execute
    def delete(self, condition: Union[str, Column, None] = None) -> None:
        """Delete rows matching the given condition."""
        rows = self._load_table_rows()
        schema = self._current_schema()
        alias = getattr(self, "_alias", "target")

        if not condition and condition is not False:  # type: ignore[comparison-overlap]
            remaining_rows: List[Dict[str, Any]] = []
        elif isinstance(condition, (Column, ColumnOperation)):
            remaining_rows = [
                row
                for row in rows
                if not ConditionEvaluator.evaluate_condition(row, condition)
            ]
        else:
            normalized = _normalize_boolean_expression(condition)
            remaining_rows = [
                row
                for row in rows
                if not self._evaluate_row_condition(normalized, row, alias)
            ]

        new_df = self._spark.createDataFrame(remaining_rows, schema)
        self._overwrite_table(new_df)

    def update(
        self,
        condition: Union[str, Column, None] = None,
        set_values: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Update rows that satisfy condition with provided assignments."""
        if not set_values:
            return

        rows = self._load_table_rows()
        schema = self._current_schema()
        alias = getattr(self, "_alias", "target")
        use_column_condition = isinstance(condition, (Column, ColumnOperation))
        normalized_condition = (
            _normalize_boolean_expression(str(condition))
            if condition and not use_column_condition
            else None
        )

        updated_rows: List[Dict[str, Any]] = []
        for row in rows:
            if use_column_condition:
                should_update = bool(
                    ConditionEvaluator.evaluate_condition(row, condition)
                )
            elif normalized_condition is None:
                should_update = True
            else:
                should_update = self._evaluate_row_condition(
                    normalized_condition, row, alias
                )

            if not should_update:
                updated_rows.append(row)
                continue

            new_row = dict(row)
            for column_name, expression in set_values.items():
                if isinstance(expression, (Column, ColumnOperation)) or (
                    hasattr(expression, "value") and not isinstance(expression, str)
                ):
                    new_row[column_name] = ConditionEvaluator._get_column_value(
                        new_row, expression
                    )
                else:
                    new_row[column_name] = self._evaluate_update_expression(
                        expression, new_row, alias
                    )
            updated_rows.append(new_row)

        new_df = self._spark.createDataFrame(updated_rows, schema)
        self._overwrite_table(new_df)

    def merge(self, source: Any, condition: str) -> DeltaMergeBuilder:
        """Mock merge (returns builder for chaining)."""
        return DeltaMergeBuilder(
            self,
            source,
            condition,
            getattr(self, "_alias", None),
        )

    def vacuum(self, retention_hours: Union[float, None] = None) -> None:
        """Mock vacuum (no-op)."""
        pass

    def optimize(self) -> DeltaOptimizeBuilder:
        """
        Mock OPTIMIZE operation.

        In real Delta Lake, this compacts small files.
        For testing, returns a builder that supports executeCompaction() and zOrderBy().

        Returns:
            DeltaOptimizeBuilder for method chaining
        """
        return DeltaOptimizeBuilder(self)

    def detail(self) -> DataFrame:
        """
        Mock table detail information.

        Returns a DataFrame with table metadata.

        Returns:
            DataFrame with table details
        """
        from .spark_types import (
            StructType,
            StructField,
            StringType,
            LongType,
            ArrayType,
            MapType,
        )
        from .dataframe import DataFrame

        # Create mock table details
        details: List[Dict[str, Any]] = [
            {
                "format": "delta",
                "id": f"mock-table-{hash(self._table_name)}",
                "name": self._table_name,
                "description": None,
                "location": f"/mock/delta/{self._table_name.replace('.', '/')}",
                "createdAt": "2024-01-01T00:00:00.000+0000",
                "lastModified": "2024-01-01T00:00:00.000+0000",
                "partitionColumns": [],
                "numFiles": 1,
                "sizeInBytes": 1024,
                "properties": {},
                "minReaderVersion": 1,
                "minWriterVersion": 2,
            }
        ]

        schema = StructType(
            [
                StructField("format", StringType()),
                StructField("id", StringType()),
                StructField("name", StringType()),
                StructField("description", StringType()),
                StructField("location", StringType()),
                StructField("createdAt", StringType()),
                StructField("lastModified", StringType()),
                StructField("partitionColumns", ArrayType(StringType())),
                StructField("numFiles", LongType()),
                StructField("sizeInBytes", LongType()),
                StructField("properties", MapType(StringType(), StringType())),
                StructField("minReaderVersion", LongType()),
                StructField("minWriterVersion", LongType()),
            ]
        )

        # Handle PySpark sessions (which don't have _storage)
        # Check if this is a real PySpark session by checking the module name
        is_pyspark = (
            not hasattr(self._spark, "_storage")
            and hasattr(self._spark, "__class__")
            and "pyspark" in str(self._spark.__class__.__module__)
        )

        if is_pyspark:
            # This is a real PySpark session
            # Note: In PySpark mode, users should use delta.tables.DeltaTable directly
            # For our mock DeltaTable, we'll return a PySpark DataFrame with mock data
            # to avoid recursion issues and maintain compatibility
            from typing import cast

            return cast("DataFrame", self._spark.createDataFrame(details, schema))

        # Mock SparkSession - use our storage
        return DataFrame(details, schema, self._spark._storage)

    def history(self, limit: Union[int, None] = None) -> DataFrame:
        """
        Mock table history.

        Returns a DataFrame with table version history.

        Args:
            limit: Optional limit on number of versions to return

        Returns:
            DataFrame with version history
        """
        from .spark_types import (
            StructType,
            StructField,
            StringType,
            LongType,
            MapType,
        )
        from .dataframe import DataFrame

        # Create mock history
        history = [
            {
                "version": 0,
                "timestamp": "2024-01-01T00:00:00.000+0000",
                "userId": "mock_user",
                "userName": "mock_user",
                "operation": "CREATE TABLE",
                "operationParameters": {},
                "readVersion": None,
                "isolationLevel": "Serializable",
                "isBlindAppend": True,
            }
        ]

        if limit and limit < len(history):
            history = history[:limit]

        schema = StructType(
            [
                StructField("version", LongType()),
                StructField("timestamp", StringType()),
                StructField("userId", StringType()),
                StructField("userName", StringType()),
                StructField("operation", StringType()),
                StructField("operationParameters", MapType(StringType(), StringType())),
                StructField("readVersion", LongType()),
                StructField("isolationLevel", StringType()),
                StructField("isBlindAppend", LongType()),
            ]
        )

        return DataFrame(history, schema, self._spark._storage)

    def _overwrite_table(self, df: DataFrame) -> None:
        """Persist the provided DataFrame back into the Delta table."""
        df.write.format("delta").mode("overwrite").saveAsTable(self._table_name)

    def _resolve_table_parts(self) -> Tuple[str, str]:
        if "." in self._table_name:
            schema, table = self._table_name.split(".", 1)
        else:
            schema, table = "default", self._table_name
        return schema, table

    def _load_table_rows(self) -> List[Dict[str, Any]]:
        schema_name, table_name = self._resolve_table_parts()
        data = self._spark._storage.get_data(schema_name, table_name)
        return [dict(row) for row in data]

    def _current_schema(self) -> StructType:
        schema_name, table_name = self._resolve_table_parts()
        schema = self._spark._storage.get_table_schema(schema_name, table_name)
        if schema is None:
            from .spark_types import StructType

            return StructType([])
        from typing import cast

        return cast("StructType", schema)

    def _evaluate_row_condition(
        self, normalized_expression: str, row: Dict[str, Any], alias: str
    ) -> bool:
        context = _build_eval_context(row, {alias, "target"})
        try:
            return SafeExpressionEvaluator.evaluate_boolean(
                normalized_expression, context
            )
        except _EVALUATION_FAILURE_EXCEPTIONS:
            return False

    def _evaluate_update_expression(
        self, expression: Any, row: Dict[str, Any], alias: str
    ) -> Any:
        if isinstance(expression, (Column, ColumnOperation)):
            return ConditionEvaluator._get_column_value(row, expression)

        if isinstance(expression, str):
            expr = expression.strip()
            if expr.startswith("'") and expr.endswith("'"):
                return expr[1:-1]
            if expr in row:
                return get_row_value(row, expr)

            normalized = _normalize_boolean_expression(expr)
            context = _build_eval_context(row, {alias, "target"})
            try:
                result = SafeExpressionEvaluator.evaluate(normalized, context)
                return result if result is not None else expr
            except _EVALUATION_FAILURE_EXCEPTIONS as e:
                logger.debug(
                    "Delta update expression evaluation failed, using literal: %s",
                    e,
                    extra={"expression": expr},
                )
                return expr
        return expression


class DeltaOptimizeBuilder:
    """Builder returned by DeltaTable.optimize() for compaction and z-ordering."""

    def __init__(self, delta_table: DeltaTable):
        self._table = delta_table

    def executeCompaction(self) -> DataFrame:
        """Execute compaction (no-op). Returns an empty DataFrame."""
        from .dataframe import DataFrame
        from .spark_types import StructType

        return DataFrame([], StructType([]), self._table._spark._storage)

    def zOrderBy(self, *cols: str) -> DataFrame:
        """Z-order by columns (no-op). Returns an empty DataFrame."""
        from .dataframe import DataFrame
        from .spark_types import StructType

        return DataFrame([], StructType([]), self._table._spark._storage)


class DeltaMergeMatchedActionBuilder:
    """Builder for actions on matched rows (PySpark chained API)."""

    def __init__(
        self, merge_builder: DeltaMergeBuilder, condition: Union[str, None] = None
    ):
        self._merge_builder = merge_builder
        self._condition = condition

    def update(self, set: Optional[Dict[str, Any]] = None) -> DeltaMergeBuilder:
        """Update matched rows with the given assignments."""
        self._merge_builder._when_matched_actions.append(
            {"type": "update", "set": set or {}, "condition": self._condition}
        )
        return self._merge_builder

    def updateAll(self) -> DeltaMergeBuilder:
        """Update all columns of matched rows from source."""
        self._merge_builder._when_matched_actions.append(
            {"type": "update_all", "condition": self._condition}
        )
        return self._merge_builder

    def delete(self) -> DeltaMergeBuilder:
        """Delete matched rows."""
        self._merge_builder._when_matched_actions.append(
            {"type": "delete", "condition": self._condition}
        )
        return self._merge_builder


class DeltaMergeNotMatchedActionBuilder:
    """Builder for actions on not-matched rows (PySpark chained API)."""

    def __init__(
        self, merge_builder: DeltaMergeBuilder, condition: Union[str, None] = None
    ):
        self._merge_builder = merge_builder
        self._condition = condition

    def insert(self, values: Optional[Dict[str, Any]] = None) -> DeltaMergeBuilder:
        """Insert not-matched rows with the given values."""
        self._merge_builder._when_not_matched_actions.append(
            {"type": "insert", "values": values or {}, "condition": self._condition}
        )
        return self._merge_builder

    def insertAll(self) -> DeltaMergeBuilder:
        """Insert all columns of not-matched rows from source."""
        self._merge_builder._when_not_matched_actions.append(
            {"type": "insert_all", "condition": self._condition}
        )
        return self._merge_builder


class DeltaMergeNotMatchedBySourceActionBuilder:
    """Builder for actions on target rows not matched by any source row."""

    def __init__(
        self, merge_builder: DeltaMergeBuilder, condition: Union[str, None] = None
    ):
        self._merge_builder = merge_builder
        self._condition = condition

    def update(self, set: Optional[Dict[str, Any]] = None) -> DeltaMergeBuilder:
        """Update not-matched-by-source rows with the given assignments."""
        self._merge_builder._when_not_matched_by_source_actions.append(
            {"type": "update", "set": set or {}, "condition": self._condition}
        )
        return self._merge_builder

    def delete(self) -> DeltaMergeBuilder:
        """Delete target rows not matched by any source row."""
        self._merge_builder._when_not_matched_by_source_actions.append(
            {"type": "delete", "condition": self._condition}
        )
        return self._merge_builder


class DeltaMergeBuilder:
    """Mock merge builder for method chaining."""

    def __init__(
        self,
        delta_table: DeltaTable,
        source: Any,
        condition: str,
        target_alias: Union[str, None],
    ):
        self._table = delta_table
        self._source = source
        self._condition = condition
        self._target_alias = target_alias or "target"
        self._source_alias = getattr(source, "_alias", None) or "source"
        self._matched_update_assignments: Optional[Dict[str, Any]] = None
        self._matched_update_all: bool = False
        self._matched_delete_condition: Union[str, None] = None
        self._not_matched_insert_assignments: Optional[Dict[str, Any]] = None
        self._not_matched_insert_all: bool = False
        # New chained API action lists
        self._when_matched_actions: List[Dict[str, Any]] = []
        self._when_not_matched_actions: List[Dict[str, Any]] = []
        self._when_not_matched_by_source_actions: List[Dict[str, Any]] = []

    @property
    def _target_aliases(self) -> Set[str]:
        return {self._target_alias, "target", "t"}

    @property
    def _source_aliases(self) -> Set[str]:
        return {self._source_alias, "source", "s"}

    def whenMatchedUpdate(self, set_values: Dict[str, Any]) -> DeltaMergeBuilder:
        assignments = dict(set_values)
        if self._matched_update_assignments is None:
            self._matched_update_assignments = assignments
        else:
            self._matched_update_assignments.update(assignments)
        return self

    def whenMatchedUpdateAll(self) -> DeltaMergeBuilder:
        self._matched_update_all = True
        return self

    def whenMatchedDelete(
        self, condition: Union[str, None] = None
    ) -> DeltaMergeBuilder:
        self._matched_delete_condition = condition
        return self

    def whenNotMatchedInsert(self, values: Dict[str, Any]) -> DeltaMergeBuilder:
        self._not_matched_insert_assignments = dict(values)
        self._not_matched_insert_all = False
        return self

    def whenNotMatchedInsertAll(self) -> DeltaMergeBuilder:
        self._not_matched_insert_assignments = None
        self._not_matched_insert_all = True
        return self

    # ------------------------------------------------------------------
    # Chained API (PySpark standard)
    # ------------------------------------------------------------------

    def whenMatched(
        self, condition: Union[str, None] = None
    ) -> DeltaMergeMatchedActionBuilder:
        """Return a builder for matched-row actions (chained API)."""
        return DeltaMergeMatchedActionBuilder(self, condition)

    def whenNotMatched(
        self, condition: Union[str, None] = None
    ) -> DeltaMergeNotMatchedActionBuilder:
        """Return a builder for not-matched-row actions (chained API)."""
        return DeltaMergeNotMatchedActionBuilder(self, condition)

    def whenNotMatchedBySource(
        self, condition: Union[str, None] = None
    ) -> DeltaMergeNotMatchedBySourceActionBuilder:
        """Return a builder for target rows not matched by source (chained API)."""
        return DeltaMergeNotMatchedBySourceActionBuilder(self, condition)

    def execute(self) -> None:
        """Execute merge operation by reconciling target table with source data."""
        source_df = self._ensure_dataframe(self._source)
        target_schema = self._table._current_schema()
        target_rows = self._table._load_table_rows()
        source_rows = [row.asDict() for row in source_df.collect()]

        target_key, source_key = self._parse_join_keys(self._condition)
        source_groups = defaultdict(list)
        for row in source_rows:
            source_groups[get_row_value(row, source_key)].append(row)

        matched_keys: Set[Any] = set()
        unmatched_target_indices: List[int] = []
        result_rows: List[Dict[str, Any]] = []

        for idx, target_row in enumerate(target_rows):
            key = get_row_value(target_row, target_key)
            source_candidates = source_groups.get(key)
            source_row = source_candidates[0] if source_candidates else None
            if source_row is not None:
                matched_keys.add(key)

                # --- Process matched actions (chained API) ---
                if self._when_matched_actions:
                    row_result = self._process_matched_actions(
                        target_row, source_row, target_schema
                    )
                    if row_result is not None:
                        result_rows.append(row_result)
                    # row_result is None means the row was deleted
                else:
                    # --- Legacy flat API ---
                    if self._should_delete(target_row, source_row):
                        continue
                    updated_row = self._apply_matched_updates(
                        target_row, source_row, target_schema
                    )
                    result_rows.append(updated_row)
            else:
                unmatched_target_indices.append(len(result_rows))
                result_rows.append(dict(target_row))

        # --- Process not-matched-by-source actions (chained API) ---
        if self._when_not_matched_by_source_actions and unmatched_target_indices:
            indices_to_remove: List[int] = []
            for ri in unmatched_target_indices:
                row = result_rows[ri]
                action_result = self._process_not_matched_by_source_actions(
                    row, target_schema
                )
                if action_result is None:
                    # Row should be deleted
                    indices_to_remove.append(ri)
                else:
                    result_rows[ri] = action_result
            # Remove deleted rows (iterate in reverse to keep indices stable)
            for ri in reversed(indices_to_remove):
                result_rows.pop(ri)

        # --- Process not-matched (source rows not in target) ---
        if self._when_not_matched_actions:
            # Chained API for not-matched inserts
            existing_keys = {get_row_value(row, target_key) for row in result_rows}
            for key, rows in source_groups.items():
                if key in matched_keys or key in existing_keys:
                    continue
                for src_row in rows:
                    inserted = self._process_not_matched_actions(src_row, target_schema)
                    if inserted is not None:
                        result_rows.append(inserted)
        elif self._not_matched_insert_all or self._not_matched_insert_assignments:
            # Legacy flat API
            existing_keys = {get_row_value(row, target_key) for row in result_rows}
            for key, rows in source_groups.items():
                if key in matched_keys or key in existing_keys:
                    continue
                for src_row in rows:
                    if self._not_matched_insert_all:
                        result_rows.append(
                            self._project_source_row(src_row, target_schema)
                        )
                    elif self._not_matched_insert_assignments is not None:
                        result_rows.append(
                            self._build_insert_from_assignments(src_row, target_schema)
                        )

        new_df = self._table._spark.createDataFrame(result_rows, target_schema)
        self._table._overwrite_table(new_df)

    def _ensure_dataframe(self, source: Any) -> DataFrame:
        from .dataframe import DataFrame

        if isinstance(source, DataFrame):
            return source
        if hasattr(source, "toDF"):
            return cast("DataFrame", source.toDF())
        raise TypeError("Merge source must be a DataFrame or DeltaTable")

    def _parse_join_keys(self, condition: str) -> Tuple[str, str]:
        pattern = r"\s*(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)\s*"
        match = re.fullmatch(pattern, condition.strip())
        if not match:
            raise NotImplementedError(
                "Only equality join conditions are supported in mock Delta merge"
            )

        left_alias, left_col, right_alias, right_col = match.groups()

        if left_alias in self._target_aliases and right_alias in self._source_aliases:
            return left_col, right_col
        if left_alias in self._source_aliases and right_alias in self._target_aliases:
            return right_col, left_col

        raise NotImplementedError(
            "Join condition must reference both target and source aliases"
        )

    def _should_delete(
        self, target_row: Dict[str, Any], source_row: Dict[str, Any]
    ) -> bool:
        if self._matched_delete_condition is None:
            return False
        if self._matched_delete_condition == "":
            return True
        return bool(
            self._evaluate_condition(
                self._matched_delete_condition, target_row, source_row
            )
        )

    def _apply_matched_updates(
        self,
        target_row: Dict[str, Any],
        source_row: Dict[str, Any],
        schema: StructType,
    ) -> Dict[str, Any]:
        updated = dict(target_row)

        if self._matched_update_all:
            for field in schema.fields:
                if field.name in source_row:
                    updated[field.name] = get_row_value(source_row, field.name)

        if self._matched_update_assignments:
            for column, expression in self._matched_update_assignments.items():
                updated[column] = self._evaluate_assignment(
                    expression, target_row, source_row
                )

        return updated

    def _project_source_row(
        self, source_row: Dict[str, Any], schema: StructType
    ) -> Dict[str, Any]:
        projected = {}
        for field in schema.fields:
            projected[field.name] = get_row_value(source_row, field.name)
        return projected

    def _build_insert_from_assignments(
        self, source_row: Dict[str, Any], schema: StructType
    ) -> Dict[str, Any]:
        row = {field.name: None for field in schema.fields}
        assert self._not_matched_insert_assignments is not None
        for column, expression in self._not_matched_insert_assignments.items():
            row[column] = self._evaluate_assignment(expression, row, source_row)
        return row

    def _evaluate_assignment(
        self,
        expression: Any,
        target_row: Dict[str, Any],
        source_row: Dict[str, Any],
    ) -> Any:
        if hasattr(expression, "operation") or isinstance(expression, Column):
            raise NotImplementedError(
                "Column expressions are not supported in mock Delta merge assignments"
            )

        if isinstance(expression, str):
            expr = expression.strip()
            if expr.startswith("'") and expr.endswith("'"):
                return expr[1:-1]
            if "." in expr:
                alias, field = expr.split(".", 1)
                alias = alias.strip()
                field = field.strip()
                if alias in self._source_aliases:
                    return get_row_value(source_row, field)
                if alias in self._target_aliases:
                    return get_row_value(target_row, field)
            if expr in source_row:
                return get_row_value(source_row, expr)
            if expr in target_row:
                return get_row_value(target_row, expr)
            try:
                return int(expr)
            except ValueError:
                try:
                    return float(expr)
                except ValueError:
                    return expr
        return expression

    def _evaluate_condition(
        self,
        expression: str,
        target_row: Dict[str, Any],
        source_row: Dict[str, Any],
    ) -> bool:
        context: Dict[str, Any] = {}

        target_ns = SimpleNamespace(**target_row)
        source_ns = SimpleNamespace(**source_row)

        for alias in self._target_aliases:
            context[alias] = target_ns
        for alias in self._source_aliases:
            context[alias] = source_ns

        # Allow direct column references to target row values
        context.update(target_row)
        # Provide source columns with prefix for disambiguation
        for key, value in source_row.items():
            context[f"{self._source_alias}_{key}"] = value

        try:
            normalized = _normalize_boolean_expression(expression)
            return SafeExpressionEvaluator.evaluate_boolean(normalized, context)
        except Exception:
            return False

    def _evaluate_target_only_condition(
        self,
        expression: str,
        target_row: Dict[str, Any],
    ) -> bool:
        """Evaluate a condition against a target row only (no source context)."""
        context: Dict[str, Any] = {}
        target_ns = SimpleNamespace(**target_row)
        for alias in self._target_aliases:
            context[alias] = target_ns
        context.update(target_row)
        try:
            normalized = _normalize_boolean_expression(expression)
            return SafeExpressionEvaluator.evaluate_boolean(normalized, context)
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Chained-API action processors
    # ------------------------------------------------------------------

    def _process_matched_actions(
        self,
        target_row: Dict[str, Any],
        source_row: Dict[str, Any],
        schema: StructType,
    ) -> Optional[Dict[str, Any]]:
        """Process chained whenMatched actions. Returns updated row or None (delete)."""
        for action in self._when_matched_actions:
            cond = action.get("condition")
            if cond and not self._evaluate_condition(cond, target_row, source_row):
                continue
            action_type = action["type"]
            if action_type == "delete":
                return None
            if action_type == "update_all":
                updated = dict(target_row)
                for field in schema.fields:
                    if field.name in source_row:
                        updated[field.name] = get_row_value(source_row, field.name)
                return updated
            if action_type == "update":
                updated = dict(target_row)
                for column, expression in action["set"].items():
                    updated[column] = self._evaluate_assignment(
                        expression, target_row, source_row
                    )
                return updated
        # No action matched; keep the row unchanged
        return dict(target_row)

    def _process_not_matched_actions(
        self,
        source_row: Dict[str, Any],
        schema: StructType,
    ) -> Optional[Dict[str, Any]]:
        """Process chained whenNotMatched actions. Returns inserted row or None."""
        for action in self._when_not_matched_actions:
            cond = action.get("condition")
            if cond:
                # Evaluate condition against source row only
                context: Dict[str, Any] = {}
                source_ns = SimpleNamespace(**source_row)
                for alias in self._source_aliases:
                    context[alias] = source_ns
                context.update(source_row)
                try:
                    normalized = _normalize_boolean_expression(cond)
                    if not SafeExpressionEvaluator.evaluate_boolean(
                        normalized, context
                    ):
                        continue
                except Exception:
                    continue
            action_type = action["type"]
            if action_type == "insert_all":
                return self._project_source_row(source_row, schema)
            if action_type == "insert":
                row: Dict[str, Any] = {field.name: None for field in schema.fields}
                for column, expression in action["values"].items():
                    row[column] = self._evaluate_assignment(expression, row, source_row)
                return row
        return None

    def _process_not_matched_by_source_actions(
        self,
        target_row: Dict[str, Any],
        schema: StructType,
    ) -> Optional[Dict[str, Any]]:
        """Process whenNotMatchedBySource actions. Returns updated row or None (delete)."""
        for action in self._when_not_matched_by_source_actions:
            cond = action.get("condition")
            if cond and not self._evaluate_target_only_condition(cond, target_row):
                continue
            action_type = action["type"]
            if action_type == "delete":
                return None
            if action_type == "update":
                updated = dict(target_row)
                for column, expression in action["set"].items():
                    # For not-matched-by-source, evaluate against target only
                    if isinstance(expression, str):
                        expr = expression.strip()
                        if expr.startswith("'") and expr.endswith("'"):
                            updated[column] = expr[1:-1]
                        elif expr in target_row:
                            updated[column] = get_row_value(target_row, expr)
                        else:
                            try:
                                updated[column] = int(expr)
                            except ValueError:
                                try:
                                    updated[column] = float(expr)
                                except ValueError:
                                    updated[column] = expr
                    else:
                        updated[column] = expression
                return updated
        # No action matched; keep row unchanged
        return dict(target_row)
