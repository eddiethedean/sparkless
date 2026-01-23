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
import re
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING, Tuple, Union, cast
from collections import defaultdict

if TYPE_CHECKING:
    from .dataframe import DataFrame
    from .spark_types import StructType

from .functions import Column
from .core.safe_evaluator import SafeExpressionEvaluator


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
    def delete(self, condition: Union[str, None] = None) -> None:
        """Delete rows matching the given condition."""
        rows = self._load_table_rows()
        schema = self._current_schema()
        alias = getattr(self, "_alias", "target")

        if not condition:
            remaining_rows: List[Dict[str, Any]] = []
        else:
            normalized = _normalize_boolean_expression(condition)
            remaining_rows = [
                row
                for row in rows
                if not self._evaluate_row_condition(normalized, row, alias)
            ]

        new_df = self._spark.createDataFrame(remaining_rows, schema)
        self._overwrite_table(new_df)

    def update(self, condition: str, set_values: Dict[str, Any]) -> None:
        """Update rows that satisfy condition with provided assignments."""
        if not set_values:
            return

        rows = self._load_table_rows()
        schema = self._current_schema()
        alias = getattr(self, "_alias", "target")
        normalized_condition = (
            _normalize_boolean_expression(condition) if condition else None
        )

        updated_rows: List[Dict[str, Any]] = []
        for row in rows:
            should_update = (
                True
                if normalized_condition is None
                else self._evaluate_row_condition(normalized_condition, row, alias)
            )
            if not should_update:
                updated_rows.append(row)
                continue

            new_row = dict(row)
            for column_name, expression in set_values.items():
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

    def optimize(self) -> DeltaTable:
        """
        Mock OPTIMIZE operation.

        In real Delta Lake, this compacts small files.
        For testing, this is a no-op that returns self.

        Returns:
            self for method chaining
        """
        return self

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
        except Exception:
            return False

    def _evaluate_update_expression(
        self, expression: Any, row: Dict[str, Any], alias: str
    ) -> Any:
        if hasattr(expression, "operation") or isinstance(expression, Column):
            raise NotImplementedError(
                "Column expressions are not supported for DeltaTable.update in mock implementation"
            )

        if isinstance(expression, str):
            expr = expression.strip()
            if expr.startswith("'") and expr.endswith("'"):
                return expr[1:-1]
            if expr in row:
                return row.get(expr)

            normalized = _normalize_boolean_expression(expr)
            context = _build_eval_context(row, {alias, "target"})
            try:
                result = SafeExpressionEvaluator.evaluate(normalized, context)
                return result if result is not None else expr
            except Exception:
                return expr
        return expression


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

    def execute(self) -> None:
        """Execute merge operation by reconciling target table with source data."""
        source_df = self._ensure_dataframe(self._source)
        target_schema = self._table._current_schema()
        target_rows = self._table._load_table_rows()
        source_rows = [row.asDict() for row in source_df.collect()]

        target_key, source_key = self._parse_join_keys(self._condition)
        source_groups = defaultdict(list)
        for row in source_rows:
            source_groups[row.get(source_key)].append(row)

        matched_keys: Set[Any] = set()
        result_rows: List[Dict[str, Any]] = []

        for target_row in target_rows:
            key = target_row.get(target_key)
            source_candidates = source_groups.get(key)
            source_row = source_candidates[0] if source_candidates else None
            if source_row is not None:
                matched_keys.add(key)
                if self._should_delete(target_row, source_row):
                    continue
                updated_row = self._apply_matched_updates(
                    target_row, source_row, target_schema
                )
                result_rows.append(updated_row)
            else:
                result_rows.append(dict(target_row))

        if self._not_matched_insert_all or self._not_matched_insert_assignments:
            existing_keys = {row.get(target_key) for row in result_rows}
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
                    updated[field.name] = source_row.get(field.name)

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
            projected[field.name] = source_row.get(field.name)
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
                    return source_row.get(field)
                if alias in self._target_aliases:
                    return target_row.get(field)
            if expr in source_row:
                return source_row.get(expr)
            if expr in target_row:
                return target_row.get(expr)
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
