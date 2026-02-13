"""
Minimal compatibility layer for using Robin (robin-sparkless) as the Sparkless engine.

Provides conversion from PySpark-style createDataFrame(data, schema=None) to
Robin's _create_dataframe_from_rows(data, schema) where schema is list of (name, dtype_str).

Exposes only PySpark camelCase API: wraps Robin's DataFrame/GroupedData so that
Sparkless public API matches PySpark (groupBy, withColumn, orderBy, etc.) with no
extra snake_case aliases.
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple, Union

from sparkless.spark_types import StructType, StructField, Row, StringType
from sparkless.core.schema_inference import SchemaInferenceEngine

# PySpark camelCase -> Robin snake_case (DataFrame methods)
_CAMEL_TO_SNAKE_DF: dict[str, str] = {
    "groupBy": "group_by",
    "withColumn": "with_column",
    "orderBy": "order_by",
    "withColumnRenamed": "with_column_renamed",
    "dropDuplicates": "drop_duplicates",
    "unionByName": "union_by_name",
    "selectExpr": "select_expr",
    "withColumnsRenamed": "with_columns_renamed",
    "createOrReplaceTempView": "create_or_replace_temp_view",
    "printSchema": "print_schema",
    "sortWithinPartitions": "sort_within_partitions",
    "orderByExprs": "order_by_exprs",
}
# dropDuplicates: Robin may have drop_duplicates or only distinct

# Map Sparkless/PySpark DataType.typeName() to Robin dtype strings.
_SPARK_TYPE_TO_ROBIN_DTYPE: dict[str, str] = {
    "string": "string",
    "int": "int",
    "long": "bigint",
    "bigint": "bigint",
    "double": "double",
    "float": "float",
    "boolean": "boolean",
    "bool": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "timestamp_ntz": "timestamp",
    "datetime": "timestamp",
    "str": "string",
    "varchar": "string",
}


def _spark_type_to_robin_dtype(data_type: Any) -> str:
    """Map Sparkless/PySpark DataType to Robin schema dtype string."""
    name = getattr(data_type, "typeName", None)
    if callable(name):
        name = name()
    else:
        name = getattr(data_type, "__class__", None)
        name = getattr(name, "__name__", "string") if name else "string"
    return _SPARK_TYPE_TO_ROBIN_DTYPE.get(name, "string")


def _struct_type_to_robin_schema(schema: StructType) -> List[Tuple[str, str]]:
    """Convert StructType to list of (name, dtype_str) for Robin."""
    if not hasattr(schema, "fields"):
        return []
    return [
        (getattr(f, "name", ""), _spark_type_to_robin_dtype(getattr(f, "dataType", None)))
        for f in schema.fields
    ]


def _row_value_for_robin(val: Any) -> Any:
    """Convert a single cell value for Robin (e.g. datetime -> ISO string)."""
    if val is None or isinstance(val, (int, float, bool, str)):
        return val
    if isinstance(val, dict):
        return {k: _row_value_for_robin(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_row_value_for_robin(v) for v in val]
    if hasattr(val, "isoformat") and callable(getattr(val, "isoformat")):
        return val.isoformat()
    return str(val)


def _data_to_robin_rows(
    data: List[Any],
    names: List[str],
    schema: Optional[Any] = None,
) -> List[dict]:
    """Convert data to list of dicts for Robin _create_dataframe_from_rows."""
    bool_cols: set = set()
    if schema is not None and hasattr(schema, "fields"):
        for f in schema.fields:
            dt = getattr(f, "dataType", None)
            if dt is not None:
                nm = getattr(dt, "__class__", None)
                nm = getattr(nm, "__name__", "") if nm else ""
                if nm == "BooleanType":
                    bool_cols.add(getattr(f, "name", ""))

    def _convert_val(val: Any, col_name: str) -> Any:
        v = _row_value_for_robin(val)
        if col_name in bool_cols and isinstance(v, int) and v in (0, 1):
            return bool(v)
        return v

    rows: List[dict] = []
    for row in data:
        if isinstance(row, dict):
            rows.append({n: _convert_val(row.get(n), n) for n in names})
        elif isinstance(row, (list, tuple)):
            values = list(row) + [None] * (len(names) - len(row))
            rows.append(
                dict(zip(names, (_convert_val(v, n) for v, n in zip(values, names))))
            )
        else:
            rows.append({n: _convert_val(getattr(row, n, None), n) for n in names})
    return rows


def create_dataframe_via_robin(
    robin_session: Any,
    data: Union[List[dict], List[tuple], Any],
    schema: Optional[Union[StructType, List[str], str]] = None,
) -> Any:
    """
    Create a Robin DataFrame from data and optional schema (PySpark-style).

    Uses the session's _create_dataframe_from_rows or create_dataframe_from_rows.
    """
    create_fn = getattr(
        robin_session, "create_dataframe_from_rows", None
    ) or getattr(robin_session, "_create_dataframe_from_rows", None)
    if create_fn is None:
        raise RuntimeError(
            "Robin session has no create_dataframe_from_rows / _create_dataframe_from_rows"
        )

    # Normalize data to list of dicts and get schema
    if hasattr(data, "to_dict") and callable(getattr(data, "to_dict")):
        # Pandas-like
        data = data.to_dict(orient="records")  # type: ignore[union-attr]
    if hasattr(data, "collect"):
        data = [r.asDict() if hasattr(r, "asDict") else dict(r) for r in data.collect()]

    if not isinstance(data, list):
        data = list(data)

    if schema is None:
        if not data:
            raise ValueError("Cannot infer schema from empty data")
        if not all(isinstance(r, dict) for r in data):
            raise ValueError("When schema is None, data must be a list of dicts")
        inferred_schema, normalized_data = SchemaInferenceEngine.infer_from_data(data)
        data = normalized_data
        schema = inferred_schema
        names = inferred_schema.fieldNames()
        robin_schema = _struct_type_to_robin_schema(inferred_schema)
    elif isinstance(schema, (list, tuple)) and schema and all(
        isinstance(x, str) for x in schema
    ):
        names = list(schema)
        robin_schema = [(n, "string") for n in names]
        # Convert list of tuples to list of dicts if needed
        if data and not isinstance(data[0], dict):
            data = [dict(zip(names, row)) for row in data]
        schema = None
    else:
        # StructType
        names = schema.fieldNames()
        robin_schema = _struct_type_to_robin_schema(schema)

    rows = _data_to_robin_rows(data, names, schema)
    robin_df = create_fn(rows, robin_schema)
    return wrap_robin_dataframe(robin_df)


def _is_robin_dataframe(obj: Any) -> bool:
    """True if obj looks like a Robin DataFrame (needs wrapping)."""
    if obj is None or type(obj).__name__ == "_PySparkCompatDataFrame":
        return False
    return type(obj).__name__ == "DataFrame" and (
        type(obj).__module__.startswith("robin") or hasattr(obj, "group_by")
    )


def _is_robin_grouped_data(obj: Any) -> bool:
    """True if obj looks like Robin GroupedData (needs wrapping)."""
    if obj is None or type(obj).__name__ == "_PySparkCompatGroupedData":
        return False
    return type(obj).__name__ == "GroupedData" and (
        type(obj).__module__.startswith("robin") or hasattr(obj, "agg")
    )


def _wrap_if_dataframe(obj: Any) -> Any:
    """Wrap Robin DataFrame/GroupedData in PySpark-compat wrapper if applicable."""
    if obj is None:
        return None
    if _is_robin_dataframe(obj):
        return wrap_robin_dataframe(obj)
    if _is_robin_grouped_data(obj):
        return wrap_robin_grouped_data(obj)
    return obj


def wrap_robin_grouped_data(robin_gd: Any) -> "_PySparkCompatGroupedData":
    """Wrap Robin GroupedData to expose PySpark camelCase (agg, count, sum, etc.)."""
    return _PySparkCompatGroupedData(robin_gd)


def wrap_robin_dataframe(robin_df: Any) -> "_PySparkCompatDataFrame":
    """Wrap Robin DataFrame to expose only PySpark camelCase API."""
    return _PySparkCompatDataFrame(robin_df)


class _PySparkCompatGroupedData:
    """Wraps Robin GroupedData; exposes PySpark camelCase (agg, count, sum, avg, min, max)."""

    def __init__(self, robin_gd: Any) -> None:
        self._robin_gd = robin_gd

    def __getattr__(self, name: str) -> Any:
        return getattr(self._robin_gd, name)

    def agg(self, *exprs: Any, **kwargs: Any) -> Any:
        # Robin may expect a list of expressions, not *args
        exprs_list = list(exprs)
        result = self._robin_gd.agg(exprs_list, **kwargs)
        return _wrap_if_dataframe(result)

    def count(self) -> Any:
        return _wrap_if_dataframe(self._robin_gd.count())

    def sum(self, *cols: Any) -> Any:
        return _wrap_if_dataframe(getattr(self._robin_gd, "sum", lambda *a: self._robin_gd.agg(*a))(*cols))

    def avg(self, *cols: Any) -> Any:
        return _wrap_if_dataframe(getattr(self._robin_gd, "avg", lambda *a: self._robin_gd.agg(*a))(*cols))

    def min(self, *cols: Any) -> Any:
        return _wrap_if_dataframe(getattr(self._robin_gd, "min", lambda *a: self._robin_gd.agg(*a))(*cols))

    def max(self, *cols: Any) -> Any:
        return _wrap_if_dataframe(getattr(self._robin_gd, "max", lambda *a: self._robin_gd.agg(*a))(*cols))

    def mean(self, *cols: Any) -> Any:
        return _wrap_if_dataframe(getattr(self._robin_gd, "mean", self._robin_gd.avg)(*cols))


def _robin_f_col(name: str) -> Any:
    """Return Robin F.col(name) for PySpark-style df.column access."""
    import robin_sparkless as _r
    return _r.col(name)


class _NaCompat:
    """Wrapper for df.na so that .drop() and .fill() return compat DataFrames."""

    def __init__(self, robin_na_or_df: Any, wrap_fn: Any) -> None:
        self._robin_na = getattr(robin_na_or_df, "na", robin_na_or_df)
        self._wrap = wrap_fn

    def drop(self, *args: Any, **kwargs: Any) -> Any:
        robin_na = self._robin_na if not callable(self._robin_na) else self._robin_na()
        drop_fn = getattr(robin_na, "drop", None)
        if drop_fn is None:
            raise AttributeError("na.drop not available")
        return self._wrap(drop_fn(*args, **kwargs))

    def fill(self, *args: Any, **kwargs: Any) -> Any:
        robin_na = self._robin_na if not callable(self._robin_na) else self._robin_na()
        fill_fn = getattr(robin_na, "fill", None)
        if fill_fn is None:
            raise AttributeError("na.fill not available")
        return self._wrap(fill_fn(*args, **kwargs))


def _compat_column_names(robin_df: Any) -> List[str]:
    """Return list of column names for the given Robin DataFrame."""
    cols = getattr(robin_df, "columns", None)
    if callable(cols):
        cols = cols()
    return list(cols or [])


class _PySparkCompatDataFrame:
    """Wraps Robin DataFrame; exposes only PySpark camelCase method names."""

    def __init__(self, robin_df: Any) -> None:
        self._robin_df = robin_df

    @property
    def schema(self) -> Any:
        """PySpark-compatible schema (StructType)."""
        robin_df = self._robin_df
        s = getattr(robin_df, "schema", None)
        if s is not None and hasattr(s, "fields") and hasattr(s, "fieldNames"):
            return s
        cols = _compat_column_names(robin_df)
        if not cols:
            return StructType([])
        return StructType([StructField(c, StringType()) for c in cols])

    def __getitem__(self, key: Any) -> Any:
        """PySpark-style df['col'] returns column reference."""
        if isinstance(key, str):
            return _robin_f_col(key)
        # slice or list of columns: delegate to Robin; wrap result if DataFrame
        if hasattr(self._robin_df, "__getitem__"):
            result = self._robin_df[key]
            return _wrap_if_dataframe(result)
        return _robin_f_col(key)

    def __getattr__(self, name: str) -> Any:
        # PySpark column access: df.salary -> F.col("salary") when name is a column
        if not name.startswith("_"):
            cols = _compat_column_names(self._robin_df)
            if name in cols:
                return _robin_f_col(name)
        # Map PySpark camelCase to Robin snake_case
        snake = _CAMEL_TO_SNAKE_DF.get(name)
        if snake and hasattr(self._robin_df, snake):
            robin_method = getattr(self._robin_df, snake)

            def _wrapped(*args: Any, **kwargs: Any) -> Any:
                result = robin_method(*args, **kwargs)
                return _wrap_if_dataframe(result)

            return _wrapped
        # na: PySpark df.na.drop(); Robin may expose na as method or object
        if name == "na":
            robin_na = getattr(self._robin_df, "na", None)
            if robin_na is not None and not callable(robin_na):
                return _NaCompat(robin_na, _wrap_if_dataframe)
            if callable(robin_na):
                robin_na = robin_na()
            if robin_na is not None:
                return _NaCompat(robin_na, _wrap_if_dataframe)
            # Fallback: object with drop() -> dropna(), fill() -> fillna()
            return _NaCompat(self._robin_df, _wrap_if_dataframe)
        # groupby: PySpark alias for groupBy
        if name == "groupby":
            return self.groupBy
        # dropDuplicates: Robin may have drop_duplicates or distinct
        if name == "dropDuplicates":
            if hasattr(self._robin_df, "drop_duplicates"):
                def _drop_dup(*args: Any, **kwargs: Any) -> Any:
                    return _wrap_if_dataframe(self._robin_df.drop_duplicates(*args, **kwargs))
                return _drop_dup
            def _distinct() -> Any:
                return _wrap_if_dataframe(self._robin_df.distinct())
            return _distinct
        # Pass through (select, filter, limit, distinct, drop, collect, columns, etc.)
        attr = getattr(self._robin_df, name)
        if callable(attr):
            def _wrapped(*args: Any, **kwargs: Any) -> Any:
                result = attr(*args, **kwargs)
                if name == "collect":
                    return _wrap_collect_rows(result)
                return _wrap_if_dataframe(result)
            return _wrapped
        return attr

    def join(self, other: Any, on: Any = None, how: str = "inner") -> Any:
        """Join with other DataFrame; unwrap compat wrapper so Robin receives native DataFrame."""
        robin_other = getattr(other, "_robin_df", other)
        result = self._robin_df.join(robin_other, on=on, how=how)
        return _wrap_if_dataframe(result)

    def unionByName(self, other: Any, allowMissingColumns: bool = False) -> Any:
        """Union by name; unwrap other and drop allowMissingColumns if Robin does not support it."""
        robin_other = getattr(other, "_robin_df", other)
        fn = getattr(self._robin_df, "union_by_name", None)
        if fn is None:
            raise AttributeError("union_by_name")
        try:
            result = fn(robin_other, allow_missing_columns=allowMissingColumns)
        except TypeError:
            result = fn(robin_other)
        return _wrap_if_dataframe(result)

    def drop(self, *cols: Any, **kwargs: Any) -> Any:
        """Drop columns; Robin may require cols as a list."""
        if cols and len(cols) == 1 and isinstance(cols[0], str):
            result = self._robin_df.drop([cols[0]], **kwargs)
        else:
            result = self._robin_df.drop(list(cols) if cols else [], **kwargs)
        return _wrap_if_dataframe(result)

    def groupBy(self, *cols: Any, **kwargs: Any) -> Any:
        """PySpark: groupBy(*cols). Robin: group_by accepts list of column names or Column objects."""
        robin_df = self._robin_df
        if hasattr(robin_df, "group_by"):
            col_list = list(cols) if cols else []
            if len(col_list) == 1 and isinstance(col_list[0], str):
                col_list = [col_list[0]]
            result = robin_df.group_by(col_list)
            return wrap_robin_grouped_data(result)
        raise AttributeError("group_by")

    def orderBy(self, *cols: Any, **kwargs: Any) -> Any:
        """Order by; Robin may expect list of SortOrder or list of column names."""
        robin_df = self._robin_df
        if not hasattr(robin_df, "order_by"):
            return _wrap_if_dataframe(getattr(robin_df, "orderBy", lambda *a, **k: None)(*cols, **kwargs))
        # Normalize: single SortOrder -> list
        if len(cols) == 1 and not isinstance(cols[0], (list, tuple)):
            result = robin_df.order_by([cols[0]], **kwargs)
        else:
            result = robin_df.order_by(list(cols) if cols else [], **kwargs)
        return _wrap_if_dataframe(result)

    def sort(self, *cols: Any, **kwargs: Any) -> Any:
        return self.orderBy(*cols, **kwargs)


def _wrap_collect_rows(rows: Any) -> List[Any]:
    """Wrap collect() result so each row supports PySpark Row-style attribute access."""
    if not rows:
        return rows
    out: List[Any] = []
    for r in rows:
        if hasattr(r, "asDict"):
            out.append(Row(r.asDict()))
        elif isinstance(r, dict):
            out.append(Row(r))
        else:
            out.append(r)
    return out
