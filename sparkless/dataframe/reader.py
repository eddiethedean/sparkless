"""
Mock DataFrameReader implementation for DataFrame read operations.

This module provides DataFrame reading functionality, maintaining compatibility
with PySpark's DataFrameReader interface. Supports reading from various data sources
including tables, files, and custom storage backends.

Key Features:
    - Complete PySpark DataFrameReader API compatibility
    - Support for multiple data formats (parquet, json, csv, table)
    - Flexible options configuration
    - Integration with storage manager
    - Schema inference and validation
    - Error handling for missing data sources

Example:
    >>> from sparkless.sql import SparkSession
    >>> spark = SparkSession("test")
    >>> # Read from table
    >>> df = spark.read.table("my_table")
    >>> # Read with format and options
    >>> df = spark.read.format("parquet").option("header", "true").load("/path")
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, TYPE_CHECKING, Tuple, Union, cast

if TYPE_CHECKING:
    from collections.abc import Iterable
    from ..core.interfaces.dataframe import IDataFrame
    from ..core.interfaces.session import ISession

from ..errors import AnalysisException, IllegalArgumentException
from ..spark_types import StructField, StructType, StringType
from ..core.ddl_adapter import parse_ddl_schema


class DataFrameReader:
    """Mock DataFrameReader for reading data from various sources.

    Provides a PySpark-compatible interface for reading DataFrames from storage
    formats and tables. Supports various formats and options for testing and development.

    Attributes:
        session: Sparkless session instance.
        _format: Input format (e.g., 'parquet', 'json').
        _options: Additional options for the reader.

    Example:
        >>> spark.read.format("parquet").load("/path/to/file")
        >>> spark.read.table("my_table")
    """

    def __init__(self, session: ISession):
        """Initialize DataFrameReader.

        Args:
            session: Sparkless session instance.
        """
        self.session = session
        self._format = "parquet"
        self._options: Dict[str, str] = {}
        self._schema: Union[StructType, None] = None

    def format(self, source: str) -> DataFrameReader:
        """Set input format.

        Args:
            source: Data source format.

        Returns:
            Self for method chaining.

        Example:
            >>> spark.read.format("parquet")
        """
        self._format = source
        return self

    def option(self, key: str, value: Any) -> DataFrameReader:
        """Set option.

        Args:
            key: Option key.
            value: Option value.

        Returns:
            Self for method chaining.

        Example:
            >>> spark.read.option("header", "true")
        """
        self._options[key] = value
        return self

    def options(self, **options: Any) -> DataFrameReader:
        """Set multiple options.

        Args:
            **options: Option key-value pairs.

        Returns:
            Self for method chaining.

        Example:
            >>> spark.read.options(header="true", inferSchema="true")
        """
        self._options.update(options)
        return self

    def schema(self, schema: Union[StructType, str]) -> DataFrameReader:
        """Set schema.

        Args:
            schema: Schema definition.

        Returns:
            Self for method chaining.

        Example:
            >>> spark.read.schema("name STRING, age INT")
        """
        if isinstance(schema, StructType):
            self._schema = schema
        elif isinstance(schema, str):
            self._schema = parse_ddl_schema(schema)
        else:
            raise IllegalArgumentException(
                f"Unsupported schema type {type(schema)!r}. "
                "Provide a StructType or DDL string."
            )
        return self

    def load(
        self,
        path: Union[str, None] = None,
        format: Union[str, None] = None,
        **options: Any,
    ) -> IDataFrame:
        """Load data.

        Args:
            path: Path to data.
            format: Data format.
            **options: Additional options.

        Returns:
            DataFrame with loaded data.

        Example:
            >>> spark.read.load("/path/to/file")
            >>> spark.read.format("parquet").load("/path/to/file")
        """
        resolved_format = (format or self._format or "parquet").lower()
        combined_options: Dict[str, Any] = {**self._options, **options}

        if resolved_format == "delta":
            # Delegate to table() for Delta path semantics
            if path is None:
                raise IllegalArgumentException(
                    "load() with format 'delta' requires a path. "
                    "Use read.format('delta').table('schema.table') for tables."
                )
            # Treat path segments as schema.table if possible
            table_name = Path(path).name
            return self.table(table_name)

        if path is None:
            raise IllegalArgumentException(
                "Path is required for DataFrameReader.load()"
            )

        paths = self._gather_paths(Path(path), resolved_format)
        if not paths:
            raise AnalysisException(f"No {resolved_format} files found at {path}")

        data_rows, column_names = self._read_data(paths, resolved_format, combined_options)
        infer_schema = self._to_bool(
            combined_options.get("inferSchema", "false"),
            default=False,
        )
        schema, data_rows = self._build_schema_and_rows_from_data(
            data_rows, column_names, resolved_format, infer_schema
        )

        from .dataframe import DataFrame

        # Access storage through catalog (ISession protocol doesn't expose _storage)
        storage = getattr(self.session, "_storage", None)
        if storage is None:
            storage = self.session.catalog._storage  # type: ignore[attr-defined]
        return cast("IDataFrame", DataFrame(data_rows, schema, storage))

    def table(self, table_name: str) -> IDataFrame:
        """Load table.

        Args:
            table_name: Table name.

        Returns:
            DataFrame with table data.

        Example:
            >>> spark.read.table("my_table")
            >>> spark.read.format("delta").option("versionAsOf", 0).table("my_table")
        """
        # Check for versionAsOf option (Delta time travel)
        if "versionAsOf" in self._options and self._format == "delta":
            version_number = int(self._options["versionAsOf"])

            # Parse schema and table name
            if "." in table_name:
                schema_name, table_only = table_name.split(".", 1)
            else:
                schema_name, table_only = "default", table_name

            # Get table metadata to access version history
            # Access storage through catalog (ISession protocol doesn't expose _storage)
            storage = getattr(self.session, "_storage", None)
            if storage is None:
                storage = self.session.catalog._storage  # type: ignore[attr-defined]
            meta = storage.get_table_metadata(schema_name, table_only)

            if not meta or meta.get("format") != "delta":
                from ..errors import AnalysisException

                raise AnalysisException(
                    f"Table {table_name} is not a Delta table. "
                    "versionAsOf can only be used with Delta format tables."
                )

            version_history = meta.get("version_history", [])

            # Find the requested version
            target_version = None
            for v in version_history:
                # Handle both MockDeltaVersion objects and dicts
                v_num = v.version if hasattr(v, "version") else v.get("version")
                if v_num == version_number:
                    target_version = v
                    break

            if target_version is None:
                from ..errors import AnalysisException

                raise AnalysisException(
                    f"Version {version_number} does not exist for table {table_name}. "
                    f"Available versions: {[v.version if hasattr(v, 'version') else v.get('version') for v in version_history]}"
                )

            # Get the data snapshot for this version
            data_snapshot = (
                target_version.data_snapshot
                if hasattr(target_version, "data_snapshot")
                else target_version.get("data_snapshot", [])
            )

            # Create DataFrame with the historical data using session's createDataFrame
            return self.session.createDataFrame(data_snapshot)

        return self.session.table(table_name)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _gather_paths(self, root: Path, data_format: str) -> List[str]:
        """Collect concrete file paths for the requested format."""
        if root.is_file():
            return [str(root)]

        if not root.exists():
            return []

        extension = self._extension_for_format(data_format)
        if extension:
            return [str(p) for p in sorted(root.rglob(f"*{extension}")) if p.is_file()]

        # Fallback – include all files
        return [str(p) for p in sorted(root.rglob("*")) if p.is_file()]

    def _extension_for_format(self, data_format: str) -> Union[str, None]:
        """Map format names to file extensions."""
        mapping = {
            "parquet": ".parquet",
            "csv": ".csv",
            "json": ".json",
            "ndjson": ".json",
            "text": ".txt",
        }
        return mapping.get(data_format)

    def _read_data(
        self, paths: List[str], data_format: str, options: Dict[str, Any]
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Load data from disk; returns (list of row dicts, column names)."""
        if not paths:
            return [], []

        if data_format == "parquet":
            return self._read_parquet(paths)
        if data_format == "csv":
            return self._read_csv(paths, options)
        if data_format == "json":
            return self._read_json(paths, options)
        if data_format == "text":
            return self._read_text(paths)

        raise AnalysisException(
            f"Unsupported format '{data_format}' for DataFrameReader"
        )

    def _read_parquet(self, paths: List[str]) -> Tuple[List[Dict[str, Any]], List[str]]:
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "Reading Parquet requires pandas. Install with: pip install pandas"
            ) from None
        dfs = [pd.read_parquet(p) for p in paths]
        combined = pd.concat(dfs, ignore_index=True)
        names = list(combined.columns)
        rows = combined.to_dict("records")
        return rows, names

    def _read_csv(
        self, paths: List[str], options: Dict[str, Any]
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        has_header = self._to_bool(
            options.get("header", options.get("hasHeader", "true")), default=True
        )
        delimiter = options.get("sep", options.get("delimiter", ","))
        rows: List[Dict[str, Any]] = []
        names: List[str] = []
        for file_path in paths:
            with open(file_path, newline="", encoding="utf-8") as f:
                if has_header:
                    reader = csv.DictReader(f, delimiter=delimiter)
                    fieldnames = reader.fieldnames or []
                    if not names:
                        names = list(fieldnames)
                    for row in reader:
                        rows.append(row)
                else:
                    r = csv.reader(f, delimiter=delimiter)
                    for i, row_list in enumerate(r):
                        if not names:
                            names = [f"_c{j}" for j in range(len(row_list))]
                        rows.append(dict(zip(names, row_list)))
        if not names and rows:
            names = list(rows[0].keys())
        return rows, names

    def _read_json(
        self, paths: List[str], options: Dict[str, Any]
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        all_rows: List[Dict[str, Any]] = []
        for file_path in paths:
            with open(file_path, encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    continue
                try:
                    parsed = json.loads(content)
                except json.JSONDecodeError:
                    parsed = []
                    for line in content.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        parsed.append(json.loads(line))
                if isinstance(parsed, list):
                    all_rows.extend(parsed)
                else:
                    all_rows.append(parsed)
        names = list(all_rows[0].keys()) if all_rows else []
        return all_rows, names

    def _read_text(self, paths: List[str]) -> Tuple[List[Dict[str, Any]], List[str]]:
        values: List[Dict[str, str]] = []
        for file_path in paths:
            with open(file_path, encoding="utf-8") as f:
                for line in f:
                    values.append({"value": line.rstrip("\n")})
        return values, ["value"]

    def _to_bool(self, value: Any, default: bool = False) -> bool:
        """Interpret Spark-style truthy values."""
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "y"}

    def _build_schema_and_rows_from_data(
        self,
        data_rows: List[Dict[str, Any]],
        column_names: List[str],
        data_format: str = "parquet",
        infer_schema: bool = False,
    ) -> Tuple[StructType, List[Dict[str, Any]]]:
        """Build StructType and rows from loaded data; apply user schema if set.

        When inferSchema is true and no user schema is set, infers types:
        - JSON: uses SchemaInferenceEngine (values already typed from parsing)
        - CSV: infers int, float, bool, date, timestamp from string values
        """
        if self._schema is not None:
            names = self._schema.fieldNames()
            projected = [{k: r.get(k) for k in names} for r in data_rows]
            return self._schema, projected
        if infer_schema and data_rows and column_names:
            if data_format == "json":
                from ..core.schema_inference import SchemaInferenceEngine

                schema, normalized = SchemaInferenceEngine.infer_from_data(
                    data_rows, column_order=column_names
                )
                return schema, normalized
            if data_format == "csv":
                from ..core.schema_inference import infer_schema_from_csv_strings

                schema, normalized = infer_schema_from_csv_strings(
                    data_rows, column_names
                )
                return schema, normalized
        fields = [StructField(name, StringType()) for name in column_names]
        schema = StructType(fields)
        return schema, data_rows

    def json(self, path: str, **options: Any) -> IDataFrame:
        """Load JSON data from disk."""
        return self.format("json").options(**options).load(path)

    def csv(self, path: str, **options: Any) -> IDataFrame:
        """Load CSV data from disk."""
        return self.format("csv").options(**options).load(path)

    def parquet(self, path: str, **options: Any) -> IDataFrame:
        """Load Parquet data from disk."""
        return self.format("parquet").options(**options).load(path)

    def orc(self, path: str, **options: Any) -> IDataFrame:
        """Load ORC data.

        Args:
            path: Path to ORC file.
            **options: Additional options.

        Returns:
            DataFrame with ORC data.

        Example:
            >>> spark.read.orc("/path/to/file.orc")
        """
        raise AnalysisException("ORC format is not supported")

    def text(self, path: str, **options: Any) -> IDataFrame:
        """Load text data.

        Args:
            path: Path to text file.
            **options: Additional options.

        Returns:
            DataFrame with text data.

        Example:
            >>> spark.read.text("/path/to/file.txt")
        """
        return self.format("text").options(**options).load(path)

    def jdbc(self, url: str, table: str, **options: Any) -> IDataFrame:
        """Load data from JDBC source.

        Args:
            url: JDBC URL.
            table: Table name.
            **options: Additional options.

        Returns:
            DataFrame with JDBC data.

        Example:
            >>> spark.read.jdbc("jdbc:postgresql://localhost:5432/db", "table")
        """
        # Mock implementation
        from .dataframe import DataFrame

        return cast("IDataFrame", DataFrame([], StructType([])))
