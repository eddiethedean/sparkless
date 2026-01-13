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

from pathlib import Path
from typing import Any, Dict, List, TYPE_CHECKING, Tuple, Union, cast

if TYPE_CHECKING:
    from collections.abc import Iterable
    from ..core.interfaces.dataframe import IDataFrame
    from ..core.interfaces.session import ISession

import polars as pl

from ..errors import AnalysisException, IllegalArgumentException
from ..spark_types import StructField, StructType
from ..core.ddl_adapter import parse_ddl_schema
from ..backend.polars.schema_utils import align_frame_to_schema
from ..backend.polars.type_mapper import polars_dtype_to_mock_type


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

        frame = self._read_with_polars(paths, resolved_format, combined_options)

        schema, data_rows = self._build_schema_and_rows(frame)

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

    def _read_with_polars(
        self, paths: Iterable[str], data_format: str, options: Dict[str, Any]
    ) -> pl.DataFrame:
        """Load data from disk using Polars."""
        paths_list = list(paths)
        if not paths_list:
            return pl.DataFrame()

        if data_format == "parquet":
            return pl.scan_parquet(
                paths_list, **self._extract_parquet_options(options)
            ).collect()
        if data_format == "csv":
            csv_opts = self._extract_csv_options(options)
            return pl.scan_csv(paths_list, **csv_opts).collect()
        if data_format == "json":
            return self._read_json(paths_list, options)
        if data_format == "text":
            return self._read_text(paths_list)

        raise AnalysisException(
            f"Unsupported format '{data_format}' for DataFrameReader"
        )

    def _read_json(self, paths: List[str], options: Dict[str, Any]) -> pl.DataFrame:
        """Read JSON or NDJSON files via Polars, falling back to Python json."""
        ndjson_opts = {}
        if "infer_schema_length" in options:
            ndjson_opts["infer_schema_length"] = int(options["infer_schema_length"])

        try:
            return pl.scan_ndjson(paths, **ndjson_opts).collect()
        except Exception:
            pass

        frames: List[pl.DataFrame] = []
        import json

        for file_path in paths:
            with open(file_path, encoding="utf-8") as handle:
                content = handle.read().strip()
                if not content:
                    continue
                try:
                    parsed = json.loads(content)
                except json.JSONDecodeError:
                    # Attempt line-by-line parsing for permissive mode
                    parsed = []
                    for line in content.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        parsed.append(json.loads(line))
                if isinstance(parsed, list):
                    frames.append(pl.DataFrame(parsed))
                else:
                    frames.append(pl.DataFrame([parsed]))

        if not frames:
            return pl.DataFrame()

        return pl.concat(frames, how="diagonal_relaxed")

    def _read_text(self, paths: List[str]) -> pl.DataFrame:
        """Read plain text files into a single-column DataFrame."""
        values: List[str] = []
        for file_path in paths:
            with open(file_path, encoding="utf-8") as handle:
                values.extend([line.rstrip("\n") for line in handle])
        return pl.DataFrame({"value": values})

    def _extract_parquet_options(self, options: Dict[str, Any]) -> Dict[str, Any]:
        """Filter parquet-specific options."""
        parquet_opts: Dict[str, Any] = {}
        if "hive_partitioning" in options:
            parquet_opts["hive_partitioning"] = options["hive_partitioning"]
        return parquet_opts

    def _extract_csv_options(self, options: Dict[str, Any]) -> Dict[str, Any]:
        """Translate Spark CSV options to Polars equivalents."""
        csv_opts: Dict[str, Any] = {}

        header = options.get("header", options.get("hasHeader", "true"))
        csv_opts["has_header"] = self._to_bool(header, default=True)

        delimiter = options.get("sep", options.get("delimiter", ","))
        if isinstance(delimiter, str) and delimiter:
            csv_opts["separator"] = delimiter

        null_value = options.get("nullValue")
        if null_value is not None:
            csv_opts["null_values"] = null_value

        infer_schema = options.get("inferSchema")
        if infer_schema is not None and self._to_bool(infer_schema):
            # Explicitly enable schema inference
            # Don't set infer_schema_length - let Polars use default (all rows)
            csv_opts["infer_schema"] = True
        else:
            # Explicitly set infer_schema=False to match PySpark default
            # PySpark defaults to inferSchema=False (all columns as strings)
            csv_opts["infer_schema"] = False

        return csv_opts

    def _to_bool(self, value: Any, default: bool = False) -> bool:
        """Interpret Spark-style truthy values."""
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "y"}

    def _build_schema_and_rows(
        self, frame: pl.DataFrame
    ) -> Tuple[StructType, List[Dict[str, Any]]]:
        """Create StructType schema and dictionary rows from a Polars frame."""
        if self._schema is not None:
            aligned = align_frame_to_schema(frame, self._schema)
            return self._schema, aligned.to_dicts()

        fields: List[StructField] = []
        for name, dtype in zip(frame.columns, frame.dtypes):
            mock_type = polars_dtype_to_mock_type(dtype)
            fields.append(StructField(name, mock_type))
        schema = StructType(fields)
        return schema, frame.to_dicts()

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
        raise AnalysisException("ORC format is not supported by the Polars backend")

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
