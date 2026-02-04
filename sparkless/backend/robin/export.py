"""Robin backend export: delegates to Polars exporter.

DataFrame export (to_polars, to_pandas, to_parquet, etc.) uses the same
Polars exporter so existing behavior is unchanged. A future full Robin backend
could use robin_sparkless collect/to_pandas for export.
"""

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from sparkless.dataframe import DataFrame


class RobinExporter:
    """Export backend for robin; delegates to Polars exporter."""

    def __init__(self) -> None:
        from sparkless.backend.polars.export import PolarsExporter

        self._polars = PolarsExporter()

    def to_polars(self, df: "DataFrame") -> Any:
        return self._polars.to_polars(df)

    def to_pandas(self, df: "DataFrame") -> Any:
        return self._polars.to_pandas(df)

    def to_parquet(
        self, df: "DataFrame", path: str, compression: str = "snappy"
    ) -> None:
        self._polars.to_parquet(df, path, compression=compression)

    def to_csv(
        self,
        df: "DataFrame",
        path: str,
        header: bool = True,
        separator: str = ",",
    ) -> None:
        self._polars.to_csv(df, path, header=header, separator=separator)

    def to_json(self, df: "DataFrame", path: str, pretty: bool = False) -> None:
        self._polars.to_json(df, path, pretty=pretty)
