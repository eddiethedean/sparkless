"""
Session Performance and Memory Tracking

This module handles memory tracking and benchmarking for SparkSession.
Extracted from session.py to improve organization.
"""

import time
from typing import Any, Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from sparkless.dataframe import DataFrame


class SessionPerformanceTracker:
    """Tracks memory usage and performance metrics for a Spark session."""

    def __init__(self) -> None:
        """Initialize performance tracker."""
        self._tracked_dataframes: List[Any] = []
        self._approx_memory_usage_bytes = 0
        self._benchmark_results: Dict[str, Dict[str, Any]] = {}

    # -------------------------------------------------------------------------
    # Memory Tracking
    # -------------------------------------------------------------------------

    def track_dataframe(self, df: "DataFrame") -> None:
        """Track DataFrame for approximate memory accounting.

        Args:
            df: DataFrame to track
        """
        self._tracked_dataframes.append(df)
        self._approx_memory_usage_bytes += self._estimate_dataframe_size(df)

    def _estimate_dataframe_size(self, df: "DataFrame") -> int:
        """Very rough size estimate based on rows, columns, and value sizes.

        Args:
            df: DataFrame to estimate size for

        Returns:
            Approximate size in bytes
        """
        num_rows = len(df.data)
        num_cols = len(df.schema.fields)
        # Assume ~32 bytes per cell average (key+value overhead), adjustable
        return num_rows * num_cols * 32

    def get_memory_usage(self) -> int:
        """Return approximate memory usage in bytes for tracked DataFrames.

        Returns:
            Approximate memory usage in bytes
        """
        return self._approx_memory_usage_bytes

    def clear_cache(self) -> None:
        """Clear tracked DataFrames to free memory accounting."""
        self._tracked_dataframes.clear()
        self._approx_memory_usage_bytes = 0

    # -------------------------------------------------------------------------
    # Benchmarking
    # -------------------------------------------------------------------------

    def benchmark_operation(
        self, operation_name: str, func: Any, *args: Any, **kwargs: Any
    ) -> Any:
        """Benchmark an operation and record simple telemetry.

        Args:
            operation_name: Name of the operation
            func: Function to benchmark
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function result

        Records:
            duration_s: Duration in seconds
            memory_used_bytes: Memory used in bytes
            result_size: Size of result
        """
        start_mem = self.get_memory_usage()
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        end_mem = self.get_memory_usage()

        size: int = 1
        try:
            if hasattr(result, "count"):
                size = int(result.count())
            elif hasattr(result, "collect"):
                size = len(result.collect())
            elif hasattr(result, "__len__"):
                size = len(result)
        except Exception:
            size = 1

        self._benchmark_results[operation_name] = {
            "duration_s": max(end_time - start_time, 0.0),
            "memory_used_bytes": max(end_mem - start_mem, 0),
            "result_size": size,
        }
        return result

    def get_benchmark_results(self) -> Dict[str, Dict[str, Any]]:
        """Return a copy of the latest benchmark results.

        Returns:
            Dictionary of operation_name -> metrics
        """
        return dict(self._benchmark_results)
