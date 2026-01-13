"""
Performance simulation framework for Sparkless.

This module provides performance simulation capabilities that allow testing
of performance-related scenarios by simulating delays, memory constraints,
and other performance characteristics without requiring actual large datasets.

Key Features:
    - Configurable slowdown simulation
    - Memory limit simulation
    - Performance monitoring and metrics
    - Realistic performance testing scenarios
    - Easy integration with existing test frameworks

Example:
    >>> from sparkless.sql import SparkSession
    >>> from sparkless.performance_simulation import MockPerformanceSimulator
    >>>
    >>> spark = SparkSession("test")
    >>> perf_sim = MockPerformanceSimulator(spark)
    >>>
    >>> # Simulate slow operations
    >>> perf_sim.set_slowdown(2.0)  # 2x slower
    >>>
    >>> # Simulate memory constraints
    >>> perf_sim.set_memory_limit(1000)  # 1000 rows max
"""

import time
from typing import Any, Callable, Dict, Optional
from contextlib import contextmanager


class MockPerformanceSimulator:
    """Performance simulation framework for Sparkless operations.

    Provides configurable performance simulation including slowdown factors,
    memory limits, and performance monitoring for realistic testing scenarios.

    Attributes:
        spark_session: The SparkSession instance to simulate performance on.
        slowdown_factor: Multiplier for operation delays (1.0 = normal speed).
        memory_limit: Maximum number of rows allowed in operations.
        performance_metrics: Dictionary storing performance statistics.

    Example:
        >>> perf_sim = MockPerformanceSimulator(spark)
        >>> perf_sim.set_slowdown(2.0)
        >>> perf_sim.set_memory_limit(1000)
    """

    def __init__(self, spark_session: Any) -> None:
        """Initialize MockPerformanceSimulator.

        Args:
            spark_session: SparkSession instance to simulate performance on.
        """
        self.spark_session = spark_session
        self.slowdown_factor = 1.0
        self.memory_limit: Optional[int] = None
        self.performance_metrics = {
            "total_operations": 0,
            "total_time": 0.0,
            "memory_usage": 0,
            "slowdown_applied": 0,
            "memory_limits_hit": 0,
        }
        self._original_methods: Dict[str, Any] = {}

    def set_slowdown(self, factor: float) -> None:
        """Set slowdown factor for operations.

        Args:
            factor: Slowdown multiplier (1.0 = normal speed, 2.0 = 2x slower).
        """
        if factor < 0:
            raise ValueError("Slowdown factor must be non-negative")
        self.slowdown_factor = factor

    def set_memory_limit(self, max_rows: int) -> None:
        """Set memory limit for operations.

        Args:
            max_rows: Maximum number of rows allowed in operations.
        """
        if max_rows < 0:
            raise ValueError("Memory limit must be non-negative")
        self.memory_limit = max_rows

    def check_memory_usage(self, data_size: int) -> None:
        """Check if operation exceeds memory limit.

        Args:
            data_size: Number of rows in the operation.

        Raises:
            PySparkRuntimeError: If memory limit is exceeded.
        """
        if self.memory_limit is not None and data_size > self.memory_limit:
            from .errors import PySparkRuntimeError

            self.performance_metrics["memory_limits_hit"] += 1
            raise PySparkRuntimeError(
                f"Out of memory: {data_size} rows exceeds limit of {self.memory_limit}"
            )

    def simulate_slow_operation(
        self, operation: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        """Simulate a slow operation with configurable delay.

        Args:
            operation: Function to execute with slowdown.
            *args: Positional arguments for the operation.
            **kwargs: Keyword arguments for the operation.

        Returns:
            Result of the operation.
        """
        start_time = time.time()

        # Apply slowdown
        if self.slowdown_factor > 1.0:
            delay = 0.001 * (self.slowdown_factor - 1.0)  # 1ms per slowdown unit
            time.sleep(delay)
            self.performance_metrics["slowdown_applied"] += 1

        # Execute operation
        result = operation(*args, **kwargs)

        # Record metrics
        end_time = time.time()
        self.performance_metrics["total_operations"] += 1
        self.performance_metrics["total_time"] += end_time - start_time

        return result

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics.

        Returns:
            Dictionary containing performance statistics.
        """
        metrics = self.performance_metrics.copy()
        if metrics["total_operations"] > 0:
            metrics["average_time_per_operation"] = (
                metrics["total_time"] / metrics["total_operations"]
            )
        else:
            metrics["average_time_per_operation"] = 0.0
        return metrics

    def reset_metrics(self) -> None:
        """Reset performance metrics to zero."""
        self.performance_metrics = {
            "total_operations": 0,
            "total_time": 0.0,
            "memory_usage": 0,
            "slowdown_applied": 0,
            "memory_limits_hit": 0,
        }

    def enable_performance_simulation(self) -> None:
        """Enable performance simulation by wrapping methods."""
        # Store original methods
        self._original_methods = {
            "createDataFrame": self.spark_session.createDataFrame,
            "table": self.spark_session.table,
            "sql": self.spark_session.sql,
        }

        # Wrap methods with performance simulation
        self.spark_session.createDataFrame = self._wrap_method("createDataFrame")
        self.spark_session.table = self._wrap_method("table")
        self.spark_session.sql = self._wrap_method("sql")

    def disable_performance_simulation(self) -> None:
        """Disable performance simulation by restoring original methods."""
        for method_name, original_method in self._original_methods.items():
            setattr(self.spark_session, method_name, original_method)
        self._original_methods.clear()

    def _wrap_method(self, method_name: str) -> Callable[..., Any]:
        """Wrap a method with performance simulation logic."""
        original_method = getattr(self.spark_session, method_name)

        def wrapped_method(*args: Any, **kwargs: Any) -> Any:
            # Check memory limits for createDataFrame
            if method_name == "createDataFrame" and args:
                data = args[0]
                if isinstance(data, list):
                    self.check_memory_usage(len(data))

            # Simulate slow operation
            return self.simulate_slow_operation(original_method, *args, **kwargs)

        return wrapped_method


class MockPerformanceSimulatorBuilder:
    """Builder pattern for creating common performance simulation scenarios.

    Provides convenient methods for setting up common performance testing
    scenarios without requiring manual configuration.

    Example:
        >>> builder = MockPerformanceSimulatorBuilder(spark)
        >>> perf_sim = (builder
        ...     .slowdown(2.0)
        ...     .memory_limit(1000)
        ...     .enable_monitoring()
        ...     .build())
    """

    def __init__(self, spark_session: Any) -> None:
        """Initialize MockPerformanceSimulatorBuilder.

        Args:
            spark_session: SparkSession instance to build performance simulation for.
        """
        self.spark_session = spark_session
        self.perf_sim = MockPerformanceSimulator(spark_session)

    def slowdown(self, factor: float) -> "MockPerformanceSimulatorBuilder":
        """Set slowdown factor.

        Args:
            factor: Slowdown multiplier.
        """
        self.perf_sim.set_slowdown(factor)
        return self

    def memory_limit(self, max_rows: int) -> "MockPerformanceSimulatorBuilder":
        """Set memory limit.

        Args:
            max_rows: Maximum number of rows allowed.
        """
        self.perf_sim.set_memory_limit(max_rows)
        return self

    def enable_monitoring(self) -> "MockPerformanceSimulatorBuilder":
        """Enable performance monitoring."""
        self.perf_sim.enable_performance_simulation()
        return self

    def build(self) -> MockPerformanceSimulator:
        """Build the performance simulator with all configured settings.

        Returns:
            MockPerformanceSimulator instance with all settings applied.
        """
        return self.perf_sim


@contextmanager
def performance_simulation(
    spark_session: Any, slowdown_factor: float = 1.0, memory_limit: Optional[int] = None
) -> Any:
    """Context manager for temporary performance simulation.

    Args:
        spark_session: SparkSession instance.
        slowdown_factor: Slowdown multiplier for operations.
        memory_limit: Maximum number of rows allowed.

    Example:
        >>> with performance_simulation(spark, slowdown_factor=2.0, memory_limit=1000):
        ...     df = spark.createDataFrame(data)
    """
    perf_sim = MockPerformanceSimulator(spark_session)
    perf_sim.set_slowdown(slowdown_factor)
    if memory_limit:
        perf_sim.set_memory_limit(memory_limit)

    try:
        perf_sim.enable_performance_simulation()
        yield perf_sim
    finally:
        perf_sim.disable_performance_simulation()


# Convenience functions for common performance scenarios
def create_slow_simulator(
    spark_session: Any, slowdown_factor: float = 2.0
) -> MockPerformanceSimulator:
    """Create a simulator that slows down operations.

    Args:
        spark_session: SparkSession instance.
        slowdown_factor: Slowdown multiplier.

    Returns:
        MockPerformanceSimulator configured for slow operations.
    """
    return (
        MockPerformanceSimulatorBuilder(spark_session).slowdown(slowdown_factor).build()
    )


def create_memory_limited_simulator(
    spark_session: Any, memory_limit: int = 1000
) -> MockPerformanceSimulator:
    """Create a simulator that limits memory usage.

    Args:
        spark_session: SparkSession instance.
        memory_limit: Maximum number of rows allowed.

    Returns:
        MockPerformanceSimulator configured for memory limits.
    """
    return (
        MockPerformanceSimulatorBuilder(spark_session)
        .memory_limit(memory_limit)
        .build()
    )


def create_high_performance_simulator(spark_session: Any) -> MockPerformanceSimulator:
    """Create a simulator optimized for high performance.

    Args:
        spark_session: SparkSession instance.

    Returns:
        MockPerformanceSimulator configured for high performance.
    """
    return MockPerformanceSimulatorBuilder(spark_session).slowdown(0.5).build()
