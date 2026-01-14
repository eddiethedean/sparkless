"""Tests for performance_simulation module."""

import pytest
import time
from sparkless.sql import SparkSession
from sparkless.performance_simulation import (
    MockPerformanceSimulator,
    MockPerformanceSimulatorBuilder,
    performance_simulation,
    create_slow_simulator,
    create_memory_limited_simulator,
    create_high_performance_simulator,
)
from sparkless.errors import PySparkRuntimeError


class TestMockPerformanceSimulator:
    """Test cases for MockPerformanceSimulator."""

    def test_init(self):
        """Test MockPerformanceSimulator initialization with SparkSession."""
        spark = SparkSession("test")
        perf_sim = MockPerformanceSimulator(spark)

        assert perf_sim.spark_session == spark
        assert perf_sim.slowdown_factor == 1.0
        assert perf_sim.memory_limit is None
        assert perf_sim.performance_metrics["total_operations"] == 0
        assert perf_sim.performance_metrics["total_time"] == 0.0
        assert perf_sim.performance_metrics["memory_usage"] == 0
        assert perf_sim.performance_metrics["slowdown_applied"] == 0
        assert perf_sim.performance_metrics["memory_limits_hit"] == 0

    def test_set_slowdown_valid(self):
        """Test setting valid slowdown factors."""
        spark = SparkSession("test")
        perf_sim = MockPerformanceSimulator(spark)

        # Test various valid factors
        perf_sim.set_slowdown(1.0)
        assert perf_sim.slowdown_factor == 1.0

        perf_sim.set_slowdown(2.0)
        assert perf_sim.slowdown_factor == 2.0

        perf_sim.set_slowdown(0.5)
        assert perf_sim.slowdown_factor == 0.5

        perf_sim.set_slowdown(0.0)
        assert perf_sim.slowdown_factor == 0.0

    def test_set_slowdown_invalid(self):
        """Test negative slowdown factor raises ValueError."""
        spark = SparkSession("test")
        perf_sim = MockPerformanceSimulator(spark)

        with pytest.raises(ValueError, match="Slowdown factor must be non-negative"):
            perf_sim.set_slowdown(-1.0)

    def test_set_memory_limit_valid(self):
        """Test setting valid memory limits."""
        spark = SparkSession("test")
        perf_sim = MockPerformanceSimulator(spark)

        perf_sim.set_memory_limit(1000)
        assert perf_sim.memory_limit == 1000

        perf_sim.set_memory_limit(0)
        assert perf_sim.memory_limit == 0

        perf_sim.set_memory_limit(1000000)
        assert perf_sim.memory_limit == 1000000

    def test_set_memory_limit_invalid(self):
        """Test negative memory limit raises ValueError."""
        spark = SparkSession("test")
        perf_sim = MockPerformanceSimulator(spark)

        with pytest.raises(ValueError, match="Memory limit must be non-negative"):
            perf_sim.set_memory_limit(-1)

    def test_check_memory_usage_within_limit(self):
        """Test memory check passes when within limit."""
        spark = SparkSession("test")
        perf_sim = MockPerformanceSimulator(spark)
        perf_sim.set_memory_limit(1000)

        # Should not raise
        perf_sim.check_memory_usage(500)
        perf_sim.check_memory_usage(1000)
        perf_sim.check_memory_usage(0)

    def test_check_memory_usage_exceeds_limit(self):
        """Test memory check raises PySparkRuntimeError when exceeded."""
        spark = SparkSession("test")
        perf_sim = MockPerformanceSimulator(spark)
        perf_sim.set_memory_limit(1000)

        with pytest.raises(PySparkRuntimeError, match="Out of memory"):
            perf_sim.check_memory_usage(1001)

        with pytest.raises(PySparkRuntimeError, match="Out of memory"):
            perf_sim.check_memory_usage(2000)

        # Verify metrics are updated
        assert perf_sim.performance_metrics["memory_limits_hit"] == 2

    def test_check_memory_usage_no_limit(self):
        """Test memory check passes when no limit set."""
        spark = SparkSession("test")
        perf_sim = MockPerformanceSimulator(spark)

        # Should not raise even with large data
        perf_sim.check_memory_usage(1000000)
        perf_sim.check_memory_usage(0)

    def test_apply_slowdown(self):
        """Test slowdown factor is applied to operations."""
        spark = SparkSession("test")
        perf_sim = MockPerformanceSimulator(spark)
        perf_sim.set_slowdown(2.0)

        def dummy_operation():
            return "result"

        start_time = time.time()
        result = perf_sim.simulate_slow_operation(dummy_operation)
        end_time = time.time()

        assert result == "result"
        # Should have some delay (at least 1ms per slowdown unit)
        assert (end_time - start_time) >= 0.001
        assert perf_sim.performance_metrics["slowdown_applied"] == 1

    def test_record_metrics(self):
        """Test performance metrics are recorded correctly."""
        spark = SparkSession("test")
        perf_sim = MockPerformanceSimulator(spark)

        def dummy_operation():
            return "result"

        # Execute multiple operations
        perf_sim.simulate_slow_operation(dummy_operation)
        perf_sim.simulate_slow_operation(dummy_operation)
        perf_sim.simulate_slow_operation(dummy_operation)

        metrics = perf_sim.get_performance_metrics()
        assert metrics["total_operations"] == 3
        assert metrics["total_time"] > 0
        assert "average_time_per_operation" in metrics
        assert metrics["average_time_per_operation"] > 0

    def test_reset_metrics(self):
        """Test metrics can be reset."""
        spark = SparkSession("test")
        perf_sim = MockPerformanceSimulator(spark)

        # Execute some operations
        def dummy_operation():
            return "result"

        perf_sim.simulate_slow_operation(dummy_operation)
        assert perf_sim.performance_metrics["total_operations"] == 1

        # Reset metrics
        perf_sim.reset_metrics()
        assert perf_sim.performance_metrics["total_operations"] == 0
        assert perf_sim.performance_metrics["total_time"] == 0.0
        assert perf_sim.performance_metrics["memory_usage"] == 0
        assert perf_sim.performance_metrics["slowdown_applied"] == 0
        assert perf_sim.performance_metrics["memory_limits_hit"] == 0

    def test_context_manager(self):
        """Test using simulator as context manager."""
        spark = SparkSession("test")

        with performance_simulation(
            spark, slowdown_factor=2.0, memory_limit=1000
        ) as perf_sim:
            assert isinstance(perf_sim, MockPerformanceSimulator)
            assert perf_sim.slowdown_factor == 2.0
            assert perf_sim.memory_limit == 1000

        # Verify simulation is disabled after context exit
        # (original methods should be restored)
        assert hasattr(spark, "createDataFrame")

    def test_enable_disable_performance_simulation(self):
        """Test enabling and disabling performance simulation."""
        spark = SparkSession("test")
        perf_sim = MockPerformanceSimulator(spark)

        # Store original method
        original_createDataFrame = spark.createDataFrame

        # Enable simulation
        perf_sim.enable_performance_simulation()
        assert spark.createDataFrame != original_createDataFrame

        # Disable simulation
        perf_sim.disable_performance_simulation()
        assert spark.createDataFrame == original_createDataFrame

    def test_wrap_method_with_memory_check(self):
        """Test wrapped methods check memory limits."""
        spark = SparkSession("test")
        perf_sim = MockPerformanceSimulator(spark)
        perf_sim.set_memory_limit(10)
        perf_sim.enable_performance_simulation()

        # Should raise error when exceeding memory limit
        large_data = [{"x": i} for i in range(20)]
        with pytest.raises(PySparkRuntimeError, match="Out of memory"):
            spark.createDataFrame(large_data)

        # Should work within limit
        small_data = [{"x": i} for i in range(5)]
        df = spark.createDataFrame(small_data)
        assert df.count() == 5

        perf_sim.disable_performance_simulation()


class TestMockPerformanceSimulatorBuilder:
    """Test cases for MockPerformanceSimulatorBuilder."""

    def test_builder_init(self):
        """Test MockPerformanceSimulatorBuilder initialization."""
        spark = SparkSession("test")
        builder = MockPerformanceSimulatorBuilder(spark)

        assert builder.spark_session == spark
        assert isinstance(builder.perf_sim, MockPerformanceSimulator)

    def test_builder_slowdown(self):
        """Test builder slowdown method."""
        spark = SparkSession("test")
        builder = MockPerformanceSimulatorBuilder(spark)

        result = builder.slowdown(2.0)
        assert result is builder  # Should return self for chaining
        assert builder.perf_sim.slowdown_factor == 2.0

    def test_builder_memory_limit(self):
        """Test builder memory_limit method."""
        spark = SparkSession("test")
        builder = MockPerformanceSimulatorBuilder(spark)

        result = builder.memory_limit(1000)
        assert result is builder  # Should return self for chaining
        assert builder.perf_sim.memory_limit == 1000

    def test_builder_enable_monitoring(self):
        """Test builder enable_monitoring method."""
        spark = SparkSession("test")
        builder = MockPerformanceSimulatorBuilder(spark)

        original_createDataFrame = spark.createDataFrame
        result = builder.enable_monitoring()
        assert result is builder  # Should return self for chaining
        assert spark.createDataFrame != original_createDataFrame

        # Cleanup
        builder.perf_sim.disable_performance_simulation()

    def test_builder_build(self):
        """Test builder build method."""
        spark = SparkSession("test")
        builder = MockPerformanceSimulatorBuilder(spark)
        builder.slowdown(2.0).memory_limit(1000)

        perf_sim = builder.build()
        assert isinstance(perf_sim, MockPerformanceSimulator)
        assert perf_sim.slowdown_factor == 2.0
        assert perf_sim.memory_limit == 1000

    def test_builder_fluent_interface(self):
        """Test builder fluent interface (method chaining)."""
        spark = SparkSession("test")
        perf_sim = (
            MockPerformanceSimulatorBuilder(spark)
            .slowdown(2.0)
            .memory_limit(1000)
            .build()
        )

        assert perf_sim.slowdown_factor == 2.0
        assert perf_sim.memory_limit == 1000


class TestConvenienceFunctions:
    """Test cases for convenience functions."""

    def test_create_slow_simulator(self):
        """Test create_slow_simulator function."""
        spark = SparkSession("test")
        perf_sim = create_slow_simulator(spark, slowdown_factor=3.0)

        assert isinstance(perf_sim, MockPerformanceSimulator)
        assert perf_sim.slowdown_factor == 3.0

    def test_create_slow_simulator_default(self):
        """Test create_slow_simulator with default factor."""
        spark = SparkSession("test")
        perf_sim = create_slow_simulator(spark)

        assert isinstance(perf_sim, MockPerformanceSimulator)
        assert perf_sim.slowdown_factor == 2.0  # Default

    def test_create_memory_limited_simulator(self):
        """Test create_memory_limited_simulator function."""
        spark = SparkSession("test")
        perf_sim = create_memory_limited_simulator(spark, memory_limit=500)

        assert isinstance(perf_sim, MockPerformanceSimulator)
        assert perf_sim.memory_limit == 500

    def test_create_memory_limited_simulator_default(self):
        """Test create_memory_limited_simulator with default limit."""
        spark = SparkSession("test")
        perf_sim = create_memory_limited_simulator(spark)

        assert isinstance(perf_sim, MockPerformanceSimulator)
        assert perf_sim.memory_limit == 1000  # Default

    def test_create_high_performance_simulator(self):
        """Test create_high_performance_simulator function."""
        spark = SparkSession("test")
        perf_sim = create_high_performance_simulator(spark)

        assert isinstance(perf_sim, MockPerformanceSimulator)
        assert perf_sim.slowdown_factor == 0.5  # Faster than normal
