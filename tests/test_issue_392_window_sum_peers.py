"""
Tests for issue #392: sum() over window returns wrong value when orderBy cols
are subset of partitionBy cols (all rows are peers in RANGE frame semantics).

PySpark uses RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW by default.
Rows with the same ORDER BY value are peers and share the same frame.
When orderBy("Type") and partitionBy("Type"), all rows in partition have same
order key -> both should get partition sum (30), not cumulative (10, 30).
"""

import pytest

from sparkless.sql import SparkSession
import sparkless.sql.functions as F
from sparkless.window import Window


@pytest.fixture
def spark():
    """Create SparkSession for testing."""
    session = SparkSession.builder.appName("Example").getOrCreate()
    yield session
    session.stop()


class TestIssue392WindowSumPeers:
    """Test sum() window with orderBy cols subset of partitionBy (issue #392)."""

    def test_sum_over_window_order_by_same_as_partition_by(self, spark):
        """Exact scenario from issue #392: partitionBy(Type).orderBy(Type)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Type": "A", "Value": 10},
                {"Name": "Bob", "Type": "A", "Value": 20},
            ]
        )
        w = Window().partitionBy("Type").orderBy("Type")
        df = df.withColumn("SumValue", F.sum(df.Value).over(w))
        rows = df.collect()
        # Both rows are peers (same Type) -> both get partition sum 30
        assert len(rows) == 2
        alice = next(r for r in rows if r["Name"] == "Alice")
        bob = next(r for r in rows if r["Name"] == "Bob")
        assert alice["SumValue"] == 30, f"Alice should get 30, got {alice['SumValue']}"
        assert bob["SumValue"] == 30, f"Bob should get 30, got {bob['SumValue']}"

    def test_sum_over_window_order_by_subset_of_partition_by(self, spark):
        """orderBy is subset of partitionBy -> all rows in partition are peers."""
        df = spark.createDataFrame(
            [
                {"Dept": "IT", "Type": "A", "Value": 5},
                {"Dept": "IT", "Type": "A", "Value": 15},
                {"Dept": "HR", "Type": "B", "Value": 10},
            ]
        )
        w = Window.partitionBy("Dept", "Type").orderBy("Type")
        df = df.withColumn("SumValue", F.sum("Value").over(w))
        rows = df.collect()
        # IT partition: both rows have Type=A -> peers -> both get 20
        it_rows = [r for r in rows if r["Dept"] == "IT"]
        assert len(it_rows) == 2
        assert it_rows[0]["SumValue"] == 20
        assert it_rows[1]["SumValue"] == 20
        # HR partition: one row, sum=10
        hr_rows = [r for r in rows if r["Dept"] == "HR"]
        assert len(hr_rows) == 1
        assert hr_rows[0]["SumValue"] == 10

    def test_sum_over_window_order_by_differs_from_partition_running_sum(self, spark):
        """When orderBy is NOT subset of partitionBy, use running sum (cumulative)."""
        df = spark.createDataFrame(
            [
                {"Type": "A", "Name": "Alice", "Value": 10},
                {"Type": "A", "Name": "Bob", "Value": 20},
            ]
        )
        w = Window.partitionBy("Type").orderBy("Name")
        df = df.withColumn("SumValue", F.sum("Value").over(w))
        rows = df.collect()
        # Different order keys -> cumulative: Alice 10, Bob 30
        alice = next(r for r in rows if r["Name"] == "Alice")
        bob = next(r for r in rows if r["Name"] == "Bob")
        assert alice["SumValue"] == 10
        assert bob["SumValue"] == 30
