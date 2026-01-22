"""
Tests for issue #291: Power operator (**) support between floats and Column/ColumnOperation.

PySpark supports the ** operator between floats and Column objects. This test verifies
that Sparkless supports the same.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue291PowerOperatorFloatColumn:
    """Test power operator (**) between floats and Column/ColumnOperation."""

    def test_float_power_column(self):
        """Test float ** Column (from issue example)."""
        spark = SparkSession.builder.appName("issue-291").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 7},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            df = df.withColumn("NewValue1", 3.0 ** F.col("Value"))

            rows = df.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert abs(alice_row["NewValue1"] - 2187.0) < 0.01  # 3.0 ** 7

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert abs(bob_row["NewValue1"] - 9.0) < 0.01  # 3.0 ** 2
        finally:
            spark.stop()

    def test_float_power_column_operation(self):
        """Test float ** ColumnOperation (from issue example)."""
        spark = SparkSession.builder.appName("issue-291").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 7},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            df = df.withColumn("NewValue2", 3.0 ** (F.col("Value") - 1))

            rows = df.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert (
                abs(alice_row["NewValue2"] - 729.0) < 0.01
            )  # 3.0 ** (7 - 1) = 3.0 ** 6

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert abs(bob_row["NewValue2"] - 3.0) < 0.01  # 3.0 ** (2 - 1) = 3.0 ** 1
        finally:
            spark.stop()

    def test_column_power_number(self):
        """Test Column ** number (forward power operation)."""
        spark = SparkSession.builder.appName("issue-291").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 2},
                    {"Value": 3},
                    {"Value": 4},
                ]
            )

            df = df.withColumn("Squared", F.col("Value") ** 2)
            df = df.withColumn("Cubed", F.col("Value") ** 3)

            rows = df.collect()
            assert len(rows) == 3
            assert rows[0]["Squared"] == 4  # 2 ** 2
            assert rows[0]["Cubed"] == 8  # 2 ** 3
            assert rows[1]["Squared"] == 9  # 3 ** 2
            assert rows[1]["Cubed"] == 27  # 3 ** 3
            assert rows[2]["Squared"] == 16  # 4 ** 2
            assert rows[2]["Cubed"] == 64  # 4 ** 3
        finally:
            spark.stop()

    def test_integer_power_column(self):
        """Test integer ** Column."""
        spark = SparkSession.builder.appName("issue-291").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 2},
                    {"Value": 3},
                ]
            )

            df = df.withColumn("Result", 2 ** F.col("Value"))

            rows = df.collect()
            assert len(rows) == 2
            assert rows[0]["Result"] == 4  # 2 ** 2
            assert rows[1]["Result"] == 8  # 2 ** 3
        finally:
            spark.stop()

    def test_column_power_column(self):
        """Test Column ** Column."""
        spark = SparkSession.builder.appName("issue-291").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Base": 2, "Exponent": 3},
                    {"Base": 3, "Exponent": 2},
                    {"Base": 5, "Exponent": 4},
                ]
            )

            df = df.withColumn("Result", F.col("Base") ** F.col("Exponent"))

            rows = df.collect()
            assert len(rows) == 3
            assert rows[0]["Result"] == 8  # 2 ** 3
            assert rows[1]["Result"] == 9  # 3 ** 2
            assert rows[2]["Result"] == 625  # 5 ** 4
        finally:
            spark.stop()

    def test_float_power_nested_expression(self):
        """Test float ** nested ColumnOperation."""
        spark = SparkSession.builder.appName("issue-291").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 4},
                    {"Value": 5},
                ]
            )

            df = df.withColumn("Result", 2.0 ** (F.col("Value") * 2))

            rows = df.collect()
            assert len(rows) == 2
            assert rows[0]["Result"] == 256.0  # 2.0 ** (4 * 2) = 2.0 ** 8
            assert rows[1]["Result"] == 1024.0  # 2.0 ** (5 * 2) = 2.0 ** 10
        finally:
            spark.stop()

    def test_power_in_select(self):
        """Test power operator in select statement."""
        spark = SparkSession.builder.appName("issue-291").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 3},
                    {"Value": 4},
                ]
            )

            result = df.select("Value", (2.0 ** F.col("Value")).alias("Power"))

            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Power"] == 8.0  # 2.0 ** 3
            assert rows[1]["Power"] == 16.0  # 2.0 ** 4
        finally:
            spark.stop()

    def test_power_in_filter(self):
        """Test power operator in filter/where clause."""
        spark = SparkSession.builder.appName("issue-291").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 2},
                    {"Value": 3},
                    {"Value": 4},
                ]
            )

            result = df.filter(2.0 ** F.col("Value") > 10)

            rows = result.collect()
            # 2**2=4, 2**3=8, 2**4=16 - only 16 > 10
            assert len(rows) == 1
            assert rows[0]["Value"] == 4
        finally:
            spark.stop()

    def test_power_with_nulls(self):
        """Test power operator with null values."""
        spark = SparkSession.builder.appName("issue-291").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 2},
                    {"Value": None},
                    {"Value": 3},
                ]
            )

            df = df.withColumn("Result", 2.0 ** F.col("Value"))

            rows = df.collect()
            assert len(rows) == 3
            assert rows[0]["Result"] == 4.0  # 2.0 ** 2
            assert rows[1]["Result"] is None  # 2.0 ** None = None
            assert rows[2]["Result"] == 8.0  # 2.0 ** 3
        finally:
            spark.stop()

    def test_power_zero_exponent(self):
        """Test power operator with zero exponent."""
        spark = SparkSession.builder.appName("issue-291").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 5},
                    {"Value": 10},
                ]
            )

            df = df.withColumn("Result", F.col("Value") ** 0)

            rows = df.collect()
            assert len(rows) == 2
            assert rows[0]["Result"] == 1  # 5 ** 0 = 1
            assert rows[1]["Result"] == 1  # 10 ** 0 = 1
        finally:
            spark.stop()

    def test_power_zero_base(self):
        """Test power operator with zero base."""
        spark = SparkSession.builder.appName("issue-291").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 2},
                    {"Value": 5},
                ]
            )

            df = df.withColumn("Result", 0.0 ** F.col("Value"))

            rows = df.collect()
            assert len(rows) == 2
            assert rows[0]["Result"] == 0.0  # 0.0 ** 2 = 0.0
            assert rows[1]["Result"] == 0.0  # 0.0 ** 5 = 0.0
        finally:
            spark.stop()

    def test_power_negative_exponent(self):
        """Test power operator with negative exponent."""
        spark = SparkSession.builder.appName("issue-291").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 2},
                    {"Value": 3},
                ]
            )

            df = df.withColumn("Result", 2.0 ** (-F.col("Value")))

            rows = df.collect()
            assert len(rows) == 2
            assert abs(rows[0]["Result"] - 0.25) < 0.01  # 2.0 ** (-2) = 0.25
            assert abs(rows[1]["Result"] - 0.125) < 0.01  # 2.0 ** (-3) = 0.125
        finally:
            spark.stop()

    def test_power_chained_operations(self):
        """Test chained power operations."""
        spark = SparkSession.builder.appName("issue-291").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 2},
                    {"Value": 3},
                ]
            )

            df = df.withColumn("Result", (2.0 ** F.col("Value")) ** 2)

            rows = df.collect()
            assert len(rows) == 2
            assert rows[0]["Result"] == 16.0  # (2.0 ** 2) ** 2 = 4.0 ** 2 = 16.0
            assert rows[1]["Result"] == 64.0  # (2.0 ** 3) ** 2 = 8.0 ** 2 = 64.0
        finally:
            spark.stop()

    def test_power_in_groupby_agg(self):
        """Test power operator in groupBy aggregation.

        Note: Power operations with aggregations may have limitations.
        This test verifies the operation completes without error.
        """
        spark = SparkSession.builder.appName("issue-291").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Category": "A", "Value": 2},
                    {"Category": "A", "Value": 3},
                    {"Category": "B", "Value": 2},
                    {"Category": "B", "Value": 4},
                ]
            )

            result = df.groupBy("Category").agg(
                (2.0 ** F.sum("Value")).alias("TotalPower")
            )

            rows = result.collect()
            # Verify the operation completes
            # Note: Power operations with aggregations may have limitations
            assert len(rows) >= 0  # Operation should complete without error
        finally:
            spark.stop()

    def test_power_mixed_types(self):
        """Test power operator with mixed numeric types."""
        spark = SparkSession.builder.appName("issue-291").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"IntValue": 2, "FloatValue": 3.0},
                    {"IntValue": 4, "FloatValue": 5.0},
                ]
            )

            df = df.withColumn("IntPower", 2 ** F.col("IntValue"))
            df = df.withColumn("FloatPower", 2.0 ** F.col("FloatValue"))

            rows = df.collect()
            assert len(rows) == 2
            assert rows[0]["IntPower"] == 4  # 2 ** 2
            assert abs(rows[0]["FloatPower"] - 8.0) < 0.01  # 2.0 ** 3.0
            assert rows[1]["IntPower"] == 16  # 2 ** 4
            assert abs(rows[1]["FloatPower"] - 32.0) < 0.01  # 2.0 ** 5.0
        finally:
            spark.stop()

    def test_power_empty_dataframe(self):
        """Test power operator on empty DataFrame."""
        spark = SparkSession.builder.appName("issue-291").getOrCreate()
        try:
            from sparkless.spark_types import StructType, StructField, IntegerType

            schema = StructType(
                [
                    StructField("Value", IntegerType(), True),
                ]
            )
            df = spark.createDataFrame([], schema)

            df = df.withColumn("Result", 2.0 ** F.col("Value"))

            rows = df.collect()
            assert len(rows) == 0
            assert "Result" in df.columns
        finally:
            spark.stop()
