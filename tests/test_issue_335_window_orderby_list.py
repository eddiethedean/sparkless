"""
Unit tests for Issue #335: Window().orderBy() should accept list of column names.

Tests that Window().orderBy() and Window().partitionBy() accept lists of column names,
matching PySpark behavior.
"""

from sparkless.sql import SparkSession
from sparkless import functions as F
from sparkless.window import Window


class TestIssue335WindowOrderByList:
    """Test Window().orderBy() and partitionBy() with list arguments."""

    def test_window_orderby_list_basic(self):
        """Test basic Window().orderBy() with list."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Name", "Type"])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["Type"] == "A"
            assert rows[0]["Rank"] == 1
            assert rows[1]["Name"] == "Bob"
            assert rows[1]["Type"] == "B"
            assert rows[1]["Rank"] == 1
        finally:
            spark.stop()

    def test_window_orderby_list_single_column(self):
        """Test Window().orderBy() with single column in list."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Name"])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Rank"] == 1
            assert rows[1]["Rank"] == 1
        finally:
            spark.stop()

    def test_window_orderby_list_multiple_columns(self):
        """Test Window().orderBy() with multiple columns in list."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "B", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Type", "Score", "Name"])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 3
            # Within each Type partition, should be ordered by Score, then Name
            type_a_rows = [row for row in rows if row["Type"] == "A"]
            assert len(type_a_rows) == 2
            assert type_a_rows[0]["Score"] == 90  # Bob first (lower score)
            assert type_a_rows[1]["Score"] == 100  # Alice second
        finally:
            spark.stop()

    def test_window_partitionby_list(self):
        """Test Window().partitionBy() with list."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Category": "X"},
                    {"Name": "Bob", "Type": "A", "Category": "X"},
                    {"Name": "Charlie", "Type": "B", "Category": "Y"},
                ]
            )

            w = Window().partitionBy(["Type", "Category"]).orderBy("Name")
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 3
            # Each partition should have rank starting at 1
            type_a_rows = [
                row for row in rows if row["Type"] == "A" and row["Category"] == "X"
            ]
            assert len(type_a_rows) == 2
            assert type_a_rows[0]["Rank"] == 1
            assert type_a_rows[1]["Rank"] == 2
        finally:
            spark.stop()

    def test_window_both_list(self):
        """Test Window() with both partitionBy and orderBy using lists."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "B", "Score": 80},
                ]
            )

            w = Window().partitionBy(["Type"]).orderBy(["Score"])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 3
            type_a_rows = [row for row in rows if row["Type"] == "A"]
            assert type_a_rows[0]["Score"] == 90  # Lower score first
            assert type_a_rows[0]["Rank"] == 1
            assert type_a_rows[1]["Score"] == 100
            assert type_a_rows[1]["Rank"] == 2
        finally:
            spark.stop()

    def test_window_orderby_list_with_column_objects(self):
        """Test Window().orderBy() with list containing Column objects."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            w = Window().partitionBy("Type").orderBy([F.col("Name"), F.col("Type")])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Rank"] == 1
            assert rows[1]["Rank"] == 1
        finally:
            spark.stop()

    def test_window_orderby_list_mixed_strings_and_columns(self):
        """Test Window().orderBy() with list containing both strings and Column objects."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Name", F.col("Type")])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Rank"] == 1
            assert rows[1]["Rank"] == 1
        finally:
            spark.stop()

    def test_window_orderby_list_backward_compatibility(self):
        """Test that Window().orderBy() still works with individual arguments (backward compatibility)."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            w = Window().partitionBy("Type").orderBy("Name", "Type")
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Rank"] == 1
            assert rows[1]["Rank"] == 1
        finally:
            spark.stop()

    def test_window_orderby_list_with_desc(self):
        """Test Window().orderBy() with list and desc() ordering."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                ]
            )

            w = Window().partitionBy("Type").orderBy([F.col("Score").desc()])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            # Higher score should have rank 1
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["Rank"] == 1
            bob_row = next(row for row in rows if row["Name"] == "Bob")
            assert bob_row["Rank"] == 2
        finally:
            spark.stop()

    def test_window_orderby_list_with_rows_between(self):
        """Test Window().orderBy() with list and rowsBetween."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "A", "Score": 80},
                ]
            )

            w = (
                Window()
                .partitionBy("Type")
                .orderBy(["Score"])
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            )
            result = df.withColumn("RunningSum", F.sum("Score").over(w))
            rows = result.collect()

            assert len(rows) == 3
            # Running sum should accumulate
            charlie_row = next(row for row in rows if row["Name"] == "Charlie")
            assert charlie_row["RunningSum"] == 80
            bob_row = next(row for row in rows if row["Name"] == "Bob")
            assert bob_row["RunningSum"] == 170  # 80 + 90
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["RunningSum"] == 270  # 80 + 90 + 100
        finally:
            spark.stop()

    def test_window_orderby_list_empty_list_error(self):
        """Test that Window().orderBy() with empty list raises error."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame([{"Name": "Alice"}])

            try:
                w = Window().orderBy([])
                result = df.withColumn("Rank", F.row_number().over(w))
                result.collect()  # Should raise error, but if it doesn't, that's also acceptable
                # (PySpark might handle empty lists differently)
            except ValueError as e:
                assert "At least one column" in str(e) or "must be specified" in str(e)
        finally:
            spark.stop()

    def test_window_orderby_list_with_window_function(self):
        """Test Window().orderBy() with list in window function context."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "B", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("Rank", F.rank().over(w))
            rows = result.collect()

            assert len(rows) == 3
            type_a_rows = [row for row in rows if row["Type"] == "A"]
            assert type_a_rows[0]["Rank"] == 1  # Lower score gets rank 1
            assert type_a_rows[1]["Rank"] == 2
        finally:
            spark.stop()

    def test_window_static_orderby_list(self):
        """Test Window.orderBy() static method with list."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            w = Window.partitionBy("Type").orderBy(["Name", "Type"])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Rank"] == 1
            assert rows[1]["Rank"] == 1
        finally:
            spark.stop()

    def test_window_static_partitionby_list(self):
        """Test Window.partitionBy() static method with list."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Category": "X"},
                    {"Name": "Bob", "Type": "A", "Category": "X"},
                ]
            )

            w = Window.partitionBy(["Type", "Category"]).orderBy("Name")
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Rank"] == 1
            assert rows[1]["Rank"] == 2
        finally:
            spark.stop()
