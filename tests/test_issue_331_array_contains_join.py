"""
Unit tests for Issue #331: Join doesn't support array_contains() condition.

Tests that join operations work correctly with expression-based conditions like array_contains.
"""

from sparkless.sql import SparkSession
from sparkless import functions as F


class TestIssue331ArrayContainsJoin:
    """Test array_contains() as join condition."""

    def test_array_contains_join_basic(self):
        """Test basic array_contains join."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="left"
            )
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["IDs"] == [1, 2, 3]
            assert rows[0]["Dept"] == "A"
            assert rows[0]["ID"] == 3
            assert rows[1]["Name"] == "Bob"
            assert rows[1]["IDs"] == [4, 5, 6]
            assert rows[1]["Dept"] == "B"
            assert rows[1]["ID"] == 5
        finally:
            spark.stop()

    def test_array_contains_join_inner(self):
        """Test array_contains join with inner join type."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                    {"Name": "Charlie", "IDs": [7, 8, 9]},  # No match
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="inner"
            )
            rows = result.collect()

            assert len(rows) == 2
            names = {row["Name"] for row in rows}
            assert names == {"Alice", "Bob"}
        finally:
            spark.stop()

    def test_array_contains_join_left(self):
        """Test array_contains join with left join type."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                    {"Name": "Charlie", "IDs": [7, 8, 9]},  # No match
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="left"
            )
            rows = result.collect()

            assert len(rows) == 3
            # Find Charlie row (no match)
            charlie_row = next(row for row in rows if row["Name"] == "Charlie")
            assert charlie_row["Dept"] is None
            assert charlie_row["ID"] is None
        finally:
            spark.stop()

    def test_array_contains_join_multiple_matches(self):
        """Test array_contains join with multiple matches."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 1},
                    {"Dept": "B", "ID": 2},
                    {"Dept": "C", "ID": 3},
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="inner"
            )
            rows = result.collect()

            assert len(rows) == 3
            # All rows should have Alice's name and IDs
            for row in rows:
                assert row["Name"] == "Alice"
                assert row["IDs"] == [1, 2, 3]
            # Check all depts are present
            depts = {row["Dept"] for row in rows}
            assert depts == {"A", "B", "C"}
        finally:
            spark.stop()

    def test_array_contains_join_no_matches(self):
        """Test array_contains join with no matches."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 10},  # No match
                    {"Dept": "B", "ID": 20},  # No match
                ]
            )

            # Inner join with no matches should return empty
            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="inner"
            )
            rows = result.collect()
            assert len(rows) == 0

            # Left join with no matches should return left rows with nulls
            result2 = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="left"
            )
            rows2 = result2.collect()
            assert len(rows2) == 1
            assert rows2[0]["Name"] == "Alice"
            assert rows2[0]["Dept"] is None
            assert rows2[0]["ID"] is None
        finally:
            spark.stop()

    def test_array_contains_join_null_arrays(self):
        """Test array_contains join with null arrays."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": None},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="left"
            )
            rows = result.collect()

            assert len(rows) >= 1
            # Alice should match
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["Dept"] == "A"
            # Bob with null array should not match (or match with null)
            bob_rows = [row for row in rows if row["Name"] == "Bob"]
            if bob_rows:
                assert bob_rows[0]["Dept"] is None
        finally:
            spark.stop()

    def test_array_contains_join_null_ids(self):
        """Test array_contains join with null IDs."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": None},
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="left"
            )
            rows = result.collect()

            # Should have at least one match (ID=3)
            assert len(rows) >= 1
            matching_rows = [row for row in rows if row["Dept"] == "A"]
            assert len(matching_rows) >= 1
        finally:
            spark.stop()

    def test_array_contains_join_right(self):
        """Test array_contains join with right join type."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                    {"Dept": "C", "ID": 10},  # No match
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="right"
            )
            rows = result.collect()

            # Should include all right rows
            assert len(rows) >= 2
            depts = {row["Dept"] for row in rows}
            assert "A" in depts
            assert "B" in depts
        finally:
            spark.stop()

    def test_array_contains_join_outer(self):
        """Test array_contains join with outer join type."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                    {"Name": "Charlie", "IDs": [7, 8, 9]},  # No match
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                    {"Dept": "C", "ID": 10},  # No match
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="outer"
            )
            rows = result.collect()

            # Should include matches and unmatched rows from both sides
            assert len(rows) >= 2
            names = {row["Name"] for row in rows if row["Name"] is not None}
            assert "Alice" in names
            assert "Bob" in names
        finally:
            spark.stop()

    def test_array_contains_join_with_select(self):
        """Test array_contains join followed by select."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            result = (
                df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="left")
                .select("Name", "Dept")
            )
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["Dept"] == "A"
        finally:
            spark.stop()

    def test_array_contains_join_with_filter(self):
        """Test array_contains join followed by filter."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            result = (
                df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="left")
                .filter(F.col("Dept") == "A")
            )
            rows = result.collect()

            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["Dept"] == "A"
        finally:
            spark.stop()

    def test_array_contains_join_column_name_conflicts(self):
        """Test array_contains join when DataFrames have column name conflicts."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3], "Value": 10},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Name": "Bob", "ID": 3, "Value": 20},
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="inner"
            )
            rows = result.collect()

            assert len(rows) == 1
            # Both Name and Value columns should be present (may be from left or right)
            assert "Name" in rows[0]
            assert "Value" in rows[0]
        finally:
            spark.stop()

    def test_array_contains_join_empty_dataframes(self):
        """Test array_contains join with empty DataFrames."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            from sparkless.spark_types import StructType, StructField, StringType, ArrayType, IntegerType

            schema1 = StructType(
                [
                    StructField("Name", StringType(), True),
                    StructField("IDs", ArrayType(IntegerType()), True),
                ]
            )
            schema2 = StructType(
                [
                    StructField("Dept", StringType(), True),
                    StructField("ID", IntegerType(), True),
                ]
            )

            df1 = spark.createDataFrame([], schema=schema1)
            df2 = spark.createDataFrame([], schema=schema2)

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="inner"
            )
            rows = result.collect()

            assert len(rows) == 0
        finally:
            spark.stop()

    def test_array_contains_join_backward_compatibility(self):
        """Test that regular column-based joins still work (backward compatibility)."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "ID": 1},
                    {"Name": "Bob", "ID": 2},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 1},
                    {"Dept": "B", "ID": 2},
                ]
            )

            # Regular column-based join should still work
            result = df1.join(df2, on="ID", how="inner")
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["Dept"] == "A"
        finally:
            spark.stop()
