"""
Tests for issue #287: NAHandler.replace method.

PySpark supports df.na.replace() for mapping values within a DataFrame.
This test verifies that Sparkless supports the same operation.
"""

from sparkless.sql import SparkSession


class TestIssue287NAReplace:
    """Test NAHandler.replace method."""

    def test_na_replace_with_dict_and_subset(self):
        """Test na.replace with dict mapping and subset parameter."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            map_value = {"A": "TypeA", "B": "TypeB"}

            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            # Test na.replace with dict and subset
            result = df.na.replace(map_value, subset=["Type"])

            rows = result.collect()
            assert len(rows) == 2

            # Verify replacements
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Type"] == "TypeA"

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Type"] == "TypeB"

            # Verify Name column unchanged
            assert alice_row["Name"] == "Alice"
            assert bob_row["Name"] == "Bob"
        finally:
            spark.stop()

    def test_na_replace_with_dict_no_subset(self):
        """Test na.replace with dict mapping without subset (applies to all columns)."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            map_value = {"A": "TypeA", "B": "TypeB"}

            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            # Test na.replace with dict but no subset (applies to all columns)
            result = df.na.replace(map_value)

            rows = result.collect()
            assert len(rows) == 2

            # Verify replacements in Type column
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Type"] == "TypeA"

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Type"] == "TypeB"
        finally:
            spark.stop()

    def test_na_replace_single_value(self):
        """Test na.replace with single value replacement."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 1},
                    {"Name": "Charlie", "Value": 2},
                ]
            )

            # Replace 1 with 99 in Value column
            result = df.na.replace(1, 99, subset=["Value"])

            rows = result.collect()
            assert len(rows) == 3

            # Verify replacements
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Value"] == 99

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Value"] == 99

            # Value 2 should remain unchanged
            charlie_row = next((r for r in rows if r["Name"] == "Charlie"), None)
            assert charlie_row is not None
            assert charlie_row["Value"] == 2
        finally:
            spark.stop()

    def test_na_replace_list_with_single_value(self):
        """Test na.replace with list of values replaced by single value."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                    {"Name": "Charlie", "Value": 3},
                ]
            )

            # Replace [1, 2] with 99
            result = df.na.replace([1, 2], 99, subset=["Value"])

            rows = result.collect()
            assert len(rows) == 3

            # Verify replacements
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Value"] == 99

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Value"] == 99

            # Value 3 should remain unchanged
            charlie_row = next((r for r in rows if r["Name"] == "Charlie"), None)
            assert charlie_row is not None
            assert charlie_row["Value"] == 3
        finally:
            spark.stop()

    def test_na_replace_list_with_list(self):
        """Test na.replace with list of values replaced by list of values."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                    {"Name": "Charlie", "Value": 3},
                ]
            )

            # Replace [1, 2] with [10, 20]
            result = df.na.replace([1, 2], [10, 20], subset=["Value"])

            rows = result.collect()
            assert len(rows) == 3

            # Verify replacements
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Value"] == 10

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Value"] == 20

            # Value 3 should remain unchanged
            charlie_row = next((r for r in rows if r["Name"] == "Charlie"), None)
            assert charlie_row is not None
            assert charlie_row["Value"] == 3
        finally:
            spark.stop()

    def test_na_replace_with_string_subset(self):
        """Test na.replace with string subset (single column)."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            map_value = {"A": "TypeA", "B": "TypeB"}

            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            # Test with string subset (should be converted to list)
            result = df.na.replace(map_value, subset="Type")

            rows = result.collect()
            assert len(rows) == 2

            # Verify replacements
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Type"] == "TypeA"

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Type"] == "TypeB"
        finally:
            spark.stop()

    def test_na_replace_with_tuple_subset(self):
        """Test na.replace with tuple subset."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            map_value = {"A": "TypeA", "B": "TypeB"}

            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Category": "A"},
                    {"Name": "Bob", "Type": "B", "Category": "B"},
                ]
            )

            # Test with tuple subset
            result = df.na.replace(map_value, subset=("Type", "Category"))

            rows = result.collect()
            assert len(rows) == 2

            # Verify replacements in both columns
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Type"] == "TypeA"
            assert alice_row["Category"] == "TypeA"

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Type"] == "TypeB"
            assert bob_row["Category"] == "TypeB"
        finally:
            spark.stop()

    def test_na_replace_multiple_columns(self):
        """Test na.replace affecting multiple columns."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Status": "A"},
                    {"Name": "Bob", "Type": "B", "Status": "B"},
                ]
            )

            # Replace in multiple columns
            result = df.na.replace(
                {"A": "TypeA", "B": "TypeB"}, subset=["Type", "Status"]
            )

            rows = result.collect()
            assert len(rows) == 2

            # Verify replacements in both columns
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Type"] == "TypeA"
            assert alice_row["Status"] == "TypeA"

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Type"] == "TypeB"
            assert bob_row["Status"] == "TypeB"
        finally:
            spark.stop()

    def test_na_replace_with_numeric_values(self):
        """Test na.replace with numeric values."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Score": 1.0},
                    {"Name": "Bob", "Score": 2.0},
                    {"Name": "Charlie", "Score": 3.0},
                ]
            )

            # Replace numeric values
            result = df.na.replace({1.0: 10.0, 2.0: 20.0}, subset=["Score"])

            rows = result.collect()
            assert len(rows) == 3

            # Verify replacements
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Score"] == 10.0

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Score"] == 20.0

            # Value 3.0 should remain unchanged
            charlie_row = next((r for r in rows if r["Name"] == "Charlie"), None)
            assert charlie_row is not None
            assert charlie_row["Score"] == 3.0
        finally:
            spark.stop()

    def test_na_replace_no_matches(self):
        """Test na.replace when no values match."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            # Replace values that don't exist
            result = df.na.replace({"X": "TypeX", "Y": "TypeY"}, subset=["Type"])

            rows = result.collect()
            assert len(rows) == 2

            # Values should remain unchanged
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Type"] == "A"

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Type"] == "B"
        finally:
            spark.stop()

    def test_na_replace_partial_matches(self):
        """Test na.replace when only some values match."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                    {"Name": "Charlie", "Type": "C"},
                ]
            )

            # Replace only A and B, C should remain unchanged
            result = df.na.replace({"A": "TypeA", "B": "TypeB"}, subset=["Type"])

            rows = result.collect()
            assert len(rows) == 3

            # Verify replacements
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Type"] == "TypeA"

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Type"] == "TypeB"

            # C should remain unchanged
            charlie_row = next((r for r in rows if r["Name"] == "Charlie"), None)
            assert charlie_row is not None
            assert charlie_row["Type"] == "C"
        finally:
            spark.stop()

    def test_na_replace_empty_dataframe(self):
        """Test na.replace on empty DataFrame."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame([], schema="Name string, Type string")

            # Should not raise error
            result = df.na.replace({"A": "TypeA"}, subset=["Type"])

            rows = result.collect()
            assert len(rows) == 0
        finally:
            spark.stop()

    def test_na_replace_chained_operations(self):
        """Test na.replace with chained DataFrame operations."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Value": 1},
                    {"Name": "Bob", "Type": "B", "Value": 2},
                ]
            )

            # Chain replace with filter
            result = df.na.replace(
                {"A": "TypeA", "B": "TypeB"}, subset=["Type"]
            ).filter("Type = 'TypeA'")

            rows = result.collect()
            assert len(rows) == 1

            # Verify result
            alice_row = rows[0]
            assert alice_row["Name"] == "Alice"
            assert alice_row["Type"] == "TypeA"
        finally:
            spark.stop()
