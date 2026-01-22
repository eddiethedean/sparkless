"""
Tests for issue #292: Look-around regex support in rlike().

PySpark supports look-ahead and look-behind assertions in regex patterns for rlike().
This test verifies that Sparkless supports the same via Python fallback when Polars
doesn't support these patterns.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue292RlikeLookaround:
    """Test look-around regex support in rlike() and related functions."""

    def test_rlike_negative_lookahead(self):
        """Test rlike with negative lookahead (from issue example)."""
        spark = SparkSession.builder.appName("issue-292").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": "Alice Train", "Value": 2},
                ]
            )

            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"  # Negative lookahead
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice Train"
            assert rows[0]["Value"] == 2
        finally:
            spark.stop()

    def test_rlike_positive_lookahead(self):
        """Test rlike with positive lookahead."""
        spark = SparkSession.builder.appName("issue-292").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": "Bob Cat", "Value": 2},
                    {"Name": "Alice Dog", "Value": 3},
                ]
            )

            # Match names that contain "Alice" followed by "Cat"
            regex_string = r"(?i)^.*(?=.*Alice)(?=.*Cat).*$"
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice Cat"
        finally:
            spark.stop()

    def test_rlike_lookbehind(self):
        """Test rlike with lookbehind assertion."""
        spark = SparkSession.builder.appName("issue-292").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "Price: $100", "Value": 1},
                    {"Text": "Cost: $50", "Value": 2},
                    {"Text": "Price: $200", "Value": 3},
                ]
            )

            # Match text with $ followed by digits (using lookbehind)
            regex_string = r"(?<=\$)\d+"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 3  # All rows match
        finally:
            spark.stop()

    def test_rlike_negative_lookbehind(self):
        """Test rlike with negative lookbehind."""
        spark = SparkSession.builder.appName("issue-292").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "Price: $100", "Value": 1},
                    {"Text": "Cost: 50", "Value": 2},
                    {"Text": "Price: $200", "Value": 3},
                ]
            )

            # Match numbers not preceded by $
            regex_string = r"(?<!\$)\d+"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            # Should match "Cost: 50" (50 is not preceded by $)
            assert len(rows) >= 1
        finally:
            spark.stop()

    def test_rlike_complex_lookaround(self):
        """Test rlike with complex look-around patterns."""
        spark = SparkSession.builder.appName("issue-292").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Email": "user@example.com", "Value": 1},
                    {"Email": "admin@test.org", "Value": 2},
                    {"Email": "test@example.com", "Value": 3},
                ]
            )

            # Match emails from example.com but not starting with "user"
            regex_string = r"(?i)^(?!user).*@example\.com$"
            result = df.filter(F.col("Email").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Email"] == "test@example.com"
        finally:
            spark.stop()

    def test_regexp_alias_lookaround(self):
        """Test regexp alias with look-around patterns (using rlike method)."""
        spark = SparkSession.builder.appName("issue-292").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": "Alice Train", "Value": 2},
                ]
            )

            # regexp is an alias for rlike, so use rlike method
            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice Train"
        finally:
            spark.stop()

    def test_regexp_like_alias_lookaround(self):
        """Test regexp_like alias with look-around patterns (using rlike method)."""
        spark = SparkSession.builder.appName("issue-292").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": "Alice Train", "Value": 2},
                ]
            )

            # regexp_like is an alias for rlike, so use rlike method
            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice Train"
        finally:
            spark.stop()

    def test_rlike_without_lookaround(self):
        """Test rlike with regular patterns (no look-around) still works."""
        spark = SparkSession.builder.appName("issue-292").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                    {"Name": "Charlie", "Value": 3},
                ]
            )

            # Regular pattern without look-around
            regex_string = r"^A.*"
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice"
        finally:
            spark.stop()

    def test_rlike_case_insensitive_lookaround(self):
        """Test rlike with case-insensitive flag and look-around."""
        spark = SparkSession.builder.appName("issue-292").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "alice cat", "Value": 1},
                    {"Name": "ALICE TRAIN", "Value": 2},
                    {"Name": "Bob Cat", "Value": 3},
                ]
            )

            # Case-insensitive with negative lookahead
            regex_string = r"(?i)^(?!.*(alice\sCat)).*$"
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            # Should match "ALICE TRAIN" and "Bob Cat" (case-insensitive)
            assert len(rows) == 2
            names = [r["Name"] for r in rows]
            assert "ALICE TRAIN" in names
            assert "Bob Cat" in names
        finally:
            spark.stop()

    def test_rlike_multiple_lookaheads(self):
        """Test rlike with multiple lookahead assertions."""
        spark = SparkSession.builder.appName("issue-292").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "abc123def", "Value": 1},
                    {"Text": "abc456def", "Value": 2},
                    {"Text": "xyz123def", "Value": 3},
                ]
            )

            # Match text that has "abc" and "123" (multiple lookaheads)
            regex_string = r"^(?=.*abc)(?=.*123).*$"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Text"] == "abc123def"
        finally:
            spark.stop()

    def test_rlike_lookaround_with_nulls(self):
        """Test rlike with look-around patterns and null values."""
        spark = SparkSession.builder.appName("issue-292").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": None, "Value": 2},
                    {"Name": "Alice Train", "Value": 3},
                ]
            )

            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            # Should only match "Alice Train" (null doesn't match)
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice Train"
        finally:
            spark.stop()

    def test_rlike_empty_dataframe(self):
        """Test rlike with look-around on empty DataFrame."""
        spark = SparkSession.builder.appName("issue-292").getOrCreate()
        try:
            from sparkless.spark_types import StructType, StructField, StringType

            schema = StructType(
                [
                    StructField("Name", StringType(), True),
                ]
            )
            df = spark.createDataFrame([], schema)

            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 0
        finally:
            spark.stop()

    def test_rlike_in_select(self):
        """Test rlike with look-around in select statement."""
        spark = SparkSession.builder.appName("issue-292").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": "Alice Train", "Value": 2},
                ]
            )

            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"
            result = df.select(
                "Name", "Value", F.col("Name").rlike(regex_string).alias("Matches")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Matches"] is False  # "Alice Cat" doesn't match
            assert rows[1]["Matches"] is True  # "Alice Train" matches
        finally:
            spark.stop()

    def test_rlike_in_withcolumn(self):
        """Test rlike with look-around in withColumn."""
        spark = SparkSession.builder.appName("issue-292").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": "Alice Train", "Value": 2},
                ]
            )

            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"
            df = df.withColumn("Matches", F.col("Name").rlike(regex_string))

            rows = df.collect()
            assert len(rows) == 2
            assert rows[0]["Matches"] is False
            assert rows[1]["Matches"] is True
        finally:
            spark.stop()

    def test_rlike_chained_operations(self):
        """Test rlike with look-around in chained filter operations."""
        spark = SparkSession.builder.appName("issue-292").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": "Alice Train", "Value": 2},
                    {"Name": "Bob Cat", "Value": 3},
                ]
            )

            # Filter out "Alice Cat" using negative lookahead
            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"
            result = df.filter(F.col("Name").rlike(regex_string)).filter(
                F.col("Value") > 1
            )

            rows = result.collect()
            # Should match "Alice Train" (Value=2) and "Bob Cat" (Value=3)
            # Both match the regex (not "Alice Cat") and have Value > 1
            assert len(rows) == 2
            names = [r["Name"] for r in rows]
            assert "Alice Train" in names
            assert "Bob Cat" in names
        finally:
            spark.stop()
