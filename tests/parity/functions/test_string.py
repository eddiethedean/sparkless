"""
PySpark parity tests for string functions.

Tests validate that Sparkless string functions behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_imports import get_spark_imports


class TestStringFunctionsParity(ParityTestBase):
    """Test string function parity with PySpark."""

    def test_string_upper(self, spark):
        """Test upper function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "string_upper")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.upper(df.name))
        self.assert_parity(result, expected)

    def test_string_lower(self, spark):
        """Test lower function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "string_lower")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.lower(df.name))
        self.assert_parity(result, expected)

    def test_string_length(self, spark):
        """Test length function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "string_length")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.length(df.name))
        self.assert_parity(result, expected)

    def test_string_substring(self, spark):
        """Test substring function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "string_substring")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.substring(df.name, 1, 3))
        self.assert_parity(result, expected)

    def test_string_substr_method(self, spark):
        """Test substr method matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        # Test the exact example from issue #238
        df = spark.createDataFrame(
            [
                {"name": "Alice"},
                {"name": "Bob"},
            ]
        )
        result = df.select(F.col("name").substr(1, 2).alias("partial_name"))
        rows = result.collect()

        # Verify results match PySpark behavior
        assert len(rows) == 2
        assert rows[0]["partial_name"] == "Al"
        assert rows[1]["partial_name"] == "Bo"

    def test_column_astype_method(self, spark):
        """Test astype method matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        # Test the exact example from issue #239
        df = spark.createDataFrame(
            [
                {"proc_date": "2025-01-01 ABC"},
                {"proc_date": "2025-01-02 DEF"},
            ]
        )
        # This should not raise AttributeError
        result = df.withColumn(
            "final_date", F.substring("proc_date", 1, 10).astype("date")
        )
        rows = result.collect()

        # Verify results match PySpark behavior
        assert len(rows) == 2
        assert "final_date" in rows[0]
        assert rows[0]["final_date"] is not None
        assert rows[1]["final_date"] is not None

    def test_string_concat(self, spark):
        """Test concat function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "string_concat")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.concat(df.name, F.lit(" - "), df.email))
        self.assert_parity(result, expected)

    def test_string_split(self, spark):
        """Test split function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "string_split")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.split(df.email, "@"))
        self.assert_parity(result, expected)

    def test_string_regexp_extract(self, spark):
        """Test regexp_extract function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "string_regexp_extract")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.regexp_extract(df.email, r"@(.+)", 1))
        self.assert_parity(result, expected)

    def test_string_trim(self, spark):
        """Test trim function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "string_trim")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.trim(df.name))
        self.assert_parity(result, expected)

    def test_string_ltrim(self, spark):
        """Test ltrim function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "string_ltrim")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.ltrim(df.name))
        self.assert_parity(result, expected)

    def test_string_rtrim(self, spark):
        """Test rtrim function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "string_rtrim")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.rtrim(df.name))
        self.assert_parity(result, expected)

    def test_string_lpad(self, spark):
        """Test lpad function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "string_lpad")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.lpad(df.name, 10, " "))
        self.assert_parity(result, expected)

    def test_string_rpad(self, spark):
        """Test rpad function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "string_rpad")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.rpad(df.name, 10, " "))
        self.assert_parity(result, expected)

    def test_string_like(self, spark):
        """Test like function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "string_like")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.col("name").like("%a%"))
        self.assert_parity(result, expected)

    def test_string_rlike(self, spark):
        """Test rlike function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "string_rlike")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.col("name").rlike("^[A-Z]"))
        self.assert_parity(result, expected)

    def test_concat_ws(self, spark):
        """Test concat_ws function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "concat_ws")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.concat_ws("-", df.name, df.email))
        self.assert_parity(result, expected)

    def test_ascii(self, spark):
        """Test ascii function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "ascii")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.ascii(df.name))
        self.assert_parity(result, expected)

    def test_hex(self, spark):
        """Test hex function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "hex")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.hex(df.name))
        self.assert_parity(result, expected)

    def test_base64(self, spark):
        """Test base64 function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "base64")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.base64(df.name))
        self.assert_parity(result, expected)

    def test_initcap(self, spark):
        """Test initcap function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "initcap")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.initcap(df.text))
        self.assert_parity(result, expected)

    def test_repeat(self, spark):
        """Test repeat function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "repeat")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.repeat(df.name, 2))
        self.assert_parity(result, expected)

    def test_reverse(self, spark):
        """Test reverse function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "reverse")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.reverse(df.name))
        self.assert_parity(result, expected)
