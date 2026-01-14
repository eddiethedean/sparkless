"""
Comprehensive tests for issues 225-231.

This module contains robust tests for:
- Issue 225: String-to-Numeric Type Coercion
- Issue 226: isin method with *values (backward compatibility)
- Issue 227: getItem method
- Issue 228: Regex Look-Ahead/Look-Behind Fallback
- Issue 229: Pandas DataFrame Support
- Issue 230: Case-Insensitive Column Name Matching
- Issue 231: DataType simpleString() Method
"""

import pytest
from tests.fixtures.spark_imports import get_spark_imports
from tests.fixtures.spark_backend import get_backend_type, BackendType

# Get the appropriate imports based on backend (sparkless or PySpark)
imports = get_spark_imports()
F = imports.F
SparkSession = imports.SparkSession
StringType = imports.StringType
IntegerType = imports.IntegerType
LongType = imports.LongType
DoubleType = imports.DoubleType
ArrayType = imports.ArrayType
MapType = imports.MapType
StructType = imports.StructType
StructField = imports.StructField


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend = get_backend_type()
    return backend == BackendType.PYSPARK


# Try to import pandas - will skip tests if not available
# For PySpark mode, need to check for real pandas (not mock)
try:
    import pandas as pd
    # Check if it's the real pandas (not the mock from sparkless/pandas/)
    if hasattr(pd, "__version__") and pd.__version__ != "0.0.0-mock":
        PANDAS_AVAILABLE = True
    else:
        PANDAS_AVAILABLE = False
except ImportError:
    PANDAS_AVAILABLE = False


class TestIssue225StringToNumericCoercion:
    """Comprehensive tests for Issue 225: String-to-Numeric Type Coercion."""

    def test_string_eq_numeric_int(self, spark):
        """Test string == numeric (int) comparison with coercion."""
        data = [{"value": "100"}, {"value": "200"}, {"value": "50"}]
        df = spark.createDataFrame(data)

        # String == int should coerce string to numeric
        result = df.filter(F.col("value") == 100).collect()
        assert len(result) == 1
        assert result[0].value == "100"

        # String != int should work
        result = df.filter(F.col("value") != 100).collect()
        assert len(result) == 2

    def test_string_eq_numeric_float(self, spark):
        """Test string == numeric (float) comparison with coercion."""
        data = [{"value": "10.5"}, {"value": "20.3"}, {"value": "5.0"}]
        df = spark.createDataFrame(data)

        # String == float should coerce string to numeric
        result = df.filter(F.col("value") == 10.5).collect()
        assert len(result) == 1
        assert result[0].value == "10.5"

    def test_string_lt_numeric(self, spark):
        """Test string < numeric comparison with coercion."""
        data = [{"value": "100"}, {"value": "200"}, {"value": "50"}]
        df = spark.createDataFrame(data)

        # String < int should coerce
        result = df.filter(F.col("value") < 150).collect()
        assert len(result) == 2  # "100" and "50"

    def test_string_le_numeric(self, spark):
        """Test string <= numeric comparison with coercion."""
        data = [{"value": "100"}, {"value": "200"}, {"value": "50"}]
        df = spark.createDataFrame(data)

        result = df.filter(F.col("value") <= 100).collect()
        assert len(result) == 2  # "100" and "50"

    def test_string_gt_numeric(self, spark):
        """Test string > numeric comparison with coercion."""
        data = [{"value": "100"}, {"value": "200"}, {"value": "50"}]
        df = spark.createDataFrame(data)

        result = df.filter(F.col("value") > 150).collect()
        assert len(result) == 1  # "200"

    def test_string_ge_numeric(self, spark):
        """Test string >= numeric comparison with coercion."""
        data = [{"value": "100"}, {"value": "200"}, {"value": "50"}]
        df = spark.createDataFrame(data)

        result = df.filter(F.col("value") >= 100).collect()
        assert len(result) == 2  # "100" and "200"

    def test_numeric_eq_string(self, spark):
        """Test numeric == string comparison with coercion."""
        data = [{"value": 100}, {"value": 200}, {"value": 50}]
        df = spark.createDataFrame(data)

        # Int == string should coerce string to numeric
        result = df.filter(F.col("value") == "150").collect()
        assert len(result) == 0  # No match

        result = df.filter(F.col("value") == "100").collect()
        assert len(result) == 1
        assert result[0].value == 100

    def test_coercion_with_null(self, spark):
        """Test coercion handles NULL values correctly."""
        data = [{"value": "100"}, {"value": None}, {"value": "200"}]
        df = spark.createDataFrame(data)

        # NULL comparisons should return False
        result = df.filter(F.col("value") == 150).collect()
        assert len(result) == 0

    def test_coercion_invalid_string(self, spark):
        """Test coercion with invalid numeric strings."""
        data = [{"value": "abc"}, {"value": "100"}, {"value": "xyz"}]
        df = spark.createDataFrame(data)

        # Invalid strings should not match
        result = df.filter(F.col("value") == 100).collect()
        assert len(result) == 1
        assert result[0].value == "100"


class TestIssue226IsinWithValues:
    """Comprehensive tests for Issue 226: isin method with *values."""

    def test_isin_with_list(self, spark):
        """Test isin with list argument (backward compatibility)."""
        data = [{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}]
        df = spark.createDataFrame(data)

        # List argument should work
        result = df.filter(F.col("value").isin([2, 3])).collect()
        assert len(result) == 2
        assert {r.value for r in result} == {2, 3}

    def test_isin_with_star_args(self, spark):
        """Test isin with *args (new signature)."""
        data = [{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}]
        df = spark.createDataFrame(data)

        # *args should work
        result = df.filter(F.col("value").isin(2, 3)).collect()
        assert len(result) == 2
        assert {r.value for r in result} == {2, 3}

    def test_isin_with_strings(self, spark):
        """Test isin with string values."""
        data = [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}]
        df = spark.createDataFrame(data)

        result = df.filter(F.col("name").isin("Alice", "Bob")).collect()
        assert len(result) == 2
        assert {r.name for r in result} == {"Alice", "Bob"}

    def test_isin_with_mixed_types(self, spark):
        """Test isin with mixed type values."""
        # PySpark can't infer schema from mixed types, so skip in PySpark mode
        if _is_pyspark_mode():
            pytest.skip("PySpark can't infer schema from mixed types without explicit schema")
        data = [{"value": 1}, {"value": 2}, {"value": "test"}]
        df = spark.createDataFrame(data)

        result = df.filter(F.col("value").isin(1, 2)).collect()
        assert len(result) == 2

    def test_isin_with_empty_list(self, spark):
        """Test isin with empty list."""
        data = [{"value": 1}, {"value": 2}, {"value": 3}]
        df = spark.createDataFrame(data)

        result = df.filter(F.col("value").isin([])).collect()
        assert len(result) == 0

    def test_isin_with_single_value(self, spark):
        """Test isin with single value."""
        data = [{"value": 1}, {"value": 2}, {"value": 3}]
        df = spark.createDataFrame(data)

        result = df.filter(F.col("value").isin(2)).collect()
        assert len(result) == 1
        assert result[0].value == 2

    def test_isin_with_column_method(self, spark):
        """Test isin using column method."""
        data = [{"value": 1}, {"value": 2}, {"value": 3}]
        df = spark.createDataFrame(data)

        # isin is a column method, not a function
        result = df.filter(F.col("value").isin(2, 3)).collect()
        assert len(result) == 2
        assert {r.value for r in result} == {2, 3}


class TestIssue227GetItem:
    """Comprehensive tests for Issue 227: getItem method."""

    def test_getItem_with_array_index(self, spark):
        """Test getItem with array column and index."""
        data = [{"arr": [1, 2, 3]}, {"arr": [4, 5, 6]}]
        df = spark.createDataFrame(data, schema=StructType([StructField("arr", ArrayType(IntegerType()), True)]))

        result = df.select(F.col("arr").getItem(0).alias("first")).collect()
        assert len(result) == 2
        assert result[0].first == 1
        assert result[1].first == 4  # Second array's first element is 4

    def test_getItem_with_split_result(self, spark):
        """Test getItem with split result."""
        data = [{"text": "a,b,c"}, {"text": "x,y,z"}]
        df = spark.createDataFrame(data)

        result = df.select(F.split(F.col("text"), ",").getItem(0).alias("first")).collect()
        assert len(result) == 2
        assert result[0].first == "a"
        assert result[1].first == "x"

    def test_getItem_with_map_key(self, spark):
        """Test getItem with map column and key."""
        data = [{"map": {"key1": "value1", "key2": "value2"}}]
        df = spark.createDataFrame(data, schema=StructType([StructField("map", MapType(StringType(), StringType()), True)]))

        result = df.select(F.col("map").getItem("key1").alias("val")).collect()
        assert len(result) == 1
        assert result[0].val == "value1"

    def test_getItem_out_of_bounds(self, spark):
        """Test getItem with out-of-bounds index."""
        data = [{"arr": [1, 2, 3]}]
        df = spark.createDataFrame(data, schema=StructType([StructField("arr", ArrayType(IntegerType()), True)]))

        result = df.select(F.col("arr").getItem(10).alias("val")).collect()
        assert len(result) == 1
        assert result[0].val is None

    def test_getItem_negative_index(self, spark):
        """Test getItem with negative index."""
        data = [{"arr": [1, 2, 3]}]
        df = spark.createDataFrame(data, schema=StructType([StructField("arr", ArrayType(IntegerType()), True)]))

        # Negative indices may not be supported, test behavior
        result = df.select(F.col("arr").getItem(-1).alias("val")).collect()
        assert len(result) == 1


class TestIssue228RegexLookAheadLookBehind:
    """Comprehensive tests for Issue 228: Regex Look-Ahead/Look-Behind Fallback."""

    def test_regexp_extract_with_lookbehind(self, spark):
        """Test regexp_extract with look-behind pattern (should use Python fallback)."""
        data = [{"text": "hello world"}]
        df = spark.createDataFrame(data)

        # Look-behind pattern: (?<=hello )\w+
        result = df.select(F.regexp_extract(F.col("text"), r"(?<=hello )\w+", 0).alias("extracted")).collect()
        assert len(result) == 1
        # Should extract "world" (everything after "hello ")
        assert result[0].extracted == "world"

    def test_regexp_extract_with_lookahead(self, spark):
        """Test regexp_extract with look-ahead pattern (should use Python fallback)."""
        data = [{"text": "world hello"}]
        df = spark.createDataFrame(data)

        # Look-ahead pattern: \w+(?= hello)
        result = df.select(F.regexp_extract(F.col("text"), r"\w+(?= hello)", 0).alias("extracted")).collect()
        assert len(result) == 1
        # Should extract "world" (everything before " hello")
        assert result[0].extracted == "world"

    def test_regexp_extract_without_lookaround(self, spark):
        """Test regexp_extract without look-around (should use Polars native)."""
        data = [{"email": "alice@example.com"}]
        df = spark.createDataFrame(data)

        # Regular pattern without look-around
        result = df.select(F.regexp_extract(F.col("email"), r"@(.+)", 1).alias("domain")).collect()
        assert len(result) == 1
        assert result[0].domain == "example.com"

    def test_regexp_extract_with_complex_lookaround(self, spark):
        """Test regexp_extract with complex look-around patterns."""
        data = [{"text": "prefix123suffix"}]
        df = spark.createDataFrame(data)

        # Complex pattern with look-behind and look-ahead
        result = df.select(F.regexp_extract(F.col("text"), r"(?<=prefix)\d+(?=suffix)", 0).alias("extracted")).collect()
        assert len(result) == 1
        assert result[0].extracted == "123"


@pytest.mark.skipif(not PANDAS_AVAILABLE, reason="pandas not available")
class TestIssue229PandasDataFrameSupport:
    """Comprehensive tests for Issue 229: Pandas DataFrame Support."""

    def test_createDataFrame_from_pandas(self, spark):
        """Test createDataFrame with pandas DataFrame."""
        pd_df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        df = spark.createDataFrame(pd_df)

        assert df.count() == 3
        assert "id" in df.columns
        assert "name" in df.columns
        rows = df.collect()
        assert rows[0].id == 1
        assert rows[0].name == "Alice"

    def test_createDataFrame_from_pandas_with_schema(self, spark):
        """Test createDataFrame with pandas DataFrame and explicit schema."""
        pd_df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        schema = StructType([StructField("id", IntegerType(), True), StructField("name", StringType(), True)])
        df = spark.createDataFrame(pd_df, schema)

        assert df.count() == 3
        assert df.schema.fields[0].dataType == IntegerType()
        assert df.schema.fields[1].dataType == StringType()

    def test_createDataFrame_from_pandas_empty(self, spark):
        """Test createDataFrame with empty pandas DataFrame."""
        pd_df = pd.DataFrame({"id": [], "name": []})
        df = spark.createDataFrame(pd_df)

        assert df.count() == 0
        assert "id" in df.columns
        assert "name" in df.columns

    def test_createDataFrame_from_pandas_with_nulls(self, spark):
        """Test createDataFrame with pandas DataFrame containing nulls."""
        pd_df = pd.DataFrame({"id": [1, None, 3], "name": ["Alice", None, "Charlie"]})
        df = spark.createDataFrame(pd_df)

        assert df.count() == 3
        rows = df.collect()
        assert rows[1].id is None
        assert rows[1].name is None


class TestIssue230CaseInsensitiveColumnMatching:
    """Comprehensive tests for Issue 230: Case-Insensitive Column Name Matching."""

    def test_select_case_insensitive(self, spark):
        """Test select with case-insensitive column names."""
        data = [{"Name": "Alice", "Age": 25}, {"Name": "Bob", "Age": 30}]
        df = spark.createDataFrame(data)

        # Should find column regardless of case, but output preserves original column name
        result = df.select("name").collect()
        assert len(result) == 2
        assert result[0].Name == "Alice"  # Output uses original column name "Name"

        result = df.select("NAME").collect()
        assert len(result) == 2
        assert result[0].Name == "Alice"  # Output uses original column name "Name"

        result = df.select("nAmE").collect()
        assert len(result) == 2
        assert result[0].Name == "Alice"  # Output uses original column name "Name"

    def test_selectExpr_case_insensitive(self, spark):
        """Test selectExpr with case-insensitive column names."""
        data = [{"Name": "Alice", "Age": 25}, {"Name": "Bob", "Age": 30}]
        df = spark.createDataFrame(data)

        result = df.selectExpr("name").collect()
        assert len(result) == 2

        result = df.selectExpr("NAME").collect()
        assert len(result) == 2

    def test_filter_case_insensitive(self, spark):
        """Test filter with case-insensitive column names."""
        data = [{"Name": "Alice", "Age": 25}, {"Name": "Bob", "Age": 30}]
        df = spark.createDataFrame(data)

        result = df.filter(F.col("name") == "Alice").collect()
        assert len(result) == 1

        result = df.filter(F.col("NAME") == "Alice").collect()
        assert len(result) == 1

    def test_withColumn_case_insensitive(self, spark):
        """Test withColumn with case-insensitive column names."""
        data = [{"Name": "Alice", "Age": 25}]
        df = spark.createDataFrame(data)

        result = df.withColumn("new_col", F.col("age") * 2).collect()
        assert len(result) == 1
        assert result[0].new_col == 50

    def test_groupBy_case_insensitive(self, spark):
        """Test groupBy with case-insensitive column names."""
        data = [{"Dept": "IT", "Salary": 100}, {"Dept": "IT", "Salary": 200}, {"Dept": "HR", "Salary": 150}]
        df = spark.createDataFrame(data)

        result = df.groupBy("dept").agg(F.sum("salary").alias("total")).collect()
        assert len(result) == 2

    def test_orderBy_case_insensitive(self, spark):
        """Test orderBy with case-insensitive column names."""
        data = [{"Name": "Bob", "Age": 30}, {"Name": "Alice", "Age": 25}]
        df = spark.createDataFrame(data)

        result = df.orderBy("age").collect()
        assert len(result) == 2
        assert result[0].Age == 25

    def test_withColumnRenamed_case_insensitive(self, spark):
        """Test withColumnRenamed with case-insensitive column names."""
        data = [{"Name": "Alice", "Age": 25}]
        df = spark.createDataFrame(data)

        result = df.withColumnRenamed("name", "FullName").collect()
        assert len(result) == 1
        assert hasattr(result[0], "FullName")
        assert result[0].FullName == "Alice"
        assert hasattr(result[0], "Age")

        # Test with uppercase
        result = df.withColumnRenamed("AGE", "Years").collect()
        assert len(result) == 1
        assert hasattr(result[0], "Years")
        assert result[0].Years == 25

    def test_withColumnsRenamed_case_insensitive(self, spark):
        """Test withColumnsRenamed with case-insensitive column names."""
        data = [{"Name": "Alice", "Age": 25, "City": "NYC"}]
        df = spark.createDataFrame(data)

        result = df.withColumnsRenamed({"name": "FullName", "age": "Years"}).collect()
        assert len(result) == 1
        assert hasattr(result[0], "FullName")
        assert result[0].FullName == "Alice"
        assert hasattr(result[0], "Years")
        assert result[0].Years == 25
        assert hasattr(result[0], "City")

    def test_drop_case_insensitive(self, spark):
        """Test drop with case-insensitive column names."""
        data = [{"Name": "Alice", "Age": 25, "City": "NYC"}]
        df = spark.createDataFrame(data)

        result_df = df.drop("name")
        result = result_df.collect()
        assert len(result) == 1
        # Check columns list instead of hasattr (Row objects may have None attributes)
        assert "Name" not in result_df.columns
        assert "Age" in result_df.columns
        assert "City" in result_df.columns

        # Test dropping multiple columns
        result_df = df.drop("AGE", "city")
        result = result_df.collect()
        assert len(result) == 1
        assert "Name" in result_df.columns
        assert "Age" not in result_df.columns
        assert "City" not in result_df.columns

    def test_dropDuplicates_case_insensitive(self, spark):
        """Test dropDuplicates with case-insensitive column names."""
        data = [
            {"Name": "Alice", "Age": 25, "City": "NYC"},
            {"Name": "Bob", "Age": 25, "City": "NYC"},
            {"Name": "Alice", "Age": 30, "City": "NYC"},
        ]
        df = spark.createDataFrame(data)

        # Drop duplicates based on case-insensitive column name
        result = df.dropDuplicates(subset=["name"]).collect()
        assert len(result) == 2  # Should have Alice and Bob

        # Verify the result
        names = [row.Name for row in result]
        assert "Alice" in names
        assert "Bob" in names

    def test_unionByName_case_insensitive(self, spark):
        """Test unionByName with case-insensitive column names."""
        data1 = [{"Name": "Alice", "Age": 25}]
        df1 = spark.createDataFrame(data1)

        # DataFrame with different case column names
        data2 = [{"NAME": "Bob", "AGE": 30}]
        df2 = spark.createDataFrame(data2)

        # Should work with case-insensitive matching
        result_df = df1.unionByName(df2, allowMissingColumns=False)
        result = result_df.collect()
        assert len(result) == 2
        # Both rows should have the same column names (from df1)
        assert "Name" in result_df.columns
        assert "Age" in result_df.columns
        # Verify data
        names = [row.Name for row in result]
        assert "Alice" in names
        assert "Bob" in names

    def test_join_case_insensitive(self, spark):
        """Test join with case-insensitive column names."""
        data1 = [{"ID": 1, "Name": "Alice"}, {"ID": 2, "Name": "Bob"}]
        df1 = spark.createDataFrame(data1)

        data2 = [{"id": 1, "Dept": "IT"}, {"id": 2, "Dept": "HR"}]
        df2 = spark.createDataFrame(data2)

        # Join on case-insensitive column names (use lowercase to match df2's "id")
        result_df = df1.join(df2, on="id", how="inner")
        result = result_df.collect()
        assert len(result) == 2
        assert "Name" in result_df.columns
        assert "Dept" in result_df.columns
        # Verify data
        assert result[0].Name == "Alice"
        assert result[0].Dept == "IT"

        # Test with uppercase join key (should also work)
        result_df = df1.join(df2, on="ID", how="inner")
        result = result_df.collect()
        assert len(result) == 2


class TestIssue231DataTypeSimpleString:
    """Comprehensive tests for Issue 231: DataType simpleString() Method."""

    def test_string_type_simpleString(self):
        """Test StringType.simpleString()."""
        st = StringType()
        assert st.simpleString() == "string"

    def test_integer_type_simpleString(self):
        """Test IntegerType.simpleString()."""
        it = IntegerType()
        assert it.simpleString() == "int"

    def test_array_type_simpleString(self):
        """Test ArrayType.simpleString()."""
        at = ArrayType(StringType())
        assert at.simpleString() == "array<string>"

        at_int = ArrayType(IntegerType())
        assert at_int.simpleString() == "array<int>"

    def test_array_type_nested_simpleString(self):
        """Test nested ArrayType.simpleString()."""
        nested = ArrayType(ArrayType(StringType()))
        assert nested.simpleString() == "array<array<string>>"

    def test_map_type_simpleString(self):
        """Test MapType.simpleString()."""
        mt = MapType(StringType(), IntegerType())
        assert mt.simpleString() == "map<string,int>"

    def test_map_type_complex_simpleString(self):
        """Test MapType with complex value type."""
        mt = MapType(StringType(), ArrayType(IntegerType()))
        assert mt.simpleString() == "map<string,array<int>>"

    def test_struct_type_simpleString(self):
        """Test StructType.simpleString()."""
        st = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        result = st.simpleString()
        assert result == "struct<name:string,age:int>" or result == "struct<age:int,name:string>"

    def test_struct_type_nested_simpleString(self):
        """Test nested StructType.simpleString()."""
        nested = StructType(
            [
                StructField("address", StructType([StructField("city", StringType(), True)]), True),
                StructField("age", IntegerType(), True),
            ]
        )
        result = nested.simpleString()
        assert "struct" in result
        assert "city" in result
        assert "age" in result

    def test_struct_type_with_array_simpleString(self):
        """Test StructType with ArrayType field."""
        st = StructType(
            [
                StructField("name", StringType(), True),
                StructField("scores", ArrayType(IntegerType()), True),
            ]
        )
        result = st.simpleString()
        assert "name" in result
        assert "array<int>" in result