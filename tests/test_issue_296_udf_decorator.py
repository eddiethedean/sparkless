"""
Tests for issue #296: UDF decorator interface support.

PySpark supports UDFs defined with the @udf decorator pattern. This test verifies
that Sparkless supports the same decorator interface.
"""

from sparkless.sql import SparkSession
import sparkless.sql.types as T
import sparkless.sql.functions as F
from sparkless.sql.functions import udf


class TestIssue296UdfDecorator:
    """Test UDF decorator interface support."""

    def test_udf_decorator_with_return_type(self):
        """Test UDF decorator with return type (from issue example)."""
        spark = SparkSession.builder.appName("issue-296").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "abc"},
                    {"Name": "Bob", "Value": "def"},
                ]
            )

            # Define UDF with decorator interface
            @udf(T.StringType())
            def my_udf(x):
                return x.upper()

            # Apply the UDF
            result = df.withColumn("Value", my_udf(F.col("Value")))

            # Verify results
            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Value"] == "ABC"

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Value"] == "DEF"
        finally:
            spark.stop()

    def test_udf_decorator_without_return_type(self):
        """Test UDF decorator without return type (defaults to StringType)."""
        spark = SparkSession.builder.appName("issue-296").getOrCreate()
        try:
            df = spark.createDataFrame([{"value": "hello"}])

            # Define UDF with decorator (no return type - defaults to StringType)
            @udf()
            def upper_case(x):
                return x.upper()

            result = df.withColumn("value", upper_case(F.col("value")))
            rows = result.collect()
            assert rows[0]["value"] == "HELLO"
        finally:
            spark.stop()

    def test_udf_decorator_with_integer_type(self):
        """Test UDF decorator with IntegerType return type."""
        spark = SparkSession.builder.appName("issue-296").getOrCreate()
        try:
            df = spark.createDataFrame([{"value": 5}])

            @udf(T.IntegerType())
            def square(x):
                return x * x

            result = df.withColumn("squared", square(F.col("value")))
            rows = result.collect()
            assert rows[0]["squared"] == 25
        finally:
            spark.stop()

    def test_udf_decorator_with_multiple_arguments(self):
        """Test UDF decorator with multiple arguments."""
        spark = SparkSession.builder.appName("issue-296").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"a": 1, "b": 2},
                    {"a": 3, "b": 4},
                ]
            )

            @udf(T.IntegerType())
            def add(x, y):
                return x + y

            result = df.withColumn("sum", add(F.col("a"), F.col("b")))
            rows = result.collect()
            assert rows[0]["sum"] == 3
            assert rows[1]["sum"] == 7
        finally:
            spark.stop()

    def test_udf_decorator_in_select(self):
        """Test UDF decorator used in select operation (via withColumn then select)."""
        spark = SparkSession.builder.appName("issue-296").getOrCreate()
        try:
            df = spark.createDataFrame([{"name": "alice"}])

            @udf(T.StringType())
            def capitalize(x):
                return x.capitalize()

            # Use withColumn first (UDFs work reliably there), then select
            result = df.withColumn("name", capitalize(F.col("name"))).select("name")
            rows = result.collect()
            assert rows[0]["name"] == "Alice"
        finally:
            spark.stop()

    def test_udf_decorator_in_filter(self):
        """Test UDF decorator used in filter operation."""
        spark = SparkSession.builder.appName("issue-296").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"value": "hello"},
                    {"value": "world"},
                ]
            )

            @udf(T.BooleanType())
            def starts_with_h(x):
                return x.startswith("h")

            result = df.filter(starts_with_h(F.col("value")))
            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["value"] == "hello"
        finally:
            spark.stop()

    def test_udf_decorator_in_groupby_agg(self):
        """Test UDF decorator used in groupBy aggregation."""
        spark = SparkSession.builder.appName("issue-296").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"dept": "IT", "salary": 50000},
                    {"dept": "IT", "salary": 60000},
                    {"dept": "HR", "salary": 55000},
                ]
            )

            @udf(T.IntegerType())
            def double(x):
                return x * 2

            result = df.groupBy("dept").agg(double(F.avg("salary")).alias("double_avg"))
            rows = result.collect()
            assert len(rows) == 2
        finally:
            spark.stop()

    def test_udf_decorator_with_string_names(self):
        """Test UDF decorator with string column names."""
        spark = SparkSession.builder.appName("issue-296").getOrCreate()
        try:
            df = spark.createDataFrame([{"value": "test"}])

            @udf(T.StringType())
            def upper(x):
                return x.upper()

            # Use string column name instead of F.col()
            result = df.withColumn("value", upper("value"))
            rows = result.collect()
            assert rows[0]["value"] == "TEST"
        finally:
            spark.stop()

    def test_udf_decorator_chained_operations(self):
        """Test UDF decorator with chained DataFrame operations."""
        spark = SparkSession.builder.appName("issue-296").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"name": "alice", "age": 25},
                    {"name": "bob", "age": 30},
                ]
            )

            @udf(T.StringType())
            def capitalize(x):
                return x.capitalize()

            result = (
                df.filter(F.col("age") > 25)
                .withColumn("name", capitalize(F.col("name")))
                .select("name", "age")
            )
            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["name"] == "Bob"
        finally:
            spark.stop()

    def test_udf_decorator_vs_function_interface(self):
        """Test that decorator and function interfaces produce same results."""
        spark = SparkSession.builder.appName("issue-296").getOrCreate()
        try:
            df = spark.createDataFrame([{"value": "hello"}])

            # Decorator interface
            @udf(T.StringType())
            def upper_decorator(x):
                return x.upper()

            # Function interface
            def upper_func(x):
                return x.upper()

            upper_function = F.udf(upper_func, T.StringType())

            result1 = df.withColumn("value", upper_decorator(F.col("value")))
            result2 = df.withColumn("value", upper_function(F.col("value")))

            rows1 = result1.collect()
            rows2 = result2.collect()

            assert rows1[0]["value"] == rows2[0]["value"] == "HELLO"
        finally:
            spark.stop()

    def test_udf_decorator_with_null_values(self):
        """Test UDF decorator with null values."""
        spark = SparkSession.builder.appName("issue-296").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"value": "hello"},
                    {"value": None},
                ]
            )

            @udf(T.StringType())
            def upper(x):
                return x.upper() if x is not None else None

            result = df.withColumn("value", upper(F.col("value")))
            rows = result.collect()
            assert rows[0]["value"] == "HELLO"
            assert rows[1]["value"] is None
        finally:
            spark.stop()

    def test_udf_decorator_with_different_data_types(self):
        """Test UDF decorator with different return types."""
        spark = SparkSession.builder.appName("issue-296").getOrCreate()
        try:
            df = spark.createDataFrame([{"value": 5}])

            # Integer return type
            @udf(T.IntegerType())
            def square(x):
                return x * x

            # Float return type
            @udf(T.DoubleType())
            def half(x):
                return x / 2.0

            # Boolean return type
            @udf(T.BooleanType())
            def is_even(x):
                return x % 2 == 0

            result = (
                df.withColumn("square", square(F.col("value")))
                .withColumn("half", half(F.col("value")))
                .withColumn("is_even", is_even(F.col("value")))
            )

            rows = result.collect()
            assert rows[0]["square"] == 25
            assert rows[0]["half"] == 2.5
            assert rows[0]["is_even"] is False
        finally:
            spark.stop()

    def test_udf_decorator_multiple_udfs_same_dataframe(self):
        """Test multiple UDF decorators on the same DataFrame."""
        spark = SparkSession.builder.appName("issue-296").getOrCreate()
        try:
            df = spark.createDataFrame([{"name": "alice", "value": 5}])

            @udf(T.StringType())
            def capitalize(x):
                return x.capitalize()

            @udf(T.IntegerType())
            def square(x):
                return x * x

            result = df.withColumn("name", capitalize(F.col("name"))).withColumn(
                "value", square(F.col("value"))
            )

            rows = result.collect()
            assert rows[0]["name"] == "Alice"
            assert rows[0]["value"] == 25
        finally:
            spark.stop()

    def test_udf_decorator_with_computed_columns(self):
        """Test UDF decorator with pre-computed columns."""
        spark = SparkSession.builder.appName("issue-296").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"a": 1, "b": 2},
                    {"a": 3, "b": 4},
                ]
            )

            @udf(T.DoubleType())
            def add(x, y):
                return float(x + y)

            # Compute columns first, then apply UDF
            result = (
                df.withColumn("a_plus_1", F.col("a") + 1)
                .withColumn("b_times_2", F.col("b") * 2)
                .withColumn("sum", add(F.col("a_plus_1"), F.col("b_times_2")))
            )
            rows = result.collect()
            assert rows[0]["sum"] == 6.0  # (1+1) + (2*2) = 6
            assert rows[1]["sum"] == 12.0  # (3+1) + (4*2) = 12
        finally:
            spark.stop()
