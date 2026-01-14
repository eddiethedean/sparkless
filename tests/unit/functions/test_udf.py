"""Tests for udf module."""

import pytest
from sparkless.sql import SparkSession
from sparkless.functions.udf import UserDefinedFunction, UserDefinedTableFunction
from sparkless.functions.core.column import Column
from sparkless.functions.core.operations import ColumnOperation
from sparkless.spark_types import StringType, IntegerType, StructType, StructField


class TestUserDefinedFunction:
    """Test cases for UserDefinedFunction."""

    def test_udf_init(self):
        """Test UserDefinedFunction initialization."""

        def upper_case(s: str) -> str:
            return s.upper()

        udf = UserDefinedFunction(upper_case, StringType())

        assert udf.func == upper_case
        assert udf.returnType == StringType()
        assert udf.evalType == "SQL"
        assert udf._name is None
        assert udf._deterministic is True
        assert udf._is_pandas_udf is False

    def test_udf_init_with_name(self):
        """Test UDF initialization with custom name."""

        def upper_case(s: str) -> str:
            return s.upper()

        udf = UserDefinedFunction(upper_case, StringType(), name="my_upper")

        assert udf._name == "my_upper"

    def test_udf_init_pandas_evaltype(self):
        """Test UDF with PANDAS evalType."""

        def upper_case(s: str) -> str:
            return s.upper()

        udf = UserDefinedFunction(upper_case, StringType(), evalType="PANDAS")

        assert udf.evalType == "PANDAS"
        assert udf._is_pandas_udf is True

    def test_udf_as_nondeterministic(self):
        """Test marking UDF as nondeterministic."""

        def random_value() -> int:
            return 42

        udf = UserDefinedFunction(random_value, IntegerType())
        assert udf._deterministic is True

        result = udf.asNondeterministic()
        assert result is udf  # Should return self
        assert udf._deterministic is False

    def test_udf_call_with_string_column(self):
        """Test UDF call with string column name."""

        def upper_case(s: str) -> str:
            return s.upper()

        udf = UserDefinedFunction(upper_case, StringType())
        result = udf("name")

        assert isinstance(result, ColumnOperation)
        assert result.operation == "udf"
        assert hasattr(result, "_udf_func")
        assert result._udf_func == upper_case
        assert result._udf_return_type == StringType()

    def test_udf_call_with_column_object(self):
        """Test UDF call with Column object."""

        def upper_case(s: str) -> str:
            return s.upper()

        udf = UserDefinedFunction(upper_case, StringType())
        col = Column("name")
        result = udf(col)

        assert isinstance(result, ColumnOperation)
        assert result.operation == "udf"
        assert result._udf_func == upper_case

    def test_udf_call_with_multiple_columns(self):
        """Test UDF call with multiple columns."""

        def concat_strings(s1: str, s2: str) -> str:
            return s1 + s2

        udf = UserDefinedFunction(concat_strings, StringType())
        result = udf("first_name", "last_name")

        assert isinstance(result, ColumnOperation)
        assert len(result._udf_cols) == 2
        assert result._udf_cols[0].name == "first_name"
        assert result._udf_cols[1].name == "last_name"

    def test_udf_call_no_columns(self):
        """Test UDF call with no columns raises ValueError."""

        def no_args() -> int:
            return 42

        udf = UserDefinedFunction(no_args, IntegerType())

        with pytest.raises(
            ValueError, match="UDF requires at least one column argument"
        ):
            udf()

    def test_udf_creates_column_operation(self):
        """Test UDF creates ColumnOperation."""

        def upper_case(s: str) -> str:
            return s.upper()

        udf = UserDefinedFunction(upper_case, StringType())
        result = udf("name")

        assert isinstance(result, ColumnOperation)
        assert result.operation == "udf"

    def test_udf_stores_function(self):
        """Test UDF function is stored in ColumnOperation."""

        def upper_case(s: str) -> str:
            return s.upper()

        udf = UserDefinedFunction(upper_case, StringType())
        result = udf("name")

        assert hasattr(result, "_udf_func")
        assert result._udf_func == upper_case

    def test_udf_stores_return_type(self):
        """Test UDF return type is stored."""

        def upper_case(s: str) -> str:
            return s.upper()

        udf = UserDefinedFunction(upper_case, StringType())
        result = udf("name")

        assert hasattr(result, "_udf_return_type")
        assert result._udf_return_type == StringType()

    def test_udf_integration_with_dataframe(self):
        """Test UDF usage in DataFrame operations."""
        spark = SparkSession("test")
        spark.createDataFrame([{"name": "alice"}, {"name": "bob"}])

        def upper_case(s: str) -> str:
            return s.upper()

        udf = UserDefinedFunction(upper_case, StringType())
        result_op = udf("name")

        # Verify the operation can be created
        assert isinstance(result_op, ColumnOperation)
        assert result_op.operation == "udf"

    def test_udf_with_filter(self):
        """Test UDF in filter operations."""

        def is_long(s: str) -> bool:
            return len(s) > 5

        udf = UserDefinedFunction(is_long, StringType())
        result_op = udf("name")

        # Verify operation structure
        assert isinstance(result_op, ColumnOperation)
        assert result_op.operation == "udf"

    def test_udf_with_select(self):
        """Test UDF in select operations."""

        def upper_case(s: str) -> str:
            return s.upper()

        udf = UserDefinedFunction(upper_case, StringType())
        result_op = udf("name")

        # Verify operation structure
        assert isinstance(result_op, ColumnOperation)
        assert result_op.operation == "udf"

    def test_udf_custom_name_in_operation(self):
        """Test UDF custom name appears in operation."""

        def upper_case(s: str) -> str:
            return s.upper()

        udf = UserDefinedFunction(upper_case, StringType(), name="my_upper")
        result = udf("name")

        assert result.name == "my_upper"

    def test_udf_default_name_in_operation(self):
        """Test UDF default name format."""

        def upper_case(s: str) -> str:
            return s.upper()

        udf = UserDefinedFunction(upper_case, StringType())
        result = udf("name")

        assert result.name == "udf(name)"


class TestUserDefinedTableFunction:
    """Test cases for UserDefinedTableFunction."""

    def test_udf_table_function_init(self):
        """Test UserDefinedTableFunction initialization."""

        def split_string(s: str):
            return [(char,) for char in s]

        schema = StructType([StructField("char", StringType())])
        table_udf = UserDefinedTableFunction(split_string, schema)

        assert table_udf.func == split_string
        assert table_udf.returnType == schema
        assert table_udf._name is None

    def test_udf_table_function_init_with_name(self):
        """Test UserDefinedTableFunction initialization with name."""

        def split_string(s: str):
            return [(char,) for char in s]

        schema = StructType([StructField("char", StringType())])
        table_udf = UserDefinedTableFunction(split_string, schema, name="split")

        assert table_udf._name == "split"

    def test_udf_table_function_call(self):
        """Test table function call."""

        def split_string(s: str):
            return [(char,) for char in s]

        schema = StructType([StructField("char", StringType())])
        table_udf = UserDefinedTableFunction(split_string, schema)
        result = table_udf("name")

        assert isinstance(result, ColumnOperation)
        assert result.operation == "table_udf"
        assert hasattr(result, "_udf_func")
        assert result._udf_func == split_string
        assert result._udf_return_type == schema
        assert result._is_table_udf is True

    def test_udf_table_function_call_no_columns(self):
        """Test table function call with no columns raises ValueError."""

        def split_string(s: str):
            return [(char,) for char in s]

        schema = StructType([StructField("char", StringType())])
        table_udf = UserDefinedTableFunction(split_string, schema)

        with pytest.raises(
            ValueError, match="Table UDF requires at least one column argument"
        ):
            table_udf()

    def test_udf_table_function_call_with_multiple_columns(self):
        """Test table function call with multiple columns."""

        def combine_strings(s1: str, s2: str):
            return [(s1 + s2,)]

        schema = StructType([StructField("combined", StringType())])
        table_udf = UserDefinedTableFunction(combine_strings, schema)
        result = table_udf("first", "last")

        assert isinstance(result, ColumnOperation)
        assert len(result._udf_cols) == 2

    def test_udf_table_function_custom_name(self):
        """Test table function custom name."""

        def split_string(s: str):
            return [(char,) for char in s]

        schema = StructType([StructField("char", StringType())])
        table_udf = UserDefinedTableFunction(split_string, schema, name="split")
        result = table_udf("name")

        assert result.name == "split"

    def test_udf_table_function_default_name(self):
        """Test table function default name format."""

        def split_string(s: str):
            return [(char,) for char in s]

        schema = StructType([StructField("char", StringType())])
        table_udf = UserDefinedTableFunction(split_string, schema)
        result = table_udf("name")

        assert result.name == "table_udf(name)"
