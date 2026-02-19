### Summary
When creating a DataFrame from rows that include struct columns, robin-sparkless fails with `struct value must be object or array` when the struct is passed in a format that PySpark accepts (e.g. tuple/list as positional struct fields). PySpark accepts both dict (by field name) and tuple/list (by position) for struct columns.

### PySpark (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.getOrCreate()
schema = StructType([
    StructField("id", StringType()),
    StructField("nested", StructType([
        StructField("a", IntegerType()),
        StructField("b", StringType()),
    ])),
])

# PySpark accepts dict for struct
df1 = spark.createDataFrame([("x", {"a": 1, "b": "y"})], schema)
print(df1.collect())  # [Row(id='x', nested=Row(a=1, b='y'))]

# PySpark also accepts tuple/list for struct (positional)
df2 = spark.createDataFrame([("x", (1, "y"))], schema)
print(df2.collect())  # [Row(id='x', nested=Row(a=1, b='y'))]
```

**PySpark output:** Both succeed. Rows show `nested=Row(a=1, b='y')`.

### robin-sparkless (current behavior)
When the Sparkless Python layer sends rows from `createDataFrame([("A", {"value_1": 1, "value_2": "x"})], schema)`, the struct is converted to JSON. If the conversion passes a tuple as something other than a JSON array, or the caller sends structs in another common format, the crate errors.

**Error seen from Sparkless:** `ValueError: create_dataframe_from_rows failed: struct value must be object or array`

**Request:** Document the exact JSON shape expected for struct/array columns and/or accept the same struct representations PySpark does (object by name, array by position) so that create_dataframe_from_rows matches PySpark semantics.
