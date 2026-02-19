### Summary
Creating a DataFrame from rows with array columns can fail with `array column value must be null or array`. PySpark createDataFrame accepts Python lists for array columns; the crate may expect a different JSON representation or reject some list payloads.

### PySpark (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

spark = SparkSession.builder.getOrCreate()
schema = StructType([
    StructField("id", StringType()),
    StructField("arr", ArrayType(IntegerType())),
])

df = spark.createDataFrame([("x", [1, 2, 3]), ("y", [4, 5])], schema)
print(df.collect())
```

**PySpark output:** `[Row(id='x', arr=[1, 2, 3]), Row(id='y', arr=[4, 5])]`

### robin-sparkless (current behavior)
The value must be JSON null or a JSON array. If the caller sends the array column as a JSON array and it still fails, the issue may be nested element types or encoding.

**Error seen from Sparkless:** `ValueError: create_dataframe_from_rows failed: array column value must be null or array`

**Request:** Ensure any JSON array payload that PySpark would treat as an array column is accepted (or document the exact expected format), and align behavior with PySpark for nested arrays and nulls.
