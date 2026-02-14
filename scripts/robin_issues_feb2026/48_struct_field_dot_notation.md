## Summary

PySpark struct field access: col("StructValue.E1") or getField("E1"). Robin reports Column StructValue.E1 not found.

**Request:** Support struct field access by name for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"Name": "a", "StructValue": {"E1": 1}}], [("Name", "string"), ("StructValue", "struct")])
df.select(rs.col("StructValue.E1")).collect()
```

Error: not found Column StructValue.E1

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
spark = SparkSession.builder.master("local[1]").getOrCreate()
schema = StructType([StructField("Name", StringType()), StructField("StructValue", StructType([StructField("E1", IntegerType())]))])
df = spark.createDataFrame([("a", (1,))], schema)
df.select(F.col("StructValue.E1")).collect()
spark.stop()
```

PySpark supports col("struct.field").
